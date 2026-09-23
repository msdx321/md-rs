//! Merge only complete tracks, and publish an MP4 only after validating it.

use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::Context;

use crate::jav::source::stream::m3u8::M3u8Info;

pub(super) fn require_tools() -> anyhow::Result<()> {
    for tool in ["ffmpeg", "ffprobe"] {
        let output = Command::new(tool)
            .arg("-version")
            .output()
            .with_context(|| format!("{tool} is required for verified video downloads"))?;
        anyhow::ensure!(output.status.success(), "cannot run {tool}");
    }
    Ok(())
}

fn probe(path: &Path) -> anyhow::Result<serde_json::Value> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-probesize",
            "50000000",
            "-analyzeduration",
            "30000000",
            "-show_entries",
            "stream=codec_type,codec_name,profile,duration,nb_frames",
            "-of",
            "json",
        ])
        .arg(path)
        .output()
        .context("cannot run ffprobe to inspect audio and video")?;
    anyhow::ensure!(
        output.status.success(),
        "media file is unreadable: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(serde_json::from_slice(&output.stdout)?)
}

pub(super) fn validate_output(path: &Path, duration: Option<f64>) -> anyhow::Result<()> {
    let probe = probe(path)?;
    let streams = probe["streams"]
        .as_array()
        .context("no media streams found")?;
    for kind in ["video", "audio"] {
        let stream = streams
            .iter()
            .find(|s| s["codec_type"] == kind)
            .with_context(|| {
                format!("no {kind} stream found; refusing to mark the download complete")
            })?;
        if let Some(frames) = stream["nb_frames"]
            .as_str()
            .and_then(|v| v.parse::<u64>().ok())
        {
            anyhow::ensure!(frames > 0, "merged {kind} stream is empty");
        }
        if let Some(expected) = duration {
            let actual = stream["duration"]
                .as_str()
                .and_then(|v| v.parse::<f64>().ok())
                .with_context(|| format!("merged {kind} stream has no duration"))?;
            let tolerance = 2.0f64.max(expected * 0.01);
            anyhow::ensure!(
                actual.is_finite() && actual > 0.0 && actual + tolerance >= expected,
                "merged {kind} is too short ({actual:.1}s, expected {expected:.1}s)"
            );
        }
    }
    Ok(())
}

/// Assemble a continuous input so probing is not limited to the first TS segment.
pub(super) fn assemble_track(
    dir: &Path,
    info: &M3u8Info,
    running: impl Fn() -> bool,
) -> anyhow::Result<tempfile::NamedTempFile> {
    anyhow::ensure!(!info.segments.is_empty(), "playlist contained no segments");
    let mut raw = tempfile::NamedTempFile::new_in(dir)?;
    let mut writer = std::io::BufWriter::new(raw.as_file_mut());
    let init = info.init_segment.as_ref().map(|_| dir.join("init.mp4"));
    for path in init
        .into_iter()
        .chain((0..info.segments.len()).map(|i| dir.join(format!("{i}.ts"))))
    {
        anyhow::ensure!(running(), "merge interrupted");
        let mut file = std::fs::File::open(&path)
            .with_context(|| format!("missing media segment {}", path.display()))?;
        anyhow::ensure!(
            file.metadata()?.len() > 0,
            "empty media segment {}",
            path.display()
        );
        std::io::copy(&mut file, &mut writer)?;
    }
    writer.flush()?;
    drop(writer);
    Ok(raw)
}

pub(super) fn merge_segments(
    temp_dir: &Path,
    info: &M3u8Info,
    final_path: &Path,
    running: impl Fn() -> bool,
    commit: impl Fn() -> bool,
) -> anyhow::Result<PathBuf> {
    let video = assemble_track(temp_dir, info, &running)?;
    let audio = info
        .audio
        .as_deref()
        .map(|audio| assemble_track(&temp_dir.join("audio"), audio, &running))
        .transpose()?;
    // Inspect the continuous track, not just its first segment: audio can start
    // later in a muxed HLS stream. AAC-LC already works in MP4 and Emby.
    let input = probe(audio.as_ref().unwrap_or(&video).path())?;
    let audio_stream = input["streams"]
        .as_array()
        .and_then(|streams| {
            streams
                .iter()
                .find(|stream| stream["codec_type"] == "audio")
        })
        .context("no audio stream found; refusing to produce a silent video")?;
    let copy_audio = audio_stream["codec_name"] == "aac" && audio_stream["profile"] == "LC";
    anyhow::ensure!(running(), "merge interrupted");
    // Publish from a staging file beside the destination: temp and downloads
    // may be different mounts. ffmpeg's +faststart rewrites its whole output a
    // second time, so on another filesystem it runs locally and the result is
    // copied across exactly once.
    let parent = final_path
        .parent()
        .context("output has no parent directory")?;
    let local = !same_filesystem(temp_dir, parent);
    let mut staging = tempfile::Builder::new();
    staging.prefix(".jav-merge-").suffix(".mp4.part");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // NamedTempFile otherwise forces 0600, which survives ffmpeg and rename
        // and prevents media servers running as another user from reading it.
        // Apply normal output-file permissions, respecting umask and default ACLs.
        staging.permissions(std::fs::Permissions::from_mode(0o666));
    }
    let staged = staging.tempfile_in(if local { temp_dir } else { parent })?;
    let mut repair_audio_timestamps = false;
    loop {
        anyhow::ensure!(running(), "merge interrupted");
        let mut command = Command::new("ffmpeg");
        command.args([
            "-nostdin",
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-xerror",
            "-abort_on",
            "empty_output+empty_output_stream",
        ]);
        for input in std::iter::once(&video).chain(audio.iter()) {
            command
                .args([
                    "-probesize",
                    "50000000",
                    "-analyzeduration",
                    "30000000",
                    "-i",
                ])
                .arg(input.path());
        }
        // Audio is mandatory. Optional maps would quietly produce a silent video.
        command.args([
            "-map",
            "0:v:0",
            "-map",
            if audio.is_some() { "1:a:0" } else { "0:a:0" },
        ]);
        command.args(["-c:v", "copy", "-c:a"]);
        if copy_audio && !repair_audio_timestamps {
            command.arg("copy");
        } else {
            command.args(["aac", "-profile:a", "aac_low", "-b:a", "192k"]);
        }
        if repair_audio_timestamps {
            // Reconcile audio samples with their timestamps without shifting the
            // track's start relative to video (which may contain delayed audio).
            command.args(["-af", "aresample=async=1"]);
        }
        command
            .args([
                "-movflags",
                "+faststart",
                "-avoid_negative_ts",
                "make_zero",
                "-f",
                "mp4",
            ])
            .arg(staged.path());
        // Use a file instead of a pipe so stderr cannot block a long merge.
        let mut errors = tempfile::tempfile()?;
        let mut child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(errors.try_clone()?)
            .spawn()
            .context("cannot start ffmpeg")?;
        let status = loop {
            if !running() {
                let _ = child.kill();
                let _ = child.wait();
                anyhow::bail!("merge interrupted");
            }
            if let Some(status) = child.try_wait()? {
                break status;
            }
            std::thread::sleep(Duration::from_millis(200));
        };
        if status.success() {
            break;
        }
        use std::io::Read;
        errors.rewind()?;
        let mut message = String::new();
        errors.read_to_string(&mut message)?;
        // Keep strict error handling: only retry non-monotonic output audio DTS,
        // never video timestamp failures or unrelated decoding/muxing errors.
        let audio_dts_error = message.lines().any(|line| {
            (line.contains("Non-monotonous DTS") || line.contains("Non-monotonic DTS"))
                && (line.contains("output stream 0:1;") || line.contains("[aost#0:1/"))
        });
        if !repair_audio_timestamps && audio_dts_error {
            log::warn!(
                "retrying merge for {} with audio timestamp repair: {}",
                final_path.display(),
                message.trim()
            );
            repair_audio_timestamps = true;
            continue;
        }
        anyhow::bail!("ffmpeg merge failed: {}", message.trim());
    }
    validate_output(staged.path(), Some(info.total_duration))?;
    anyhow::ensure!(running(), "merge interrupted");
    let staged = if local {
        let mut published = staging.tempfile_in(parent)?;
        copy_while(staged.as_file(), published.as_file_mut(), &running)?;
        published
    } else {
        staged
    };
    staged.as_file().sync_all()?;
    anyhow::ensure!(commit(), "merge interrupted before publication");
    staged
        .persist(final_path)
        .context("cannot publish merged MP4")?;
    Ok(final_path.to_path_buf())
}

/// True unless both paths are known to be on different filesystems.
fn same_filesystem(a: &Path, b: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let (Ok(a), Ok(b)) = (std::fs::metadata(a), std::fs::metadata(b)) {
            return a.dev() == b.dev();
        }
    }
    true
}

/// Copy in large chunks so a pause or cancel is observed during long copies.
/// `io::copy` still uses kernel copy offloading where available.
fn copy_while(
    mut source: &std::fs::File,
    destination: &mut std::fs::File,
    running: impl Fn() -> bool,
) -> anyhow::Result<()> {
    use std::io::Read;
    source.rewind()?;
    loop {
        anyhow::ensure!(running(), "merge interrupted");
        if std::io::copy(&mut source.take(64 * 1024 * 1024), destination)? == 0 {
            return Ok(());
        }
    }
}
