//! The download engine: resolve a video page, fetch its HLS segments
//! concurrently, decrypt and merge them into a single file.
//!
//! Every long-running step is interruptible: pause keeps the temp directory so
//! a later resume only fetches the missing segments.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tokio::sync::Semaphore;

use crate::app::{AppCtx, TaskState, now_rfc3339};
use crate::source::http::{Fetcher, MediaRejected, validate_media_body};
use crate::source::scraper::VideoCard;
use crate::source::stream::m3u8::{M3u8Info, Segment};
use crate::source::stream::{self, StreamKind};
use crate::storage::Record;
use crate::util::{
    SpeedTracker, cloudflare_hint, iv_for_segment, sanitize_filename, strip_fake_header,
};

/// How a download run ended.
enum Outcome {
    Done,
    Paused,
    Cancelled,
}

/// Download one video end to end, updating the task registry and the dedup
/// ledger as it goes. Errors are reported through the task, not returned.
pub async fn download_video(ctx: Arc<AppCtx>, card: VideoCard) {
    let id = card.id.clone();
    let Some(_slot) = ctx.download_slot(&id).await else {
        return;
    };
    let cfg = ctx.config();
    let fetch = ctx.fetcher();

    let mut started = false;
    ctx.update_task(&id, |t| {
        if t.state != TaskState::Queued {
            return;
        }
        started = true;
        t.state = TaskState::Running;
        t.phase = "resolving".into();
        t.title = card.title.clone();
        t.message = "resolving stream".into();
    });
    if !started {
        return;
    }

    // ── resolve ──────────────────────────────────────────────────────────
    let resolved =
        match stream::resolve_stream(&fetch, &card.url, &cfg.resolution, cfg.min_duration_secs)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let msg = cloudflare_hint(&format!("{e:#}"));
                log::warn!("[{id}] resolve failed: {msg}");
                finish_failed(&ctx, &cfg, &card, &msg).await;
                return;
            }
        };

    let title = if resolved.title.trim().is_empty() {
        card.title.clone()
    } else {
        resolved.title.clone()
    };
    let safe_title = sanitize_filename(&title);
    let file_name = format!("{id} - {safe_title}.mp4");

    let save_path: PathBuf = cfg.save_path.clone();
    if let Err(e) = tokio::fs::create_dir_all(&save_path).await {
        finish_failed(
            &ctx,
            &cfg,
            &card,
            &format!("cannot create save directory: {e}"),
        )
        .await;
        return;
    }
    let final_path = save_path.join(&file_name);
    let temp_dir = cfg.temp_path.join(format!("temp_{id}"));

    ctx.update_task(&id, |t| {
        t.title = title.clone();
        t.path = final_path.to_string_lossy().to_string();
        t.phase = "preparing".into();
    });

    // Already on disk from an earlier run or a crash after merge. Without
    // ffmpeg the merge produces a `.ts`, so both extensions are checked.
    let ts_path = final_path.with_extension("ts");
    for existing in [&final_path, &ts_path] {
        if is_non_empty(existing) {
            log::info!("[{id}] output already exists at {}", existing.display());
            finish_completed(&ctx, &card, &title, existing, 0).await;
            return;
        }
    }

    if let Err(e) = migrate_segments(&save_path.join(format!("temp_{id}")), &temp_dir).await {
        finish_failed(
            &ctx,
            &cfg,
            &card,
            &format!("cannot move temporary segments: {e}"),
        )
        .await;
        return;
    }

    // ── run ──────────────────────────────────────────────────────────────
    let run_result: anyhow::Result<(Outcome, PathBuf)> = match resolved.kind {
        StreamKind::Hls => {
            run_hls(
                &ctx,
                &fetch,
                &id,
                &title,
                &card.url,
                &resolved.url,
                &temp_dir,
                &final_path,
                &cfg,
            )
            .await
        }
    };
    let (outcome, produced) = match run_result {
        Ok((outcome, produced)) => (Ok(outcome), produced),
        Err(e) => (Err(e), final_path.clone()),
    };

    match outcome {
        Ok(Outcome::Done) => {
            let size = tokio::fs::metadata(&produced)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            finish_completed(&ctx, &card, &title, &produced, size).await;
        }
        Ok(Outcome::Paused) => {
            log::info!("[{id}] paused");
            ctx.update_task(&id, |t| {
                t.state = TaskState::Paused;
                t.phase = "paused".into();
                t.speed_kbps = 0.0;
                t.message = "paused — resume to continue from downloaded segments".into();
            });
        }
        Ok(Outcome::Cancelled) => {
            log::info!("[{id}] cancelled");
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            let _ = tokio::fs::remove_file(&final_path).await;
            let _ = tokio::fs::remove_file(&ts_path).await;
            ctx.update_task(&id, |t| {
                t.state = TaskState::Cancelled;
                t.phase = "cancelled".into();
                t.speed_kbps = 0.0;
                t.message = "cancelled".into();
            });
        }
        Err(e) => {
            for path in [&final_path, &ts_path] {
                if let Err(error) = tokio::fs::remove_file(path).await
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    log::warn!(
                        "cannot remove incomplete output {}: {error}",
                        path.display()
                    );
                }
            }
            let msg = cloudflare_hint(&format!("{e:#}"));
            log::warn!("[{id}] download failed: {msg}");
            finish_failed(&ctx, &cfg, &card, &msg).await;
        }
    }
}

/// Adopt segment files from older versions, including across Docker mounts.
async fn migrate_segments(legacy: &Path, destination: &Path) -> anyhow::Result<()> {
    if legacy == destination || !tokio::fs::try_exists(legacy).await? {
        return Ok(());
    }
    tokio::fs::create_dir_all(destination).await?;
    if tokio::fs::canonicalize(legacy).await? == tokio::fs::canonicalize(destination).await? {
        return Ok(());
    }
    let mut entries = tokio::fs::read_dir(legacy).await?;
    while let Some(entry) = entries.next_entry().await? {
        let target = destination.join(entry.file_name());
        if !entry.file_type().await?.is_file() {
            anyhow::bail!("unexpected directory in legacy segment folder");
        }
        // Copy through a temporary file so an interrupted migration cannot
        // leave a truncated segment that resume would consider complete.
        if !tokio::fs::try_exists(&target).await? {
            let staging = target.with_extension("migrating");
            tokio::fs::copy(entry.path(), &staging).await?;
            tokio::fs::rename(staging, &target).await?;
        }
        tokio::fs::remove_file(entry.path()).await?;
    }
    tokio::fs::remove_dir(legacy).await?;
    Ok(())
}

fn is_non_empty(path: &Path) -> bool {
    std::fs::metadata(path)
        .map(|m| m.len() > 0)
        .unwrap_or(false)
}

async fn finish_completed(
    ctx: &Arc<AppCtx>,
    card: &VideoCard,
    title: &str,
    path: &Path,
    size: u64,
) {
    let record = Record {
        id: card.id.clone(),
        url: card.url.clone(),
        title: title.to_string(),
        rank: card.rank,
        status: "completed".into(),
        path: path.to_string_lossy().to_string(),
        size,
        finished_at: now_rfc3339(),
        error: None,
    };
    if let Err(e) = ctx.upsert_record(record).await {
        log::error!("cannot persist state: {e:#}");
    }
    ctx.update_task(&card.id, |t| {
        t.state = TaskState::Completed;
        t.phase = "completed".into();
        if t.total_segments == 0 {
            t.total_segments = 1;
        }
        t.done_segments = t.total_segments;
        t.speed_kbps = 0.0;
        t.message = "completed".to_string();
        t.path = path.to_string_lossy().to_string();
    });
}

async fn finish_failed(
    ctx: &Arc<AppCtx>,
    cfg: &crate::config::Config,
    card: &VideoCard,
    message: &str,
) {
    // Clean before publishing Failed: a retry must not race with deletion.
    for directory in [
        cfg.temp_path.join(format!("temp_{}", card.id)),
        cfg.save_path.join(format!("temp_{}", card.id)),
    ] {
        if let Err(error) = tokio::fs::remove_dir_all(&directory).await
            && error.kind() != std::io::ErrorKind::NotFound
        {
            log::warn!(
                "cannot clean failed download {}: {error}",
                directory.display()
            );
        }
    }
    // Direct downloads from current and older versions use a title-based .part file.
    match tokio::fs::read_dir(&cfg.save_path).await {
        Ok(mut entries) => {
            let prefix = format!("{} - ", card.id);
            loop {
                match entries.next_entry().await {
                    Ok(Some(entry)) => {
                        let name = entry.file_name();
                        let name = name.to_string_lossy();
                        if name.starts_with(&prefix)
                            && name.ends_with(".mp4.part")
                            && let Err(error) = tokio::fs::remove_file(entry.path()).await
                        {
                            log::warn!(
                                "cannot clean failed download {}: {error}",
                                entry.path().display()
                            );
                        }
                    }
                    Ok(None) => break,
                    Err(error) => {
                        log::warn!("cannot list partial downloads: {error}");
                        break;
                    }
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => log::warn!("cannot list partial downloads: {error}"),
    }

    let record = Record {
        id: card.id.clone(),
        url: card.url.clone(),
        title: card.title.clone(),
        rank: card.rank,
        status: "failed".into(),
        path: String::new(),
        size: 0,
        finished_at: now_rfc3339(),
        error: Some(message.to_string()),
    };
    if let Err(e) = ctx.upsert_record(record).await {
        log::error!("cannot persist state: {e:#}");
    }
    let message = message.to_string();
    ctx.update_task(&card.id, |t| {
        t.state = TaskState::Failed;
        t.phase = "failed".into();
        t.speed_kbps = 0.0;
        t.message = message;
    });
}

/// Current control state of a task (`Cancelled` when it has vanished).
fn control(ctx: &AppCtx, id: &str) -> TaskState {
    ctx.task(id)
        .map(|t| t.state)
        .unwrap_or(TaskState::Cancelled)
}

// ─────────────────────────────────────────────────────────────────────────────
// HLS
// ─────────────────────────────────────────────────────────────────────────────

/// Run one HLS download. Returns the outcome plus the file that was actually
/// written — without ffmpeg the merge lands on a `.ts`, not a `.mp4`.
#[allow(clippy::too_many_arguments)]
async fn run_hls(
    ctx: &Arc<AppCtx>,
    fetch: &Fetcher,
    id: &str,
    title: &str,
    page_url: &str,
    playlist_url: &str,
    temp_dir: &Path,
    final_path: &Path,
    cfg: &crate::config::Config,
) -> anyhow::Result<(Outcome, PathBuf)> {
    ctx.update_task(id, |t| {
        t.phase = "playlist".into();
        t.message = "reading playlist".into();
    });

    let info = stream::resolve_playlist(fetch, page_url, playlist_url, &cfg.resolution)
        .await
        .map_err(|e| anyhow::anyhow!("playlist: {e}"))?;
    if info.segments.is_empty() {
        anyhow::bail!("playlist contained no segments");
    }
    log::info!(
        "[{id}] {} segments, {:.1} min",
        info.segments.len(),
        info.total_duration / 60.0
    );

    // Decryption key, when the playlist declares one.
    let key = match info.key_url.as_deref() {
        Some(key_url) => {
            let resp = fetch.media_response(key_url, page_url, None).await?;
            let data = resp.bytes().await?;
            validate_media_body(key_url, &data)?;
            if data.len() != 16 {
                anyhow::bail!("unexpected AES key length {} for {key_url}", data.len());
            }
            Some(data.to_vec())
        }
        None => None,
    };

    tokio::fs::create_dir_all(temp_dir).await?;

    // fMP4 init segment must lead the merged output.
    if let Some(init) = &info.init_segment {
        let init_path = temp_dir.join("init.mp4");
        if !is_non_empty(&init_path) {
            let resp = fetch
                .media_response(&init.url, page_url, init.byte_range)
                .await?;
            let mut data = resp.bytes().await?.to_vec();
            validate_media_body(&init.url, &data)?;
            if let Some(key) = key.as_deref() {
                data = decrypt(&data, key, &info.iv, 0)?;
            }
            tokio::fs::write(&init_path, &data).await?;
        }
    }

    ctx.update_task(id, |t| {
        t.total_segments = info.segments.len();
        t.phase = "downloading".into();
        t.message = format!("0/{} segments", info.segments.len());
    });

    let outcome = download_segments(
        ctx,
        fetch,
        id,
        title,
        page_url,
        &info,
        key.as_deref(),
        temp_dir,
        cfg.segment_concurrency,
    )
    .await?;

    match outcome {
        Outcome::Done => {}
        other => return Ok((other, final_path.to_path_buf())),
    }

    ctx.update_task(id, |t| {
        t.phase = "merging".into();
        t.message = "merging segments".into();
        t.speed_kbps = 0.0;
    });

    let is_fmp4 = info.init_segment.is_some();
    let temp = temp_dir.to_path_buf();
    let final_owned = final_path.to_path_buf();
    let total = info.segments.len();
    let produced =
        tokio::task::spawn_blocking(move || merge_segments(&temp, total, &final_owned, is_fmp4))
            .await??;

    let _ = tokio::fs::remove_dir_all(temp_dir).await;
    Ok((Outcome::Done, produced))
}

#[allow(clippy::too_many_arguments)]
async fn download_segments(
    ctx: &Arc<AppCtx>,
    fetch: &Fetcher,
    id: &str,
    title: &str,
    page_url: &str,
    info: &M3u8Info,
    key: Option<&[u8]>,
    temp_dir: &Path,
    segment_concurrency: usize,
) -> anyhow::Result<Outcome> {
    let total = info.segments.len();
    let semaphore = Arc::new(Semaphore::new(segment_concurrency.clamp(1, 32)));
    let done = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let finished = Arc::new(AtomicBool::new(false));

    // Periodic progress reporter so the UI speed stays live between segments.
    let reporter = {
        let ctx = Arc::clone(ctx);
        let id = id.to_string();
        let done = Arc::clone(&done);
        let bytes = Arc::clone(&bytes);
        let finished = Arc::clone(&finished);
        tokio::spawn(async move {
            let mut tracker = SpeedTracker::new(Duration::from_secs(3));
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                if finished.load(Ordering::Relaxed) {
                    break;
                }
                let state = control(&ctx, &id);
                if state != TaskState::Running {
                    break;
                }
                let total_bytes = bytes.load(Ordering::Relaxed);
                let speed = tracker.sample(Instant::now(), total_bytes);
                let done_now = done.load(Ordering::Relaxed) as usize;
                ctx.update_task(&id, |t| {
                    t.done_segments = done_now;
                    t.downloaded_bytes = total_bytes;
                    t.speed_kbps = speed;
                    t.message = format!("{done_now}/{total} segments");
                });
            }
        })
    };

    let mut handles = Vec::with_capacity(total);
    let rejected = Arc::new(AtomicBool::new(false));
    for (index, segment) in info.segments.iter().enumerate() {
        let permit = Arc::clone(&semaphore);
        let fetch = fetch.clone();
        let ctx = Arc::clone(ctx);
        let id = id.to_string();
        let page_url = page_url.to_string();
        let dir = temp_dir.to_path_buf();
        let segment = segment.clone();
        let key = key.map(|k| k.to_vec());
        let iv = info.iv.clone();
        let done = Arc::clone(&done);
        let bytes = Arc::clone(&bytes);
        let rejected = Arc::clone(&rejected);

        handles.push(tokio::spawn(async move {
            let _permit = permit.acquire().await.expect("semaphore closed");
            if rejected.load(Ordering::Relaxed) || control(&ctx, &id) != TaskState::Running {
                anyhow::bail!("paused, cancelled or media rejected");
            }
            let path = dir.join(format!("{index}.ts"));
            if is_non_empty(&path) {
                done.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            fetch_segment(
                &fetch,
                &page_url,
                &segment,
                &path,
                key.as_deref(),
                &iv,
                index,
                &bytes,
            )
            .await
            .inspect_err(|e| {
                if e.is::<MediaRejected>() {
                    rejected.store(true, Ordering::Relaxed);
                }
            })?;
            done.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));
    }

    let mut failed: Vec<usize> = Vec::new();
    let mut last_error = String::new();
    for (index, handle) in handles.into_iter().enumerate() {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if e.is::<MediaRejected>() || !rejected.load(Ordering::Relaxed) {
                    last_error = format!("{e:#}");
                }
                failed.push(index);
            }
            Err(e) => {
                log::debug!("[{id}] segment {index} task panicked: {e}");
                failed.push(index);
            }
        }
    }

    // Retry transient failures a few times before giving up.
    let mut attempt = 0;
    while !failed.is_empty() && attempt < 3 && !rejected.load(Ordering::Relaxed) {
        attempt += 1;
        tokio::time::sleep(Duration::from_millis(300 * attempt as u64)).await;
        let state = control(ctx, id);
        if state != TaskState::Running {
            break;
        }
        let mut still_failed = Vec::new();
        for &index in &failed {
            let path = temp_dir.join(format!("{index}.ts"));
            match fetch_segment(
                fetch,
                page_url,
                &info.segments[index],
                &path,
                key,
                &info.iv,
                index,
                &bytes,
            )
            .await
            {
                Ok(()) => {
                    done.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    log::debug!("[{id}] segment {index} retry {attempt}/3 failed: {e:#}");
                    last_error = format!("{e:#}");
                    if e.is::<MediaRejected>() {
                        rejected.store(true, Ordering::Relaxed);
                        break;
                    }
                    still_failed.push(index);
                }
            }
        }
        failed = still_failed;
    }

    finished.store(true, Ordering::Relaxed);
    let _ = reporter.await;

    // The reporter ticks once a second, so the last few segments may not have
    // been folded into the UI yet; publish the final count explicitly.
    let final_done = done.load(Ordering::Relaxed) as usize;
    let final_bytes = bytes.load(Ordering::Relaxed);
    ctx.update_task(id, |t| {
        t.done_segments = final_done;
        t.downloaded_bytes = final_bytes;
    });

    match control(ctx, id) {
        TaskState::Cancelled => Ok(Outcome::Cancelled),
        TaskState::Paused => Ok(Outcome::Paused),
        _ if rejected.load(Ordering::Relaxed) => anyhow::bail!("{last_error}"),
        _ if !failed.is_empty() => {
            anyhow::bail!(
                "{} segment(s) failed after 3 retries: {last_error}",
                failed.len()
            )
        }
        _ => {
            let _ = title;
            Ok(Outcome::Done)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_segment(
    fetch: &Fetcher,
    page_url: &str,
    segment: &Segment,
    path: &Path,
    key: Option<&[u8]>,
    iv: &Option<Vec<u8>>,
    index: usize,
    bytes: &AtomicU64,
) -> anyhow::Result<()> {
    let resp = fetch
        .media_response(&segment.url, page_url, segment.byte_range)
        .await?;

    let mut data = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        data.extend_from_slice(&chunk);
    }
    validate_media_body(&segment.url, &data)?;

    let data = match key {
        Some(key) => decrypt(&data, key, iv, index)?,
        None => data,
    };

    // SupJav prefixes every segment with a decoy image header. If no MPEG-TS
    // sync run is found the payload is not a decoy-wrapped TS fragment (an
    // fMP4 part, for instance), so it is stored untouched.
    let stripped = strip_fake_header(&data);
    let payload: &[u8] = if stripped.is_empty() && !data.is_empty() {
        log::debug!("segment {index}: no MPEG-TS sync found, keeping the raw payload");
        &data
    } else {
        stripped
    };

    // Write via a temp name so a crash mid-write cannot look like a
    // complete segment on the next resume.
    let tmp = path.with_extension("ts.part");
    tokio::fs::write(&tmp, payload).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

/// AES-128-CBC decrypt one segment. TS payloads are block aligned, but a
/// truncated final block is dropped rather than failing the whole segment.
fn decrypt(data: &[u8], key: &[u8], iv: &Option<Vec<u8>>, index: usize) -> anyhow::Result<Vec<u8>> {
    use aes::cipher::{BlockModeDecrypt, KeyIvInit, block_padding::NoPadding};
    type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;

    if key.len() != 16 {
        anyhow::bail!("unexpected AES key length {}", key.len());
    }
    let usable = data.len() - (data.len() % 16);
    if usable == 0 {
        return Ok(Vec::new());
    }
    let mut buf = data[..usable].to_vec();
    let iv = iv_for_segment(index, iv);
    let cipher =
        Aes128CbcDec::new_from_slices(key, &iv).map_err(|e| anyhow::anyhow!("cipher init: {e}"))?;
    let out = cipher
        .decrypt_padded::<NoPadding>(&mut buf)
        .map_err(|e| anyhow::anyhow!("decrypt: {e}"))?;
    Ok(out.to_vec())
}

// ─────────────────────────────────────────────────────────────────────────────
// Merge
// ─────────────────────────────────────────────────────────────────────────────

/// Merge the downloaded segments into `final_path`, returning the file that was
/// actually produced.
///
/// fMP4 (declared via `#EXT-X-MAP`) is concatenated binary-wise because the
/// concat demuxer cannot handle it; TS goes through ffmpeg's concat demuxer
/// with a plain binary concatenation as the fallback. Without ffmpeg the
/// fallback writes `final_path` with a `.ts` extension instead, since the
/// result is a raw MPEG-TS stream rather than an MP4 container.
pub fn merge_segments(
    temp_dir: &Path,
    total: usize,
    final_path: &Path,
    is_fmp4: bool,
) -> anyhow::Result<PathBuf> {
    let ffmpeg = which_ffmpeg();

    if is_fmp4 {
        let raw = temp_dir.join("raw_fragmented.mp4");
        {
            use std::io::Write;
            let mut writer = std::io::BufWriter::new(std::fs::File::create(&raw)?);
            let init = temp_dir.join("init.mp4");
            if !init.exists() {
                anyhow::bail!("fMP4 init segment is missing");
            }
            std::io::copy(&mut std::fs::File::open(&init)?, &mut writer)?;
            for i in 0..total {
                let mut f = std::fs::File::open(temp_dir.join(format!("{i}.ts")))?;
                std::io::copy(&mut f, &mut writer)?;
            }
            writer.flush()?;
        }

        let remuxed = ffmpeg
            .as_ref()
            .map(|ffmpeg| {
                let out = std::process::Command::new(ffmpeg)
                    .args(["-y", "-loglevel", "error", "-i"])
                    .arg(&raw)
                    .args(["-c", "copy", "-movflags", "+faststart"])
                    .arg(final_path)
                    .output();
                match out {
                    Ok(o) if o.status.success() => true,
                    Ok(o) => {
                        log::debug!(
                            "ffmpeg fMP4 remux failed: {}",
                            String::from_utf8_lossy(&o.stderr).trim()
                        );
                        false
                    }
                    Err(e) => {
                        log::debug!("cannot run ffmpeg: {e}");
                        false
                    }
                }
            })
            .unwrap_or(false);

        if !remuxed {
            // Still a valid (fragmented) MP4 — copy it into place.
            std::fs::copy(&raw, final_path)?;
        }
        let _ = std::fs::remove_file(&raw);
        return Ok(final_path.to_path_buf());
    }

    if let Some(ffmpeg) = ffmpeg.as_ref() {
        let concat_file = temp_dir.join("concat.txt");
        let mut listing = String::new();
        for i in 0..total {
            listing.push_str(&format!("file '{i}.ts'\n"));
        }
        std::fs::write(&concat_file, listing)?;
        match std::process::Command::new(ffmpeg)
            .args([
                "-y",
                "-loglevel",
                "error",
                "-f",
                "concat",
                "-safe",
                "0",
                "-i",
            ])
            .arg(&concat_file)
            .args([
                "-c",
                "copy",
                "-movflags",
                "+faststart",
                "-avoid_negative_ts",
                "make_zero",
            ])
            .arg(final_path)
            .output()
        {
            Ok(o) if o.status.success() => return Ok(final_path.to_path_buf()),
            Ok(o) => log::warn!(
                "ffmpeg concat failed ({}), falling back to a binary merge",
                String::from_utf8_lossy(&o.stderr).trim()
            ),
            Err(e) => log::warn!("cannot run ffmpeg ({e}), falling back to a binary merge"),
        }
    } else {
        log::info!("ffmpeg not found — merging segments without remuxing");
    }

    // Binary merge: the result is a valid MPEG-TS even without ffmpeg.
    let ts_path = final_path.with_extension("ts");
    {
        use std::io::Write;
        let mut writer = std::io::BufWriter::new(std::fs::File::create(&ts_path)?);
        for i in 0..total {
            let mut f = std::fs::File::open(temp_dir.join(format!("{i}.ts")))?;
            std::io::copy(&mut f, &mut writer)?;
        }
        writer.flush()?;
    }
    log::info!("merged into {}", ts_path.display());
    Ok(ts_path)
}

fn which_ffmpeg() -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join("ffmpeg"))
        .find(|candidate| candidate.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decrypts_aes_cbc_round_trip() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::NoPadding};
        type Enc = cbc::Encryptor<aes::Aes128>;

        let key = [7u8; 16];
        let iv = iv_for_segment(0, &None);
        let plaintext = vec![0x47u8; 32];
        let mut buf = plaintext.clone();
        let len = buf.len();
        let enc = Enc::new_from_slices(&key, &iv).unwrap();
        let ciphertext = enc
            .encrypt_padded::<NoPadding>(&mut buf, len)
            .unwrap()
            .to_vec();

        let out = decrypt(&ciphertext, &key, &None, 0).unwrap();
        assert_eq!(out, plaintext);
    }

    #[test]
    fn decrypt_uses_segment_index_as_iv() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::NoPadding};
        type Enc = cbc::Encryptor<aes::Aes128>;

        let key = [3u8; 16];
        let plaintext = vec![9u8; 16];
        let mut buf = plaintext.clone();
        let len = buf.len();
        let enc = Enc::new_from_slices(&key, &iv_for_segment(5, &None)).unwrap();
        let ciphertext = enc
            .encrypt_padded::<NoPadding>(&mut buf, len)
            .unwrap()
            .to_vec();

        assert_eq!(decrypt(&ciphertext, &key, &None, 5).unwrap(), plaintext);
        // Wrong index → wrong IV → garbage, never the plaintext.
        assert_ne!(decrypt(&ciphertext, &key, &None, 6).unwrap(), plaintext);
    }

    #[test]
    fn decrypt_drops_a_trailing_partial_block() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::NoPadding};
        type Enc = cbc::Encryptor<aes::Aes128>;
        let key = [1u8; 16];
        let mut buf = vec![5u8; 16];
        let len = buf.len();
        let enc = Enc::new_from_slices(&key, &iv_for_segment(0, &None)).unwrap();
        let mut ciphertext = enc
            .encrypt_padded::<NoPadding>(&mut buf, len)
            .unwrap()
            .to_vec();
        ciphertext.extend_from_slice(&[0xde, 0xad]); // truncated tail
        let out = decrypt(&ciphertext, &key, &None, 0).unwrap();
        assert_eq!(out.len(), 16);
    }

    #[test]
    fn rejects_wrong_key_length() {
        assert!(decrypt(&[0u8; 16], &[0u8; 8], &None, 0).is_err());
    }

    #[test]
    fn binary_merge_concatenates_segments() {
        let dir = std::env::temp_dir().join(format!("javd-merge-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..3 {
            std::fs::write(dir.join(format!("{i}.ts")), vec![i as u8; 4]).unwrap();
        }
        let final_path = dir.join("out.mp4");
        // Force the fMP4 path, which concatenates without needing ffmpeg.
        std::fs::write(dir.join("init.mp4"), [0xff, 0xfe]).unwrap();
        let produced = merge_segments(&dir, 3, &final_path, true).unwrap();
        assert_eq!(produced, final_path);
        let merged = std::fs::read(&produced).unwrap();
        // 2 init bytes + 3 segments × 4 bytes (an ffmpeg remux may rewrite the
        // container, so only the lower bound is asserted).
        assert!(merged.len() >= 14, "got {} bytes", merged.len());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ts_merge_falls_back_to_a_dot_ts_file_without_ffmpeg() {
        // Only meaningful when ffmpeg is unavailable; with ffmpeg present the
        // concat demuxer rejects these fake segments and we still fall back.
        let dir = std::env::temp_dir().join(format!("javd-merge-ts-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..2 {
            std::fs::write(dir.join(format!("{i}.ts")), vec![0x47u8; 188]).unwrap();
        }
        let final_path = dir.join("out.mp4");
        let produced = merge_segments(&dir, 2, &final_path, false).unwrap();
        // Whatever path is returned must actually exist and be non-empty.
        assert!(produced.exists(), "{} should exist", produced.display());
        assert!(std::fs::metadata(&produced).unwrap().len() > 0);
        assert!(produced == final_path || produced.extension().unwrap() == "ts");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_init_segment_is_an_error() {
        let dir = std::env::temp_dir().join(format!("javd-merge-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        assert!(merge_segments(&dir, 0, &dir.join("out.mp4"), true).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
