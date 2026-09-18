//! The download engine: resolve a video page, fetch its HLS segments
//! concurrently, decrypt and merge them into a single file.
//!
//! Every long-running step is interruptible: pause keeps the temp directory so
//! a later resume only fetches the missing segments.

mod merge;

use anyhow::Context;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio_util::task::AbortOnDropHandle;

use crate::jav::app::{AppCtx, TaskState, now_rfc3339};
use crate::jav::source::http::{Fetcher, MediaRejected, validate_media_body};
use crate::jav::source::scraper::VideoCard;
use crate::jav::source::stream::m3u8::{M3u8Info, Segment};
use crate::jav::source::stream::{self, StreamKind};
use crate::jav::storage::Record;
use crate::jav::util::{SpeedTracker, cloudflare_hint, sanitize_filename, strip_fake_header};
use crate::runtime::BackgroundTask;

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

    log::info!("[{id}] download started");

    // A listing or manual request may already supply the final title. Adopt
    // that output before contacting the source, even if the source is offline.
    let known_path = cfg
        .save_path
        .join(format!("{id} - {}.mp4", sanitize_filename(&card.title)));
    if let Some((path, size)) = existing_output(&known_path).await {
        finish_completed(&ctx, &card, &card.title, &path, size).await;
        return;
    }

    // ── resolve ──────────────────────────────────────────────────────────
    log::debug!("[{id}] resolving stream resolution={}", cfg.resolution);
    let resolved =
        match stream::resolve_stream(&fetch, &card.url, &cfg.resolution, cfg.min_duration_secs)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                let msg = cloudflare_hint(&format!("{e:#}"));
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
    if let Some((path, size)) = existing_output(&final_path).await {
        log::info!("[{id}] output already exists at {}", path.display());
        finish_completed(&ctx, &card, &title, &path, size).await;
        return;
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
                &card.url,
                &resolved.playlist,
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
            ctx.update_task(&id, |t| {
                t.state = TaskState::Cancelled;
                t.phase = "cancelled".into();
                t.speed_kbps = 0.0;
                t.message = "cancelled".into();
            });
        }
        Err(e) => {
            // The merger owns its staging file; preserve any previous output
            // when a replacement fails or is interrupted.
            let msg = cloudflare_hint(&format!("{e:#}"));
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

async fn existing_output(path: &Path) -> Option<(PathBuf, u64)> {
    for path in [path.to_path_buf(), path.with_extension("ts")] {
        let Ok(metadata) = tokio::fs::metadata(&path).await else {
            continue;
        };
        if !metadata.is_file() || metadata.len() == 0 {
            continue;
        }
        let candidate = path.clone();
        match tokio::task::spawn_blocking(move || merge::validate_output(&candidate, None)).await {
            Ok(Ok(())) => return Some((path, metadata.len())),
            result => log::warn!(
                "existing output {} failed media validation: {result:?}",
                path.display()
            ),
        }
    }
    None
}

async fn finish_completed(
    ctx: &Arc<AppCtx>,
    card: &VideoCard,
    title: &str,
    path: &Path,
    size: u64,
) {
    log::info!(
        "[{}] download complete bytes={size} path={}",
        card.id,
        path.display()
    );
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
        t.title = title.to_string();
        t.downloaded_bytes = size;
        t.total_bytes = size;
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
    cfg: &crate::jav::config::Config,
    card: &VideoCard,
    message: &str,
) {
    log::error!("[{}] download failed: {message}", card.id);
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

/// Download both selected tracks before merging and validating the final MP4.
#[allow(clippy::too_many_arguments)]
async fn run_hls(
    ctx: &Arc<AppCtx>,
    fetch: &Fetcher,
    id: &str,
    page_url: &str,
    info: &M3u8Info,
    temp_dir: &Path,
    final_path: &Path,
    cfg: &crate::jav::config::Config,
) -> anyhow::Result<(Outcome, PathBuf)> {
    tokio::task::spawn_blocking(merge::require_tools).await??;
    let total = info.segments.len() + info.audio.as_ref().map_or(0, |audio| audio.segments.len());
    log::debug!(
        "[{id}] HLS prepared segments={total} separate_audio={} duration_secs={:.1} workers={}",
        info.audio.is_some(),
        info.total_duration,
        cfg.segment_concurrency.clamp(1, 32)
    );
    ctx.update_task(id, |t| {
        t.total_segments = total;
        t.done_segments = 0;
        t.downloaded_bytes = 0;
        t.phase = "downloading".into();
        t.message = format!("0/{total} segments");
    });
    let audio_dir = temp_dir.join("audio");
    let tracks = std::iter::once((info, temp_dir)).chain(
        info.audio
            .as_deref()
            .map(|audio| (audio, audio_dir.as_path())),
    );
    for (track, dir) in tracks {
        match control(ctx, id) {
            TaskState::Paused => return Ok((Outcome::Paused, final_path.to_path_buf())),
            TaskState::Cancelled => return Ok((Outcome::Cancelled, final_path.to_path_buf())),
            _ => {}
        }
        let prepared = while_running(
            ctx,
            id,
            prepare_track(fetch, &ctx.download_limiter, page_url, track, dir),
        )
        .await;
        match control(ctx, id) {
            TaskState::Paused => return Ok((Outcome::Paused, final_path.to_path_buf())),
            TaskState::Cancelled => return Ok((Outcome::Cancelled, final_path.to_path_buf())),
            _ => {}
        }
        let keys = prepared?;
        let outcome = download_segments(
            ctx,
            fetch,
            id,
            page_url,
            track,
            keys,
            dir,
            cfg.segment_concurrency,
        )
        .await?;
        if !matches!(outcome, Outcome::Done) {
            return Ok((outcome, final_path.to_path_buf()));
        }
    }
    ctx.update_task(id, |t| {
        t.phase = "merging".into();
        t.message = "merging and validating audio/video".into();
        t.speed_kbps = 0.0;
    });
    log::debug!("[{id}] merging and validating {total} segments");
    let merge_started = Instant::now();
    let temp = temp_dir.to_path_buf();
    let final_owned = final_path.to_path_buf();
    let info = info.clone();
    let merge_ctx = Arc::clone(ctx);
    let merge_id = id.to_string();
    let result = tokio::task::spawn_blocking(move || {
        merge::merge_segments(&temp, &info, &final_owned, || {
            control(&merge_ctx, &merge_id) == TaskState::Running
        })
    })
    .await?;
    match control(ctx, id) {
        TaskState::Paused => return Ok((Outcome::Paused, final_path.to_path_buf())),
        TaskState::Cancelled => return Ok((Outcome::Cancelled, final_path.to_path_buf())),
        _ => {}
    }
    let produced = result?;
    log::debug!(
        "[{id}] merge complete elapsed_ms={}",
        merge_started.elapsed().as_millis()
    );
    let _ = tokio::fs::remove_dir_all(temp_dir).await;
    Ok((Outcome::Done, produced))
}

/// Dropping a throttled transfer on pause/cancel leaves only completed segments
/// in the cache, and releases its bandwidth waiter immediately.
async fn while_running<T>(
    ctx: &AppCtx,
    id: &str,
    work: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    let mut changes = ctx.subscribe();
    let stopped = async {
        loop {
            if control(ctx, id) != TaskState::Running {
                return;
            }
            if changes.recv().await.is_err_and(|error| {
                matches!(error, tokio::sync::broadcast::error::RecvError::Closed)
            }) {
                return;
            }
        }
    };
    tokio::select! {
        biased;
        _ = stopped => anyhow::bail!("download paused or cancelled"),
        result = work => result,
    }
}

/// Cache segments only while their URLs, ranges and encryption metadata match.
async fn prepare_track(
    fetch: &Fetcher,
    limiter: &crate::runtime::download_limiter::DownloadLimiter,
    page_url: &str,
    info: &M3u8Info,
    dir: &Path,
) -> anyhow::Result<Arc<HashMap<String, Vec<u8>>>> {
    anyhow::ensure!(!info.segments.is_empty(), "playlist contained no segments");
    let manifest = serde_json::to_vec(&(&info.segments, &info.init_segment))?;
    let manifest_path = dir.join("playlist.json");
    if tokio::fs::read(&manifest_path).await.ok().as_deref() != Some(&manifest) {
        if tokio::fs::try_exists(dir).await? {
            tokio::fs::remove_dir_all(dir).await?;
        }
        tokio::fs::create_dir_all(dir).await?;
        tokio::fs::write(&manifest_path, manifest).await?;
    }
    let mut keys = HashMap::new();
    for encryption in info
        .init_segment
        .iter()
        .chain(&info.segments)
        .filter_map(|s| s.encryption.as_ref())
    {
        if keys.contains_key(&encryption.key_url) {
            continue;
        }
        let resp = fetch
            .media_response(&encryption.key_url, page_url, None)
            .await?;
        let data = resp.bytes().await?;
        validate_media_body(&encryption.key_url, &data)?;
        anyhow::ensure!(data.len() == 16, "unexpected AES key length {}", data.len());
        keys.insert(encryption.key_url.clone(), data.to_vec());
    }
    if let Some(init) = &info.init_segment {
        let path = dir.join("init.mp4");
        if !is_non_empty(&path) {
            fetch_segment(
                fetch,
                limiter,
                page_url,
                init,
                &path,
                &keys,
                &AtomicU64::new(0),
            )
            .await?;
        }
    }
    Ok(Arc::new(keys))
}

#[allow(clippy::too_many_arguments)]
async fn download_segments(
    ctx: &Arc<AppCtx>,
    fetch: &Fetcher,
    id: &str,
    page_url: &str,
    info: &M3u8Info,
    keys: Arc<HashMap<String, Vec<u8>>>,
    temp_dir: &Path,
    segment_concurrency: usize,
) -> anyhow::Result<Outcome> {
    let task = ctx
        .task(id)
        .ok_or_else(|| anyhow::anyhow!("download task disappeared"))?;
    let total = task.total_segments;
    let done = Arc::new(AtomicU64::new(task.done_segments as u64));
    let bytes = Arc::new(AtomicU64::new(task.downloaded_bytes));

    // Periodic progress reporter so the UI speed stays live between segments.
    let reporter = {
        let ctx = Arc::clone(ctx);
        let id = id.to_string();
        let done = Arc::clone(&done);
        let bytes = Arc::clone(&bytes);
        BackgroundTask::spawn("JAV progress reporter", async move {
            let mut tracker = SpeedTracker::new(Duration::from_secs(3));
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
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

    let rejected = Arc::new(AtomicBool::new(false));
    let mut failed: Vec<usize> = Vec::new();
    let mut last_error = String::new();
    let mut segments = info.segments.iter().enumerate();
    let mut downloads = FuturesUnordered::new();
    // Spawn only the configured number of workers, rather than allocating a
    // waiting task for every segment. Dropping the stream aborts its workers.
    loop {
        while downloads.len() < segment_concurrency.clamp(1, 32) {
            let Some((index, segment)) = segments.next() else {
                break;
            };
            let fetch = fetch.clone();
            let ctx = Arc::clone(ctx);
            let id = id.to_string();
            let page_url = page_url.to_string();
            let dir = temp_dir.to_path_buf();
            let segment = segment.clone();
            let keys = Arc::clone(&keys);
            let done = Arc::clone(&done);
            let bytes = Arc::clone(&bytes);
            let rejected = Arc::clone(&rejected);

            let handle = AbortOnDropHandle::new(tokio::spawn(async move {
                if rejected.load(Ordering::Relaxed) || control(&ctx, &id) != TaskState::Running {
                    anyhow::bail!("paused, cancelled or media rejected");
                }
                let path = dir.join(format!("{index}.ts"));
                if is_non_empty(&path) {
                    log::trace!("[{id}] segment={index} cached path={}", path.display());
                    done.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                log::trace!("[{id}] segment={index} fetching path={}", path.display());
                let started = Instant::now();
                while_running(
                    &ctx,
                    &id,
                    fetch_segment(
                        &fetch,
                        &ctx.download_limiter,
                        &page_url,
                        &segment,
                        &path,
                        &keys,
                        &bytes,
                    ),
                )
                .await
                .inspect_err(|e| {
                    if e.is::<MediaRejected>() {
                        rejected.store(true, Ordering::Relaxed);
                    }
                })?;
                log::trace!(
                    "[{id}] segment={index} complete elapsed_ms={}",
                    started.elapsed().as_millis()
                );
                done.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }));
            downloads.push(async move { (index, handle.await) });
        }
        let Some((index, result)) = downloads.next().await else {
            break;
        };
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                log::debug!("[{id}] segment={index} fetch failed: {e:#}");
                if e.is::<MediaRejected>() || !rejected.load(Ordering::Relaxed) {
                    last_error = format!("{e:#}");
                }
                failed.push(index);
            }
            Err(e) => {
                log::error!("[{id}] segment {index} task failed: {e}");
                failed.push(index);
            }
        }
    }

    // Retry transient failures in playlist order a few times before giving up.
    failed.sort_unstable();
    let mut attempt = 0;
    while !failed.is_empty() && attempt < 3 && !rejected.load(Ordering::Relaxed) {
        attempt += 1;
        log::debug!(
            "[{id}] retry round={attempt}/3 failed_segments={}",
            failed.len()
        );
        tokio::time::sleep(Duration::from_millis(300 * attempt as u64)).await;
        let state = control(ctx, id);
        if state != TaskState::Running {
            break;
        }
        let mut still_failed = Vec::new();
        for &index in &failed {
            let path = temp_dir.join(format!("{index}.ts"));
            match while_running(
                ctx,
                id,
                fetch_segment(
                    fetch,
                    &ctx.download_limiter,
                    page_url,
                    &info.segments[index],
                    &path,
                    &keys,
                    &bytes,
                ),
            )
            .await
            {
                Ok(()) => {
                    log::trace!("[{id}] segment={index} retry={attempt}/3 complete");
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

    reporter.abort().await;

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
        _ => Ok(Outcome::Done),
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_segment(
    fetch: &Fetcher,
    limiter: &crate::runtime::download_limiter::DownloadLimiter,
    page_url: &str,
    segment: &Segment,
    path: &Path,
    keys: &HashMap<String, Vec<u8>>,
    bytes: &AtomicU64,
) -> anyhow::Result<()> {
    let resp = fetch
        .download_response(&segment.url, page_url, segment.byte_range)
        .await?;

    let mut data = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = tokio::time::timeout(Duration::from_secs(60), stream.next())
        .await
        .context("media read timed out")?
    {
        let chunk = chunk?;
        for part in chunk.chunks(64 * 1024) {
            limiter
                .acquire(
                    crate::runtime::download_limiter::DownloadModule::Jav,
                    part.len(),
                )
                .await;
            bytes.fetch_add(part.len() as u64, Ordering::Relaxed);
            data.extend_from_slice(part);
        }
    }
    validate_media_body(&segment.url, &data)?;

    if let Some((_, length)) = segment.byte_range {
        anyhow::ensure!(
            data.len() as u64 == length,
            "incomplete HLS byte range for {}",
            segment.url
        );
    }
    let data = if let Some(encryption) = &segment.encryption {
        let key = keys
            .get(&encryption.key_url)
            .ok_or_else(|| anyhow::anyhow!("segment key is missing"))?;
        decrypt(&data, key, &encryption.iv)?
    } else {
        data
    };

    // SupJav prefixes every segment with a decoy image header. If no MPEG-TS
    // sync run is found the payload is not a decoy-wrapped TS fragment (an
    // fMP4 part, for instance), so it is stored untouched.
    let stripped = strip_fake_header(&data);
    let payload: &[u8] = if stripped.is_empty() && !data.is_empty() {
        log::trace!(
            "segment {}: no MPEG-TS sync found, keeping the raw payload",
            path.display()
        );
        &data
    } else {
        stripped
    };

    // Write via a temp name so a crash mid-write cannot look like a
    // complete segment on the next resume.
    let tmp = path.with_extension("part");
    tokio::fs::write(&tmp, payload).await?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

/// HLS AES-128 uses PKCS#7 padding. Reject truncation so the segment is retried.
fn decrypt(data: &[u8], key: &[u8], iv: &[u8; 16]) -> anyhow::Result<Vec<u8>> {
    use aes::cipher::{BlockModeDecrypt, KeyIvInit, block_padding::Pkcs7};
    type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;
    anyhow::ensure!(key.len() == 16, "unexpected AES key length {}", key.len());
    anyhow::ensure!(
        !data.is_empty() && data.len().is_multiple_of(16),
        "truncated AES segment"
    );
    let mut buf = data.to_vec();
    let cipher =
        Aes128CbcDec::new_from_slices(key, iv).map_err(|e| anyhow::anyhow!("cipher init: {e}"))?;
    let out = cipher
        .decrypt_padded::<Pkcs7>(&mut buf)
        .map_err(|e| anyhow::anyhow!("decrypt: {e}"))?;
    anyhow::ensure!(!out.is_empty(), "empty decrypted segment");
    Ok(out.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jav::source::stream::m3u8::parse_media_m3u8;
    use crate::jav::util::iv_for_segment;

    #[test]
    fn decrypts_aes_cbc_round_trip() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
        type Enc = cbc::Encryptor<aes::Aes128>;

        let key = [7u8; 16];
        let iv = iv_for_segment(0, &None);
        let plaintext = vec![0x47u8; 32];
        let mut buf = plaintext.clone();
        let len = buf.len();
        buf.resize(len + 16, 0);
        let enc = Enc::new_from_slices(&key, &iv).unwrap();
        let ciphertext = enc.encrypt_padded::<Pkcs7>(&mut buf, len).unwrap().to_vec();

        let out = decrypt(&ciphertext, &key, &iv_for_segment(0, &None)).unwrap();
        assert_eq!(out, plaintext);
    }

    #[test]
    fn decrypt_uses_segment_index_as_iv() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
        type Enc = cbc::Encryptor<aes::Aes128>;

        let key = [3u8; 16];
        let plaintext = vec![9u8; 16];
        let mut buf = plaintext.clone();
        let len = buf.len();
        buf.resize(len + 16, 0);
        let enc = Enc::new_from_slices(&key, &iv_for_segment(5, &None)).unwrap();
        let ciphertext = enc.encrypt_padded::<Pkcs7>(&mut buf, len).unwrap().to_vec();

        assert_eq!(
            decrypt(&ciphertext, &key, &iv_for_segment(5, &None)).unwrap(),
            plaintext
        );
        // Wrong index → wrong IV → garbage, never the plaintext.
        assert_ne!(
            decrypt(&ciphertext, &key, &iv_for_segment(6, &None)).unwrap(),
            plaintext
        );
    }

    #[test]
    fn decrypt_rejects_a_trailing_partial_block() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit, block_padding::Pkcs7};
        type Enc = cbc::Encryptor<aes::Aes128>;
        let key = [1u8; 16];
        let mut buf = vec![5u8; 16];
        let len = buf.len();
        buf.resize(len + 16, 0);
        let enc = Enc::new_from_slices(&key, &iv_for_segment(0, &None)).unwrap();
        let mut ciphertext = enc.encrypt_padded::<Pkcs7>(&mut buf, len).unwrap().to_vec();
        ciphertext.extend_from_slice(&[0xde, 0xad]); // truncated tail
        assert!(decrypt(&ciphertext, &key, &iv_for_segment(0, &None)).is_err());
    }

    #[test]
    fn rejects_wrong_key_length() {
        assert!(decrypt(&[0u8; 16], &[0u8; 8], &iv_for_segment(0, &None)).is_err());
    }

    #[test]
    fn binary_merge_concatenates_segments() {
        let dir = std::env::temp_dir().join(format!("javd-merge-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..3 {
            std::fs::write(dir.join(format!("{i}.ts")), vec![i as u8; 4]).unwrap();
        }
        let info = parse_media_m3u8("#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\n0.ts\n#EXTINF:1,\n1.ts\n#EXTINF:1,\n2.ts", &url::Url::parse("https://example.com/media.m3u8").unwrap()).unwrap();
        std::fs::write(dir.join("init.mp4"), [0xff, 0xfe]).unwrap();
        let produced = merge::assemble_track(&dir, &info, || true).unwrap();
        let merged = std::fs::read(produced.path()).unwrap();
        assert_eq!(merged, [0xff, 0xfe, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ts_merge_rejects_invalid_media() {
        let dir = std::env::temp_dir().join(format!("javd-merge-ts-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        for i in 0..2 {
            std::fs::write(dir.join(format!("{i}.ts")), vec![0x47u8; 188]).unwrap();
        }
        let final_path = dir.join("out.mp4");
        let info = parse_media_m3u8(
            "#EXTM3U\n#EXTINF:1,\n0.ts\n#EXTINF:1,\n1.ts",
            &url::Url::parse("https://example.com/media.m3u8").unwrap(),
        )
        .unwrap();
        assert!(merge::merge_segments(&dir, &info, &final_path, || true).is_err());
        assert!(!final_path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_init_segment_is_an_error() {
        let dir = std::env::temp_dir().join(format!("javd-merge-bad-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let info = parse_media_m3u8(
            "#EXTM3U\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:1,\n0.ts",
            &url::Url::parse("https://example.com/media.m3u8").unwrap(),
        )
        .unwrap();
        assert!(merge::assemble_track(&dir, &info, || true).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
