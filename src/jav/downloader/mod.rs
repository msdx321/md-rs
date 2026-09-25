//! The download engine: resolve a video page, fetch its HLS segments
//! concurrently, decrypt and merge them into a single file.
//!
//! Every long-running step is interruptible: pause keeps the temp directory so
//! a later resume only fetches the missing segments.

mod merge;
mod segment;

use anyhow::Context;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio_util::task::AbortOnDropHandle;

use crate::jav::app::{AppCtx, DownloadRequest, TaskState, now_rfc3339};
use crate::jav::source::http::{Fetcher, MediaRejected, validate_media_body};
use crate::jav::source::scraper::VideoCard;
use crate::jav::source::stream::m3u8::{M3u8Info, Segment};
use crate::jav::source::stream::{self, StreamKind};
use crate::jav::storage::Record;
use crate::jav::util::{SpeedTracker, cloudflare_hint, sanitize_filename};
use crate::runtime::BackgroundTask;

/// How a download run ended.
enum Outcome {
    Done,
    Paused,
    Cancelled,
}

/// Download one video end to end, updating the task registry and the dedup
/// ledger as it goes. Errors are reported through the task, not returned.
pub async fn download_video(ctx: Arc<AppCtx>, card: VideoCard, request: DownloadRequest) {
    let Some(_owner) = ctx.jobs.enter() else {
        return;
    };
    let id = card.id.clone();
    let Some(_slot) = ctx.download_slot(&id, &request).await else {
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
        finish_completed(&ctx, &cfg, &card, &card.title, &path, size).await;
        return;
    }

    // ── resolve ──────────────────────────────────────────────────────────
    let minimum_resolution = ctx.minimum_video_resolution();
    log::debug!("[{id}] resolving stream resolution={}", cfg.resolution);
    let resolved = match while_running(
        &ctx,
        &id,
        stream::resolve_stream(&fetch, &card.url, &cfg.resolution, cfg.min_duration_secs),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            let msg = cloudflare_hint(&format!("{e:#}"));
            finish_failed(&ctx, &cfg, &card, &msg).await;
            return;
        }
    };

    if let Some(reason) = crate::runtime::video_resolution::rejection(
        minimum_resolution,
        resolved
            .playlist
            .variant
            .resolution
            .map(|(w, h)| (w as u64, h as u64)),
    ) {
        if !ctx.begin_terminal_commit(&id) {
            finish_stopped(&ctx, &cfg, &id).await;
            return;
        }
        log::info!("[{id}] {reason}");
        ctx.update_task(&id, |task| {
            task.state = TaskState::Skipped;
            task.phase = "filtered".into();
            task.message = reason;
            task.speed_kbps = 0.0;
        });
        ctx.end_terminal_commit(&id);
        return;
    }

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
        log::debug!("[{id}] output already exists at {}", path.display());
        finish_completed(&ctx, &cfg, &card, &title, &path, size).await;
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

    finish_run(&ctx, &cfg, &card, &title, &produced, outcome).await;
}

/// Final publication and cleanup remain inside the caller's ownership lease.
async fn finish_run(
    ctx: &Arc<AppCtx>,
    cfg: &crate::jav::config::Config,
    card: &VideoCard,
    title: &str,
    produced: &Path,
    outcome: anyhow::Result<Outcome>,
) {
    if finish_stopped(ctx, cfg, &card.id).await {
        return;
    }
    match outcome {
        Ok(Outcome::Done) => {
            let size = tokio::fs::metadata(&produced)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            finish_completed(ctx, cfg, card, title, produced, size).await;
        }
        Ok(Outcome::Paused | Outcome::Cancelled) => {}
        Err(e) => {
            // The merger owns its staging file; preserve any previous output
            // when a replacement fails or is interrupted.
            let msg = cloudflare_hint(&format!("{e:#}"));
            finish_failed(ctx, cfg, card, &msg).await;
        }
    }
}

/// A resume never clears the stop signal; cancellation can still supersede pause
/// while the owner is unwinding. Do not turn an interrupted operation into a
/// destructive failure just because its transfer returned an error.
async fn finish_stopped(ctx: &AppCtx, cfg: &crate::jav::config::Config, id: &str) -> bool {
    let mut paused = false;
    ctx.update_task(id, |task| {
        if task.state == TaskState::Paused {
            paused = true;
            task.phase = "paused".into();
            task.speed_kbps = 0.0;
            task.message = "paused — resume to continue from downloaded segments".into();
        }
    });
    if control(ctx, id) == TaskState::Cancelled {
        let _ = tokio::fs::remove_dir_all(cfg.temp_path.join(format!("temp_{id}"))).await;
        ctx.update_task(id, |task| {
            task.state = TaskState::Cancelled;
            task.phase = "cancelled".into();
            task.speed_kbps = 0.0;
            task.message = "cancelled".into();
        });
        return true;
    }
    paused
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

async fn is_non_empty(path: &Path) -> bool {
    tokio::fs::metadata(path).await.is_ok_and(|m| m.len() > 0)
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
    cfg: &crate::jav::config::Config,
    card: &VideoCard,
    title: &str,
    path: &Path,
    size: u64,
) {
    if !ctx.begin_terminal_commit(&card.id) {
        finish_stopped(ctx, cfg, &card.id).await;
        return;
    }
    log::info!(
        "[{}] download complete bytes={size} path={}",
        card.id,
        path.display()
    );
    let _ = tokio::fs::remove_dir_all(cfg.temp_path.join(format!("temp_{}", card.id))).await;
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
    ctx.end_terminal_commit(&card.id);
}

async fn finish_failed(
    ctx: &Arc<AppCtx>,
    cfg: &crate::jav::config::Config,
    card: &VideoCard,
    message: &str,
) {
    if !ctx.begin_terminal_commit(&card.id) {
        finish_stopped(ctx, cfg, &card.id).await;
        return;
    }
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
    ctx.end_terminal_commit(&card.id);
}

/// Current control state of a task (`Cancelled` when it has vanished).
fn control(ctx: &AppCtx, id: &str) -> TaskState {
    ctx.task_state(id).unwrap_or(TaskState::Cancelled)
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
        let prepared = prepare_track(ctx, id, fetch, page_url, track, dir).await;
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
        merge::merge_segments(
            &temp,
            &info,
            &final_owned,
            || control(&merge_ctx, &merge_id) == TaskState::Running,
            || merge_ctx.begin_terminal_commit(&merge_id),
        )
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
    Ok((Outcome::Done, produced))
}

/// Dropping a throttled transfer on pause/cancel leaves only completed segments
/// in the cache, and releases its bandwidth waiter immediately.
async fn while_running<T>(
    ctx: &AppCtx,
    id: &str,
    work: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    // Only state transitions wake this, not every task's progress update.
    let mut changes = ctx.subscribe_status();
    let stopped = async {
        loop {
            if control(ctx, id) != TaskState::Running || changes.changed().await.is_err() {
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
    ctx: &AppCtx,
    id: &str,
    fetch: &Fetcher,
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
        let data = while_running(ctx, id, async {
            let resp = fetch
                .media_response(&encryption.key_url, page_url, None)
                .await?;
            Ok(resp.bytes().await?)
        })
        .await?;
        validate_media_body(&encryption.key_url, &data)?;
        anyhow::ensure!(data.len() == 16, "unexpected AES key length {}", data.len());
        keys.insert(encryption.key_url.clone(), data.to_vec());
    }
    if let Some(init) = &info.init_segment {
        let path = dir.join("init.mp4");
        if !is_non_empty(&path).await {
            fetch_segment(
                ctx,
                id,
                fetch,
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
    let mut last_error = String::new();
    let pool = SegmentPool {
        ctx,
        fetch,
        id,
        page_url,
        info,
        keys: &keys,
        temp_dir,
        done: &done,
        bytes: &bytes,
        rejected: &rejected,
        concurrency: segment_concurrency.clamp(1, 32),
    };
    let mut failed = pool.run(0..info.segments.len(), &mut last_error).await;

    // Retry transient failures a few times before giving up. Rounds reuse the
    // bounded worker pool and dispatch in playlist order.
    let mut attempt = 0;
    while !failed.is_empty() && attempt < 3 && !rejected.load(Ordering::Relaxed) {
        attempt += 1;
        log::warn!(
            "[{id}] retry round={attempt}/3 failed_segments={}",
            failed.len()
        );
        tokio::time::sleep(Duration::from_millis(300 * attempt as u64)).await;
        let state = control(ctx, id);
        if state != TaskState::Running {
            break;
        }
        failed = pool.run(failed, &mut last_error).await;
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

/// Shared state of one track's segment workers.
struct SegmentPool<'a> {
    ctx: &'a Arc<AppCtx>,
    fetch: &'a Fetcher,
    id: &'a str,
    page_url: &'a str,
    info: &'a M3u8Info,
    keys: &'a Arc<HashMap<String, Vec<u8>>>,
    temp_dir: &'a Path,
    done: &'a Arc<AtomicU64>,
    bytes: &'a Arc<AtomicU64>,
    rejected: &'a Arc<AtomicBool>,
    concurrency: usize,
}

impl SegmentPool<'_> {
    /// Fetch `indices` with at most `concurrency` workers; returns the failed
    /// indices in playlist order.
    async fn run(
        &self,
        indices: impl IntoIterator<Item = usize>,
        last_error: &mut String,
    ) -> Vec<usize> {
        let id = self.id;
        let mut indices = indices.into_iter();
        let mut failed = Vec::new();
        let mut downloads = FuturesUnordered::new();
        // Spawn only the configured number of workers, rather than allocating a
        // waiting task for every segment. Dropping the stream aborts its workers.
        loop {
            while downloads.len() < self.concurrency {
                let Some(index) = indices.next() else {
                    break;
                };
                let fetch = self.fetch.clone();
                let ctx = Arc::clone(self.ctx);
                let id = id.to_string();
                let page_url = self.page_url.to_string();
                let dir = self.temp_dir.to_path_buf();
                let segment = self.info.segments[index].clone();
                let keys = Arc::clone(self.keys);
                let done = Arc::clone(self.done);
                let bytes = Arc::clone(self.bytes);
                let rejected = Arc::clone(self.rejected);

                let handle = AbortOnDropHandle::new(tokio::spawn(async move {
                    if rejected.load(Ordering::Relaxed) || control(&ctx, &id) != TaskState::Running
                    {
                        anyhow::bail!("paused, cancelled or media rejected");
                    }
                    let path = dir.join(format!("{index}.ts"));
                    if is_non_empty(&path).await {
                        log::trace!("[{id}] segment={index} cached path={}", path.display());
                        done.fetch_add(1, Ordering::Relaxed);
                        return Ok(());
                    }
                    log::trace!("[{id}] segment={index} fetching path={}", path.display());
                    let started = Instant::now();
                    fetch_segment(&ctx, &id, &fetch, &page_url, &segment, &path, &keys, &bytes)
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
                    if e.is::<MediaRejected>() || !self.rejected.load(Ordering::Relaxed) {
                        *last_error = format!("{e:#}");
                    }
                    failed.push(index);
                }
                Err(e) => {
                    log::error!("[{id}] segment {index} task failed: {e}");
                    failed.push(index);
                }
            }
        }
        failed.sort_unstable();
        failed
    }
}

#[allow(clippy::too_many_arguments)]
async fn fetch_segment(
    ctx: &AppCtx,
    id: &str,
    fetch: &Fetcher,
    page_url: &str,
    segment: &Segment,
    path: &Path,
    keys: &HashMap<String, Vec<u8>>,
    bytes: &AtomicU64,
) -> anyhow::Result<()> {
    let resp = while_running(ctx, id, async {
        fetch
            .download_response(&segment.url, page_url, segment.byte_range)
            .await
    })
    .await?;
    let encryption = segment
        .encryption
        .as_ref()
        .map(|encryption| {
            let key = keys
                .get(&encryption.key_url)
                .ok_or_else(|| anyhow::anyhow!("segment key is missing"))?;
            Ok::<_, anyhow::Error>((key.clone(), encryption.iv))
        })
        .transpose()?;
    let tmp = path.with_extension("part");
    // A bounded queue and buffered blocking writer keep memory independent of
    // segment length. The writer owns every chunk; no whole-body copy is made.
    let (send, mut receive) = tokio::sync::mpsc::channel::<Vec<u8>>(2);
    let writer_path = tmp.clone();
    let writer = tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let file = std::fs::File::create(writer_path)?;
        let output = std::io::BufWriter::with_capacity(256 * 1024, file);
        let mut writer = segment::SegmentWriter::new(
            output,
            encryption.as_ref().map(|(key, iv)| (key.as_slice(), iv)),
        )?;
        while let Some(chunk) = receive.blocking_recv() {
            writer.push(&chunk)?;
        }
        writer.finish()?;
        Ok(())
    });
    let transfer = while_running(ctx, id, async {
        let mut prefix = Vec::with_capacity(8192);
        let mut length = 0u64;
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = tokio::time::timeout(Duration::from_secs(60), stream.next())
            .await
            .context("media read timed out")?
        {
            let chunk = chunk?;
            for part in chunk.chunks(64 * 1024) {
                ctx.download_limiter
                    .acquire(
                        crate::runtime::download_limiter::DownloadModule::Jav,
                        part.len(),
                    )
                    .await;
                bytes.fetch_add(part.len() as u64, Ordering::Relaxed);
                length += part.len() as u64;
                if prefix.len() < 8192 {
                    prefix.extend_from_slice(&part[..part.len().min(8192 - prefix.len())]);
                    validate_media_body(&segment.url, &prefix)?;
                }
                if send.send(part.to_vec()).await.is_err() {
                    // The writer's error below is more useful than a closed queue.
                    return Ok(());
                }
            }
        }
        validate_media_body(&segment.url, &prefix)?;
        if let Some((_, expected)) = segment.byte_range {
            anyhow::ensure!(
                length == expected,
                "incomplete HLS byte range for {}",
                segment.url
            );
        }
        Ok(())
    })
    .await;
    drop(send);
    // Drain the writer on success, error AND pause/cancel before releasing the
    // download lease or touching its directory. Only a complete body is renamed.
    let written = writer.await.context("segment writer failed")?;
    transfer?;
    written?;
    tokio::fs::rename(&tmp, path).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::jav::source::stream::m3u8::parse_media_m3u8;
    use crate::jav::util::iv_for_segment;

    fn decrypt(data: Vec<u8>, key: &[u8], iv: &[u8; 16]) -> anyhow::Result<Vec<u8>> {
        let mut writer = segment::SegmentWriter::new(Vec::new(), Some((key, iv)))?;
        for chunk in data.chunks(17) {
            writer.push(chunk)?;
        }
        writer.finish()
    }

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

        let out = decrypt(ciphertext.clone(), &key, &iv_for_segment(0, &None)).unwrap();
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
            decrypt(ciphertext.clone(), &key, &iv_for_segment(5, &None)).unwrap(),
            plaintext
        );
        // Wrong index → wrong IV → garbage, never the plaintext.
        assert_ne!(
            decrypt(ciphertext.clone(), &key, &iv_for_segment(6, &None)).unwrap(),
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
        assert!(decrypt(ciphertext.clone(), &key, &iv_for_segment(0, &None)).is_err());
    }

    #[test]
    fn rejects_wrong_key_length() {
        assert!(decrypt(vec![0u8; 16], &[0u8; 8], &iv_for_segment(0, &None)).is_err());
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
        assert!(merge::merge_segments(&dir, &info, &final_path, || true, || true).is_err());
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

    mod transfers {
        use super::*;
        use crate::jav::app::TaskInfo;
        use crate::jav::source::stream::m3u8::parse_media_m3u8;
        use crate::test_support::http::{Server, response};

        async fn context(total: usize) -> Arc<AppCtx> {
            let common =
                tokio::sync::watch::channel(crate::configuration::app::Config::default()).1;
            let ctx = Arc::new(
                AppCtx::new(
                    crate::jav::config::Config {
                        browser_enabled: false,
                        ..Default::default()
                    },
                    crate::storage::Database::open(":memory:").await.unwrap(),
                    common.clone(),
                    Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                        common,
                    )),
                )
                .await
                .unwrap(),
            );
            let mut task = TaskInfo::new("test", "http://127.0.0.1/");
            task.state = TaskState::Running;
            task.total_segments = total;
            assert!(ctx.register_task(task).is_some());
            ctx
        }

        fn fetcher(ctx: &AppCtx) -> Fetcher {
            let mut fetch = ctx.fetcher();
            fetch.client = wreq::Client::builder().no_proxy().build().unwrap();
            fetch
        }

        fn playlist(base: &str, count: usize) -> M3u8Info {
            let mut text = "#EXTM3U\n".to_string();
            for i in 0..count {
                text.push_str(&format!("#EXTINF:1,\n{i}.ts\n"));
            }
            parse_media_m3u8(
                &text,
                &url::Url::parse(&format!("{base}/index.m3u8")).unwrap(),
            )
            .unwrap()
        }

        fn start(
            ctx: Arc<AppCtx>,
            info: M3u8Info,
            dir: PathBuf,
            concurrency: usize,
        ) -> tokio::task::JoinHandle<anyhow::Result<Outcome>> {
            tokio::spawn(async move {
                download_segments(
                    &ctx,
                    &fetcher(&ctx),
                    "test",
                    &info.variant.uri,
                    &info,
                    Arc::new(HashMap::new()),
                    &dir,
                    concurrency,
                )
                .await
            })
        }

        #[tokio::test]
        async fn prepare_track_reuses_legacy_manifest_and_invalidates_changed_identity() {
            let root = tempfile::tempdir().unwrap();
            let dir = root.path().join("track");
            tokio::fs::create_dir(&dir).await.unwrap();
            let ctx = context(1).await;
            let fetch = fetcher(&ctx);
            let mut info = playlist("http://127.0.0.1:1", 1);
            // Freeze the existing manifest format, not merely a round trip of current code.
            let manifest =
                br#"[[{"url":"http://127.0.0.1:1/0.ts","byte_range":null,"encryption":null}],null]"#;
            tokio::fs::write(dir.join("playlist.json"), manifest)
                .await
                .unwrap();
            tokio::fs::write(dir.join("0.ts"), b"cached").await.unwrap();
            prepare_track(&ctx, "test", &fetch, "", &info, &dir)
                .await
                .unwrap();
            assert_eq!(tokio::fs::read(dir.join("0.ts")).await.unwrap(), b"cached");
            assert_eq!(
                tokio::fs::read(dir.join("playlist.json")).await.unwrap(),
                manifest
            );
            for changed_range in [false, true] {
                if changed_range {
                    info.segments[0].byte_range = Some((4, 3));
                } else {
                    info.segments[0].url.push_str("?new=1");
                }
                prepare_track(&ctx, "test", &fetch, "", &info, &dir)
                    .await
                    .unwrap();
                assert!(!dir.join("0.ts").exists());
                assert_eq!(
                    tokio::fs::read(dir.join("playlist.json")).await.unwrap(),
                    serde_json::to_vec(&(&info.segments, &info.init_segment)).unwrap()
                );
                tokio::fs::write(dir.join("0.ts"), b"cached again")
                    .await
                    .unwrap();
            }
        }

        #[tokio::test]
        async fn download_segments_reuses_sparse_cache_and_merges_out_of_order_completions() {
            let mut server = Server::new().await;
            let dir = tempfile::tempdir().unwrap();
            let info = playlist(&server.url, 4);
            let ctx = context(4).await;
            prepare_track(&ctx, "test", &fetcher(&ctx), "", &info, dir.path())
                .await
                .unwrap();
            tokio::fs::write(dir.path().join("0.ts"), b"cached-zero")
                .await
                .unwrap();
            // An empty file and an abandoned staging file must not count as cached.
            tokio::fs::write(dir.path().join("2.ts"), b"")
                .await
                .unwrap();
            tokio::fs::write(dir.path().join("2.part"), b"incomplete")
                .await
                .unwrap();
            let run = start(ctx.clone(), info.clone(), dir.path().into(), 2);
            let first = server.next().await;
            let second = server.next().await;
            let (one, two) = if first.head.starts_with("GET /1.ts ") {
                (first, second)
            } else {
                (second, first)
            };
            assert!(one.head.starts_with("GET /1.ts "));
            assert!(two.head.starts_with("GET /2.ts "));
            let extra = server.next();
            tokio::pin!(extra);
            assert!(
                futures_util::poll!(&mut extra).is_pending(),
                "only two segment requests can be admitted"
            );
            drop(two.respond(response("200 OK", "", b"two")));
            let three = extra.await;
            assert!(three.head.starts_with("GET /3.ts "));
            drop(three.respond(response("200 OK", "", b"three")));
            drop(one.respond(response("200 OK", "", b"one")));
            assert!(matches!(run.await.unwrap().unwrap(), Outcome::Done));
            let raw = merge::assemble_track(dir.path(), &info, || true).unwrap();
            assert_eq!(
                std::fs::read(raw.path()).unwrap(),
                b"cached-zeroonetwothree"
            );
            assert!(!dir.path().join("2.part").exists());
            let task = ctx.task("test").unwrap();
            assert_eq!(task.done_segments, 4);
            assert_eq!(
                task.downloaded_bytes, 11,
                "cached bytes do not count as network bytes"
            );
        }

        #[tokio::test]
        async fn download_segments_retries_transient_failures_concurrently() {
            let mut server = Server::new().await;
            let dir = tempfile::tempdir().unwrap();
            let info = playlist(&server.url, 2);
            let ctx = context(2).await;
            let run = start(ctx.clone(), info.clone(), dir.path().into(), 2);
            let a = server.next().await;
            let b = server.next().await;
            drop(b.respond(response("503 Unavailable", "", b"retry")));
            drop(a.respond(response("503 Unavailable", "", b"retry")));
            // Both retries are in flight together, so either may arrive first.
            let first = server.next().await;
            let second = tokio::time::timeout(Duration::from_secs(5), server.next())
                .await
                .expect("retries run concurrently");
            let mut retried = Vec::new();
            for request in [first, second] {
                let i = u8::from(request.head.starts_with("GET /1.ts "));
                assert!(request.head.starts_with(&format!("GET /{i}.ts ")));
                retried.push(i);
                drop(request.respond(response("200 OK", "", &[b'A' + i])));
            }
            retried.sort_unstable();
            assert_eq!(retried, [0, 1]);
            assert!(matches!(run.await.unwrap().unwrap(), Outcome::Done));
            let raw = merge::assemble_track(dir.path(), &info, || true).unwrap();
            assert_eq!(std::fs::read(raw.path()).unwrap(), b"AB");
            assert_eq!(ctx.task("test").unwrap().done_segments, 2);
        }

        #[tokio::test]
        async fn download_segments_exhausts_three_retries_without_publishing_partial_segment() {
            let mut server = Server::new().await;
            let dir = tempfile::tempdir().unwrap();
            let run = start(
                context(1).await,
                playlist(&server.url, 1),
                dir.path().into(),
                1,
            );
            for _ in 0..4 {
                let request = server.next().await;
                assert!(request.head.starts_with("GET /0.ts "));
                drop(request.respond(response("503 Unavailable", "", b"retry")));
            }
            let error = run.await.unwrap().err().unwrap();
            assert!(error.to_string().contains("failed after 3 retries"));
            assert!(!dir.path().join("0.ts").exists());
            assert!(!dir.path().join("0.part").exists());
        }

        #[tokio::test]
        async fn download_segments_media_rejection_skips_retries_and_retains_cached_segments() {
            let mut server = Server::new().await;
            let dir = tempfile::tempdir().unwrap();
            tokio::fs::write(dir.path().join("0.ts"), b"cached")
                .await
                .unwrap();
            let run = start(
                context(2).await,
                playlist(&server.url, 2),
                dir.path().into(),
                1,
            );
            let request = server.next().await;
            assert!(request.head.starts_with("GET /1.ts "));
            drop(request.respond(response(
                "200 OK",
                "Content-Type: text/html\r\n",
                b"<html>denied</html>",
            )));
            // Any unwanted retry blocks on this server; completion demonstrates no retry.
            let error = tokio::time::timeout(Duration::from_secs(5), run)
                .await
                .unwrap()
                .unwrap()
                .err()
                .unwrap();
            assert!(error.to_string().contains("HTML instead of media"));
            assert_eq!(
                tokio::fs::read(dir.path().join("0.ts")).await.unwrap(),
                b"cached"
            );
            assert!(!dir.path().join("1.ts").exists());
        }

        #[tokio::test]
        async fn download_segments_pause_and_cancel_keep_completed_cache_during_pending_request() {
            for stop in [TaskState::Paused, TaskState::Cancelled] {
                let mut server = Server::new().await;
                let dir = tempfile::tempdir().unwrap();
                tokio::fs::write(dir.path().join("0.ts"), b"cached")
                    .await
                    .unwrap();
                let ctx = context(2).await;
                let run = start(ctx.clone(), playlist(&server.url, 2), dir.path().into(), 1);
                let held = server.next().await;
                assert!(held.head.starts_with("GET /1.ts "));
                ctx.update_task("test", |task| task.state = stop);
                let outcome = tokio::time::timeout(Duration::from_secs(5), run)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert!(match stop {
                    TaskState::Paused => matches!(outcome, Outcome::Paused),
                    _ => matches!(outcome, Outcome::Cancelled),
                });
                assert_eq!(
                    tokio::fs::read(dir.path().join("0.ts")).await.unwrap(),
                    b"cached"
                );
                assert!(!dir.path().join("1.ts").exists());
                drop(held);
                // Outer download_video performs cancel cleanup; this helper preserves cache.
            }
        }

        #[test]
        fn assemble_track_stop_removes_staging_but_keeps_segments_for_resume() {
            let dir = tempfile::tempdir().unwrap();
            let info = playlist("http://127.0.0.1:1", 3);
            for i in 0..3 {
                std::fs::write(dir.path().join(format!("{i}.ts")), [i as u8; 4]).unwrap();
            }
            let calls = std::cell::Cell::new(0);
            let error = merge::assemble_track(dir.path(), &info, || {
                calls.set(calls.get() + 1);
                calls.get() < 2
            })
            .unwrap_err();
            assert!(error.to_string().contains("merge interrupted"));
            assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 3);
            for i in 0..3 {
                assert_eq!(
                    std::fs::read(dir.path().join(format!("{i}.ts"))).unwrap(),
                    [i as u8; 4]
                );
            }
            let raw = merge::assemble_track(dir.path(), &info, || true).unwrap();
            assert_eq!(
                std::fs::read(raw.path()).unwrap(),
                [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2]
            );
        }

        async fn owned_transfer(
            ctx: Arc<AppCtx>,
            request: DownloadRequest,
            info: M3u8Info,
            cfg: crate::jav::config::Config,
            finish_gate: Option<(
                tokio::sync::oneshot::Sender<()>,
                tokio::sync::oneshot::Receiver<()>,
            )>,
        ) -> bool {
            let Some(_slot) = ctx.download_slot("test", &request).await else {
                return false;
            };
            ctx.update_task("test", |task| {
                task.state = TaskState::Running;
                task.total_segments = info.segments.len();
                task.done_segments = 0;
                task.downloaded_bytes = 0;
            });
            let dir = cfg.temp_path.join("temp_test");
            let fetch = fetcher(&ctx);
            let keys = prepare_track(&ctx, "test", &fetch, "", &info, &dir)
                .await
                .unwrap();
            let outcome = download_segments(&ctx, &fetch, "test", "", &info, keys, &dir, 2).await;
            if let Some((arrived, release)) = finish_gate {
                arrived.send(()).unwrap();
                release.await.unwrap();
            }
            let produced = cfg.save_path.join("output.ts");
            if matches!(&outcome, Ok(Outcome::Done)) {
                // Exercise ordered assembly without depending on installed ffmpeg/ffprobe.
                let staging = merge::assemble_track(&dir, &info, || true).unwrap();
                tokio::fs::create_dir_all(&cfg.save_path).await.unwrap();
                tokio::fs::copy(staging.path(), &produced).await.unwrap();
            }
            let card = VideoCard {
                id: "test".into(),
                url: info.variant.uri.clone(),
                title: "fixture".into(),
                image_url: String::new(),
                duration_secs: None,
                rank: None,
            };
            finish_run(&ctx, &cfg, &card, &card.title, &produced, outcome).await;
            true
        }

        fn isolated_config(root: &Path) -> crate::jav::config::Config {
            crate::jav::config::Config {
                save_path: root.join("output"),
                temp_path: root.join("partial"),
                browser_enabled: false,
                ..Default::default()
            }
        }

        #[tokio::test]
        async fn immediate_resume_waits_for_transfer_finalization_and_reuses_completed_segments() {
            let mut server = Server::new().await;
            let root = tempfile::tempdir().unwrap();
            let cfg = isolated_config(root.path());
            let dir = cfg.temp_path.join("temp_test");
            let ctx = context(2).await;
            ctx.update_task("test", |t| t.state = TaskState::Paused);
            let info = playlist(&server.url, 2);
            let first = ctx
                .register_task(TaskInfo::new("test", &server.url))
                .unwrap();
            // Completed segment from the old run's compatible playlist.
            prepare_track(&ctx, "test", &fetcher(&ctx), "", &info, &dir)
                .await
                .unwrap();
            tokio::fs::write(dir.join("0.ts"), b"cached-prefix")
                .await
                .unwrap();
            let (arrived, at_finish) = tokio::sync::oneshot::channel();
            let (release, finish) = tokio::sync::oneshot::channel();
            let old = tokio::spawn(owned_transfer(
                ctx.clone(),
                first,
                info.clone(),
                cfg.clone(),
                Some((arrived, finish)),
            ));
            let held = server.next().await;
            assert!(held.head.starts_with("GET /1.ts "));
            assert!(ctx.stop_task("test", false));
            let (_, restart) = ctx.request_resume("test").unwrap();
            assert!(
                ctx.request_resume("test").is_none(),
                "repeated resume is not another submission"
            );
            assert!(
                ctx.register_task(TaskInfo::new("test", &server.url))
                    .is_none()
            );
            assert_eq!(ctx.task_state("test"), Some(TaskState::Paused));
            let resumed = owned_transfer(ctx.clone(), restart, info.clone(), cfg.clone(), None);
            tokio::pin!(resumed);
            assert!(futures_util::poll!(&mut resumed).is_pending());
            tokio::time::timeout(Duration::from_secs(5), at_finish)
                .await
                .unwrap()
                .unwrap();
            assert!(
                futures_util::poll!(&mut resumed).is_pending(),
                "no writer during old finalization"
            );
            assert!(ctx.history().is_empty());
            release.send(()).unwrap();
            assert!(old.await.unwrap());
            assert_eq!(ctx.task_state("test"), Some(TaskState::Paused));
            assert_eq!(ctx.task("test").unwrap().phase, "paused");
            assert_eq!(
                tokio::fs::read(dir.join("0.ts")).await.unwrap(),
                b"cached-prefix"
            );
            drop(held); // Abandoned old response never becomes a completed segment.
            let request = tokio::select! {
                request = server.next() => request,
                _ = &mut resumed => panic!("restart exited before fetching missing segment"),
            };
            assert!(
                request.head.starts_with("GET /1.ts "),
                "cached segment must not be fetched"
            );
            assert_eq!(ctx.task_state("test"), Some(TaskState::Running));
            drop(request.respond(response("200 OK", "", b"new-suffix")));
            assert!(resumed.await);
            assert_eq!(ctx.task_state("test"), Some(TaskState::Completed));
            assert_eq!(ctx.task("test").unwrap().done_segments, 2);
            assert_eq!(
                tokio::fs::read(cfg.save_path.join("output.ts"))
                    .await
                    .unwrap(),
                b"cached-prefixnew-suffix"
            );
            let history = ctx.history();
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].status, "completed");
        }

        #[tokio::test]
        async fn cancel_while_resume_waits_survives_old_pause_finalization_and_cleans_cache() {
            let mut server = Server::new().await;
            let root = tempfile::tempdir().unwrap();
            let cfg = isolated_config(root.path());
            let ctx = context(1).await;
            ctx.update_task("test", |t| t.state = TaskState::Paused);
            let request = ctx
                .register_task(TaskInfo::new("test", &server.url))
                .unwrap();
            let info = playlist(&server.url, 1);
            let (arrived, at_finish) = tokio::sync::oneshot::channel();
            let (release, finish) = tokio::sync::oneshot::channel();
            let old = tokio::spawn(owned_transfer(
                ctx.clone(),
                request,
                info.clone(),
                cfg.clone(),
                Some((arrived, finish)),
            ));
            let held = server.next().await;
            assert!(ctx.stop_task("test", false));
            let (_, restart) = ctx.request_resume("test").unwrap();
            let resumed = owned_transfer(ctx.clone(), restart, info, cfg.clone(), None);
            tokio::pin!(resumed);
            assert!(futures_util::poll!(&mut resumed).is_pending());
            tokio::time::timeout(Duration::from_secs(5), at_finish)
                .await
                .unwrap()
                .unwrap();
            assert!(ctx.stop_task("test", true));
            assert!(!resumed.await, "cancel must withdraw the pending restart");
            release.send(()).unwrap();
            assert!(old.await.unwrap());
            drop(held);
            assert_eq!(ctx.task_state("test"), Some(TaskState::Cancelled));
            assert_eq!(ctx.task("test").unwrap().phase, "cancelled");
            assert!(!cfg.temp_path.join("temp_test").exists());
            assert!(ctx.history().is_empty());
        }

        #[tokio::test]
        async fn interrupted_error_finalization_preserves_cache_for_deferred_resume() {
            let root = tempfile::tempdir().unwrap();
            let cfg = isolated_config(root.path());
            let dir = cfg.temp_path.join("temp_test");
            tokio::fs::create_dir_all(&dir).await.unwrap();
            tokio::fs::write(dir.join("0.ts"), b"reusable")
                .await
                .unwrap();
            let ctx = context(1).await;
            assert!(ctx.stop_task("test", false));
            let (_, restart) = ctx.request_resume("test").unwrap();
            let card = VideoCard {
                id: "test".into(),
                url: String::new(),
                title: String::new(),
                image_url: String::new(),
                duration_secs: None,
                rank: None,
            };
            finish_failed(&ctx, &cfg, &card, "download paused or cancelled").await;
            assert_eq!(ctx.task_state("test"), Some(TaskState::Paused));
            assert_eq!(
                tokio::fs::read(dir.join("0.ts")).await.unwrap(),
                b"reusable"
            );
            assert!(ctx.history().is_empty());
            assert!(ctx.download_slot("test", &restart).await.is_some());
        }

        async fn lifecycle_fixture(
            root: &Path,
        ) -> (Arc<AppCtx>, crate::storage::Database, VideoCard) {
            let common = tokio::sync::watch::channel(crate::configuration::app::Config {
                jav_download_path: root.join("output"),
                temp_path: root.join("partial"),
                ..Default::default()
            })
            .1;
            let database = crate::storage::Database::open(":memory:").await.unwrap();
            let ctx = Arc::new(
                AppCtx::new(
                    crate::jav::config::Config {
                        browser_enabled: false,
                        concurrent_videos: 1,
                        ..Default::default()
                    },
                    database.clone(),
                    common.clone(),
                    Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                        common,
                    )),
                )
                .await
                .unwrap(),
            );
            let card = VideoCard {
                id: "test".into(),
                url: "http://127.0.0.1/test".into(),
                title: "fixture".into(),
                image_url: String::new(),
                duration_secs: None,
                rank: None,
            };
            (ctx, database, card)
        }

        #[tokio::test]
        async fn terminal_commit_rejects_late_stops_inside_real_persistence_and_shutdown_drains_it()
        {
            for completed in [false, true] {
                for database_error in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let (ctx, database, card) = lifecycle_fixture(root.path()).await;
                    let request = ctx
                        .register_task(TaskInfo::new(&card.id, &card.url))
                        .unwrap();
                    let slot = ctx.download_slot(&card.id, &request).await.unwrap();
                    ctx.update_task(&card.id, |task| task.state = TaskState::Running);
                    let cfg = ctx.config();
                    let cache = cfg.temp_path.join("temp_test");
                    tokio::fs::create_dir_all(&cache).await.unwrap();
                    tokio::fs::write(cache.join("0.ts"), b"cached")
                        .await
                        .unwrap();
                    // Hold the actual connection lock. The repository's write lock
                    // proves the finalizer has entered persistence, past cleanup.
                    let gate = database.connection().await;
                    if database_error {
                        gate.execute("DROP TABLE jav_records", ()).await.unwrap();
                    }
                    let owner_ctx = ctx.clone();
                    let run = ctx
                        .jobs
                        .spawn(async move {
                            let _slot = slot;
                            if completed {
                                finish_completed(
                                    &owner_ctx,
                                    &cfg,
                                    &card,
                                    &card.title,
                                    &cfg.save_path.join("fixture.mp4"),
                                    7,
                                )
                                .await;
                            } else {
                                finish_failed(&owner_ctx, &cfg, &card, "fixture failure").await;
                            }
                        })
                        .unwrap();
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while !ctx.history_write_in_progress() {
                            tokio::task::yield_now().await;
                        }
                    })
                    .await
                    .unwrap();
                    assert!(!cache.exists());
                    assert!(!ctx.stop_task("test", false));
                    assert!(!ctx.stop_task("test", true));
                    assert!(ctx.request_resume("test").is_none());
                    assert_eq!(ctx.task_state("test"), Some(TaskState::Running));
                    let shutdown = ctx.shutdown();
                    tokio::pin!(shutdown);
                    assert!(futures_util::poll!(&mut shutdown).is_pending());
                    assert!(!run.is_finished());
                    assert!(ctx.register_task(TaskInfo::new("new", "u")).is_none());
                    drop(gate);
                    tokio::time::timeout(Duration::from_secs(5), &mut shutdown)
                        .await
                        .unwrap();
                    run.await.unwrap();
                    assert_eq!(
                        ctx.task_state("test"),
                        Some(if completed {
                            TaskState::Completed
                        } else {
                            TaskState::Failed
                        })
                    );
                    assert_eq!(ctx.history().len(), usize::from(!database_error));
                }
            }
        }

        #[tokio::test]
        async fn stops_before_terminal_commit_win_over_success_and_failure() {
            for completed in [false, true] {
                for cancel in [false, true] {
                    let root = tempfile::tempdir().unwrap();
                    let (ctx, _, card) = lifecycle_fixture(root.path()).await;
                    let request = ctx
                        .register_task(TaskInfo::new(&card.id, &card.url))
                        .unwrap();
                    let _slot = ctx.download_slot(&card.id, &request).await.unwrap();
                    ctx.update_task(&card.id, |task| task.state = TaskState::Running);
                    let cfg = ctx.config();
                    let cache = cfg.temp_path.join("temp_test");
                    tokio::fs::create_dir_all(&cache).await.unwrap();
                    tokio::fs::write(cache.join("0.ts"), b"cached")
                        .await
                        .unwrap();
                    assert!(ctx.stop_task("test", cancel));
                    if completed {
                        finish_completed(
                            &ctx,
                            &cfg,
                            &card,
                            &card.title,
                            &cfg.save_path.join("fixture.mp4"),
                            7,
                        )
                        .await;
                    } else {
                        finish_failed(&ctx, &cfg, &card, "fixture failure").await;
                    }
                    assert_eq!(
                        ctx.task_state("test"),
                        Some(if cancel {
                            TaskState::Cancelled
                        } else {
                            TaskState::Paused
                        })
                    );
                    assert!(ctx.history().is_empty());
                    assert_eq!(cache.exists(), !cancel);
                    if !cancel {
                        assert_eq!(
                            tokio::fs::read(cache.join("0.ts")).await.unwrap(),
                            b"cached"
                        );
                    }
                }
            }
        }

        #[tokio::test]
        async fn shutdown_interrupts_held_segment_drains_owned_finalizer_and_retires_queued_runs() {
            let root = tempfile::tempdir().unwrap();
            let (ctx, _, card) = lifecycle_fixture(root.path()).await;
            let mut server = Server::new().await;
            let request = ctx
                .register_task(TaskInfo::new(&card.id, &card.url))
                .unwrap();
            let queued = ctx
                .register_task(TaskInfo::new("queued", "http://127.0.0.1/queued"))
                .unwrap();
            let cfg = ctx.config();
            let info = playlist(&server.url, 2);
            let cache = cfg.temp_path.join("temp_test");
            tokio::fs::create_dir_all(&cache).await.unwrap();
            tokio::fs::write(cache.join("0.ts"), b"cached")
                .await
                .unwrap();
            prepare_track(&ctx, "test", &fetcher(&ctx), "", &info, &cache)
                .await
                .unwrap();
            tokio::fs::write(cache.join("0.ts"), b"cached")
                .await
                .unwrap();
            let (arrived, finalizing) = tokio::sync::oneshot::channel();
            let (release, gate) = tokio::sync::oneshot::channel();
            let owner_ctx = ctx.clone();
            let run = ctx
                .jobs
                .spawn(owned_transfer(
                    owner_ctx,
                    request,
                    info,
                    cfg,
                    Some((arrived, gate)),
                ))
                .unwrap();
            let held = server.next().await;
            let queued_ctx = ctx.clone();
            let waiter = ctx
                .jobs
                .spawn(async move { queued_ctx.download_slot("queued", &queued).await.is_none() })
                .unwrap();
            let shutdown = ctx.shutdown();
            tokio::pin!(shutdown);
            assert!(futures_util::poll!(&mut shutdown).is_pending());
            tokio::time::timeout(Duration::from_secs(5), finalizing)
                .await
                .unwrap()
                .unwrap();
            assert!(waiter.await.unwrap());
            assert!(!run.is_finished());
            assert!(ctx.jobs.spawn(async { panic!("late job ran") }).is_none());
            assert!(!crate::jav::scheduler::resume_task(ctx.clone(), "test"));
            assert!(futures_util::poll!(&mut shutdown).is_pending());
            release.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(5), &mut shutdown)
                .await
                .unwrap();
            run.await.unwrap();
            assert_eq!(ctx.task_state("test"), Some(TaskState::Paused));
            assert_eq!(ctx.task_state("queued"), Some(TaskState::Paused));
            assert_eq!(
                tokio::fs::read(cache.join("0.ts")).await.unwrap(),
                b"cached"
            );
            assert!(!cache.join("1.ts").exists());
            assert!(ctx.history().is_empty());
            drop(held);
        }
    }
}
