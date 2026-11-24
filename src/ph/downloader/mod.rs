//! The Pornhub download engine: resolve a video page, then stream its
//! progressive MP4 into a `.part` file next to the final output.
//!
//! Because the site serves ordinary HTTP files with `Accept-Ranges`, pause and
//! resume are just "keep the `.part` and re-request from its current length".
//! The signed URL expires, so every attempt re-resolves the page first.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::ph::app::{AppCtx, TaskState, now_rfc3339};
use crate::ph::source::http::Fetcher;
use crate::ph::source::resolver;
use crate::ph::source::scraper::VideoCard;
use crate::ph::storage::Record;
use crate::ph::util::{SpeedTracker, sanitize_filename};
use crate::runtime::BackgroundTask;
use crate::runtime::download_limiter::DownloadModule;
use crate::runtime::video_transfer::{self, Outcome};

/// Download one video end to end, updating the task registry and the dedup
/// ledger as it goes. Errors are reported through the task, not returned.
pub async fn download_video(ctx: Arc<AppCtx>, card: VideoCard) {
    let (cfg, fetch) = ctx.request_context();
    download_video_with(ctx, card, cfg, fetch).await;
}

/// Daily and manual submissions retain their original request binding while queued.
pub(crate) async fn download_video_with(
    ctx: Arc<AppCtx>,
    card: VideoCard,
    cfg: crate::ph::config::Config,
    fetch: Fetcher,
) -> TaskState {
    let Some(_owner) = ctx.transfers.jobs.enter() else {
        return TaskState::Paused;
    };
    let id = card.id.clone();
    let Some(_slot) = ctx.download_slot(&id).await else {
        return ctx.transfers.control(&id);
    };
    let mut started = false;
    ctx.transfers.update_task(&id, |t| {
        if t.state != TaskState::Queued {
            return;
        }
        started = true;
        t.state = TaskState::Running;
        t.phase = "resolving".into();
        t.title = card.title.clone();
        t.message = "resolving video".into();
    });
    if !started {
        return ctx.transfers.control(&id);
    }

    log::info!("[{id}] download started");

    // A listing or manual request may already supply the final title. Adopt
    // that output before contacting the source, even if the source is offline.
    let known_path = output_path(&cfg.save_path, &card.id, &card.title);
    if let Some((path, size)) = existing_output(&known_path).await {
        return finish_completed(&ctx, &card, &card.title, &path, size).await;
    }

    let minimum_resolution = ctx.minimum_video_resolution();
    log::debug!("[{id}] resolving quality cap={:?}", cfg.target_height());
    let resolution = tokio::select! {
        biased;
        _ = ctx.transfers.stopped(&id) => {
            return finish_stopped(&ctx, &card).await
        }
        result = resolver::resolve(ctx.session(), &fetch, &cfg, &card) => result,
    };
    let resolved = match resolution {
        Ok(resolved) => resolved,
        Err(error) => return finish_failed(&ctx, &card, &format!("{error:#}")).await,
    };

    let origin = resolver::media_referer(&cfg, &card);
    if minimum_resolution > 0 {
        let dimensions = resolved.width.zip(resolved.height);
        if let Some(reason) =
            crate::runtime::video_resolution::rejection(minimum_resolution, dimensions)
        {
            if !ctx.transfers.begin_terminal_commit(&id) {
                return finish_stopped(&ctx, &card).await;
            }
            log::info!("[{id}] {reason}");
            ctx.transfers.update_task(&id, |task| {
                task.state = TaskState::Skipped;
                task.phase = "filtered".into();
                task.message = reason;
                task.speed_kbps = 0.0;
            });
            ctx.transfers.end_terminal_commit(&id);
            return TaskState::Skipped;
        }
    }

    let title = resolved.title.clone();
    let final_path = output_path(&cfg.save_path, &card.id, &title);
    if let Err(error) = tokio::fs::create_dir_all(&cfg.save_path).await {
        return finish_failed(
            &ctx,
            &card,
            &format!("cannot create save directory: {error}"),
        )
        .await;
    }

    log::debug!("[{id}] resolved quality={}", resolved.quality_label());
    ctx.transfers.update_task(&id, |t| {
        t.title = title.clone();
        t.path = final_path.to_string_lossy().to_string();
        t.source_url = format!("Pornhub ({})", resolved.quality_label());
        t.phase = "downloading".into();
        t.message = "downloading".into();
    });

    if let Some((path, size)) = existing_output(&final_path).await {
        log::debug!("[{id}] output already exists at {}", path.display());
        return finish_completed(&ctx, &card, &title, &path, size).await;
    }

    let run = download_file(
        &ctx,
        &fetch,
        &id,
        &resolved.source_url,
        &origin,
        &final_path,
    )
    .await;

    match run {
        Ok(Outcome::Done) => {
            let size = tokio::fs::metadata(&final_path)
                .await
                .map(|m| m.len())
                .unwrap_or(0);
            finish_completed(&ctx, &card, &title, &final_path, size).await
        }
        Ok(Outcome::Paused | Outcome::Cancelled) => finish_stopped(&ctx, &card).await,
        Err(error) => {
            // Keep the partial file: a failed transfer is resumable.
            finish_failed(&ctx, &card, &format!("{error:#}")).await
        }
    }
}

/// Reconcile the latest accepted stop, including errors returned during flush.
async fn finish_stopped(ctx: &AppCtx, card: &VideoCard) -> TaskState {
    ctx.transfers.update_task(&card.id, |task| {
        if task.state != TaskState::Cancelled {
            task.state = TaskState::Paused;
            task.phase = "paused".into();
            task.message = "paused — resume to continue from the partial file".into();
            task.speed_kbps = 0.0;
        }
    });
    if ctx.transfers.control(&card.id) == TaskState::Cancelled {
        let task = ctx.transfers.task(&card.id);
        let path = task
            .filter(|task| !task.path.is_empty())
            .map(|task| PathBuf::from(task.path))
            .unwrap_or_else(|| output_path(&ctx.config().save_path, &card.id, &card.title));
        let _ = tokio::fs::remove_file(part_path(&path)).await;
        let _ = tokio::fs::remove_file(source_path(&path)).await;
        ctx.transfers.update_task(&card.id, |task| {
            task.state = TaskState::Cancelled;
            task.phase = "cancelled".into();
            task.message = "cancelled".into();
            task.speed_kbps = 0.0;
        });
        TaskState::Cancelled
    } else {
        TaskState::Paused
    }
}

fn output_path(save_path: &Path, id: &str, title: &str) -> PathBuf {
    save_path.join(format!("{id} - {}.mp4", sanitize_filename(title)))
}

fn part_path(final_path: &Path) -> PathBuf {
    let mut name = final_path.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

fn source_path(final_path: &Path) -> PathBuf {
    let mut name = part_path(final_path).into_os_string();
    name.push(".url");
    PathBuf::from(name)
}

/// A fresh resolution may select another quality or file. Only reuse bytes
/// from the exact same source; expired signed URLs safely restart the transfer.
async fn prepare_partial(final_path: &Path, source_url: &str) -> anyhow::Result<u64> {
    let part = part_path(final_path);
    let source = source_path(final_path);
    let offset = match tokio::fs::metadata(&part).await {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    if offset > 0
        && tokio::fs::read_to_string(&source)
            .await
            .is_ok_and(|url| url == source_url)
    {
        return Ok(offset);
    }
    // Truncate before publishing the new identity, including after a crash.
    let file = tokio::fs::File::create(&part).await?;
    file.sync_all().await?;
    tokio::fs::write(source, source_url).await?;
    Ok(0)
}

async fn existing_output(path: &Path) -> Option<(PathBuf, u64)> {
    let metadata = tokio::fs::metadata(path).await.ok()?;
    (metadata.is_file() && metadata.len() > 0).then(|| (path.to_path_buf(), metadata.len()))
}

/// Stream the media URL into a resumable partial file.
async fn download_file(
    ctx: &Arc<AppCtx>,
    fetch: &Fetcher,
    id: &str,
    source_url: &str,
    origin: &str,
    final_path: &Path,
) -> anyhow::Result<Outcome> {
    let part = part_path(final_path);
    let mut offset = prepare_partial(final_path, source_url).await?;

    let response = tokio::select! {
        biased;
        state = ctx.transfers.stopped(id) => return Ok(video_transfer::stopped_outcome(state)),
        response = fetch.media_response(source_url, origin, (offset > 0).then_some((offset, None))) => response?,
    };
    let status = response.status();
    if status == wreq::StatusCode::RANGE_NOT_SATISFIABLE {
        let complete = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("bytes */"))
            .and_then(|v| v.parse::<u64>().ok());
        anyhow::ensure!(
            offset > 0 && complete == Some(offset),
            "server rejected the resume offset {offset}"
        );
        if !ctx.transfers.begin_terminal_commit(id) {
            return Ok(video_transfer::stopped_outcome(ctx.transfers.control(id)));
        }
        tokio::fs::rename(&part, final_path).await?;
        let _ = tokio::fs::remove_file(source_path(final_path)).await;
        return Ok(Outcome::Done);
    }
    if status == wreq::StatusCode::PARTIAL_CONTENT {
        let start = response
            .headers()
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("bytes "))
            .and_then(|v| v.split_once('-'))
            .and_then(|(start, _)| start.parse::<u64>().ok());
        anyhow::ensure!(
            start == Some(offset),
            "server returned an unexpected byte range for offset {offset}"
        );
    }
    // A server that ignores `Range` answers 200 and the whole file; restart.
    let resuming = offset > 0 && status == wreq::StatusCode::PARTIAL_CONTENT;
    if !resuming {
        offset = 0;
    }
    let total = video_transfer::content_total(&response, offset);
    log::debug!(
        "[{id}] transfer starting status={} offset={offset} total={} resuming={resuming}",
        status.as_u16(),
        total.unwrap_or(0)
    );

    let downloaded = Arc::new(AtomicU64::new(offset));
    let total_shared = Arc::new(AtomicU64::new(total.unwrap_or(0)));

    let reporter = {
        let ctx = Arc::clone(ctx);
        let id = id.to_string();
        let downloaded = Arc::clone(&downloaded);
        let total_shared = Arc::clone(&total_shared);
        BackgroundTask::spawn("ph progress reporter", async move {
            let mut tracker = SpeedTracker::new(Duration::from_secs(3));
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                if ctx.transfers.control(&id) != TaskState::Running {
                    break;
                }
                let bytes = downloaded.load(Ordering::Relaxed);
                let speed = tracker.sample(Instant::now(), bytes);
                let total_bytes = total_shared.load(Ordering::Relaxed);
                ctx.transfers.update_task(&id, |t| {
                    t.downloaded_bytes = bytes;
                    t.total_bytes = total_bytes;
                    t.speed_kbps = speed;
                    t.message = match total_bytes {
                        0 => "downloading".into(),
                        total => format!("{}%", bytes.saturating_mul(100) / total.max(1)),
                    };
                });
            }
        })
    };

    let result = video_transfer::write_body(
        &ctx.transfers,
        id,
        response,
        &part,
        resuming,
        &downloaded,
        |bytes| ctx.download_limiter.acquire(DownloadModule::Ph, bytes),
    )
    .await;
    reporter.abort().await;

    let outcome = match result? {
        false => video_transfer::stopped_outcome(ctx.transfers.control(id)),
        true => {
            let written = downloaded.load(Ordering::Relaxed);
            video_transfer::validate_size(written, total)?;
            if !ctx.transfers.begin_terminal_commit(id) {
                return Ok(video_transfer::stopped_outcome(ctx.transfers.control(id)));
            }
            tokio::fs::rename(&part, final_path).await?;
            let _ = tokio::fs::remove_file(source_path(final_path)).await;
            Outcome::Done
        }
    };
    // Publish the final counters before the caller flips the state.
    let written = downloaded.load(Ordering::Relaxed);
    ctx.transfers.update_task(id, |t| {
        t.downloaded_bytes = written;
        if let Some(total) = total {
            t.total_bytes = total;
        }
    });
    Ok(outcome)
}

async fn finish_completed(
    ctx: &Arc<AppCtx>,
    card: &VideoCard,
    title: &str,
    path: &Path,
    size: u64,
) -> TaskState {
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
    video_transfer::finish_completed(
        &ctx.transfers,
        record,
        |record| ctx.upsert_record(record),
        finish_stopped(ctx, card),
    )
    .await
}

async fn finish_failed(ctx: &Arc<AppCtx>, card: &VideoCard, message: &str) -> TaskState {
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
    video_transfer::finish_failed(
        &ctx.transfers,
        record,
        |record| ctx.upsert_record(record),
        finish_stopped(ctx, card),
    )
    .await
}
