//! The 91Porn download engine: resolve a video page, then stream its
//! progressive MP4 into a `.part` file next to the final output.
//!
//! Because the site serves ordinary HTTP files with `Accept-Ranges`, pause and
//! resume are just "keep the `.part` and re-request from its current length".
//! The signed URL expires, so every attempt re-resolves the page first.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tokio::io::AsyncWriteExt;

use crate::p91::app::{AppCtx, TaskState, now_rfc3339};
use crate::p91::source::http::Fetcher;
use crate::p91::source::resolver;
use crate::p91::source::scraper::VideoCard;
use crate::p91::storage::Record;
use crate::p91::util::{SpeedTracker, sanitize_filename};
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
        t.message = "resolving video".into();
    });
    if !started {
        return;
    }

    log::info!("[{id}] download started");

    // A listing or manual request may already supply the final title. Adopt
    // that output before contacting the source, even if the source is offline.
    let known_path = output_path(&cfg.save_path, &card.id, &card.title);
    if let Some((path, size)) = existing_output(&known_path).await {
        finish_completed(&ctx, &card, &card.title, &path, size).await;
        return;
    }

    log::debug!("[{id}] resolving prefer_hd={}", cfg.prefer_hd);
    let resolution = tokio::select! {
        biased;
        state = stopped(&ctx, &id) => {
            ctx.update_task(&id, |t| {
                t.state = state;
                t.phase = if state == TaskState::Paused { "paused" } else { "cancelled" }.into();
                t.message = t.phase.clone();
                t.speed_kbps = 0.0;
            });
            return;
        }
        result = resolver::resolve(ctx.session(), &fetch, &cfg, &card) => result,
    };
    let resolved = match resolution {
        Ok(resolved) => resolved,
        Err(error) => {
            finish_failed(&ctx, &card, &format!("{error:#}")).await;
            return;
        }
    };

    let title = resolved.title.clone();
    let final_path = output_path(&cfg.save_path, &card.id, &title);
    if let Err(error) = tokio::fs::create_dir_all(&cfg.save_path).await {
        finish_failed(
            &ctx,
            &card,
            &format!("cannot create save directory: {error}"),
        )
        .await;
        return;
    }

    log::info!(
        "[{id}] resolved hd={} source={}",
        resolved.hd,
        resolved.source_url
    );
    ctx.update_task(&id, |t| {
        t.title = title.clone();
        t.path = final_path.to_string_lossy().to_string();
        t.source_url = format!(
            "{} ({})",
            cfg.site_base,
            if resolved.hd { "HD" } else { "standard" }
        );
        t.phase = "downloading".into();
        t.message = "downloading".into();
    });

    if let Some((path, size)) = existing_output(&final_path).await {
        log::info!("[{id}] output already exists at {}", path.display());
        finish_completed(&ctx, &card, &title, &path, size).await;
        return;
    }

    let origin = resolver::media_referer(&cfg, &card);
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
            finish_completed(&ctx, &card, &title, &final_path, size).await;
        }
        Ok(Outcome::Paused) => {
            log::info!("[{id}] paused");
            ctx.update_task(&id, |t| {
                t.state = TaskState::Paused;
                t.phase = "paused".into();
                t.speed_kbps = 0.0;
                t.message = "paused — resume to continue from the partial file".into();
            });
        }
        Ok(Outcome::Cancelled) => {
            log::info!("[{id}] cancelled");
            let _ = tokio::fs::remove_file(part_path(&final_path)).await;
            ctx.update_task(&id, |t| {
                t.state = TaskState::Cancelled;
                t.phase = "cancelled".into();
                t.speed_kbps = 0.0;
                t.message = "cancelled".into();
            });
        }
        Err(error) => {
            // Keep the partial file: a failed transfer is resumable.
            finish_failed(&ctx, &card, &format!("{error:#}")).await;
        }
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

async fn existing_output(path: &Path) -> Option<(PathBuf, u64)> {
    let metadata = tokio::fs::metadata(path).await.ok()?;
    (metadata.is_file() && metadata.len() > 0).then(|| (path.to_path_buf(), metadata.len()))
}

/// Current control state of a task (`Cancelled` when it has vanished).
fn control(ctx: &AppCtx, id: &str) -> TaskState {
    ctx.task(id)
        .map(|t| {
            if t.phase == "pausing" && t.state == TaskState::Running {
                TaskState::Paused
            } else {
                t.state
            }
        })
        .unwrap_or(TaskState::Cancelled)
}

async fn stopped(ctx: &AppCtx, id: &str) -> TaskState {
    let mut changes = ctx.subscribe();
    loop {
        let state = control(ctx, id);
        if state != TaskState::Running {
            return state;
        }
        let _ = changes.recv().await;
    }
}

fn stopped_outcome(state: TaskState) -> Outcome {
    if state == TaskState::Cancelled {
        Outcome::Cancelled
    } else {
        Outcome::Paused
    }
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
    let mut offset = tokio::fs::metadata(&part)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

    let response = tokio::select! {
        biased;
        state = stopped(ctx, id) => return Ok(stopped_outcome(state)),
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
        tokio::fs::rename(&part, final_path).await?;
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
    let total = content_total(&response, offset);
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
        BackgroundTask::spawn("p91 progress reporter", async move {
            let mut tracker = SpeedTracker::new(Duration::from_secs(3));
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            loop {
                ticker.tick().await;
                if control(&ctx, &id) != TaskState::Running {
                    break;
                }
                let bytes = downloaded.load(Ordering::Relaxed);
                let speed = tracker.sample(Instant::now(), bytes);
                let total_bytes = total_shared.load(Ordering::Relaxed);
                ctx.update_task(&id, |t| {
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

    let result = transfer(ctx, id, response, &part, resuming, &downloaded).await;
    reporter.abort().await;

    let outcome = match result? {
        false => {
            let state = control(ctx, id);
            if state == TaskState::Cancelled {
                Outcome::Cancelled
            } else {
                Outcome::Paused
            }
        }
        true => {
            let written = downloaded.load(Ordering::Relaxed);
            if let Some(total) = total {
                anyhow::ensure!(
                    written == total,
                    "incomplete download: {written}/{total} bytes"
                );
            }
            tokio::fs::rename(&part, final_path).await?;
            Outcome::Done
        }
    };
    // Publish the final counters before the caller flips the state.
    let written = downloaded.load(Ordering::Relaxed);
    ctx.update_task(id, |t| {
        t.downloaded_bytes = written;
        if let Some(total) = total {
            t.total_bytes = total;
        }
    });
    Ok(outcome)
}

/// Write the response body into `part`. Returns `true` when the body finished,
/// `false` when the task was paused or cancelled mid-stream.
async fn transfer(
    ctx: &Arc<AppCtx>,
    id: &str,
    response: wreq::Response,
    part: &Path,
    append: bool,
    downloaded: &AtomicU64,
) -> anyhow::Result<bool> {
    let mut options = tokio::fs::OpenOptions::new();
    options.create(true).write(true);
    if append {
        options.append(true);
    } else {
        options.truncate(true);
    }
    let mut file = options.open(part).await?;

    let mut stream = response.bytes_stream();
    let mut changes = ctx.subscribe();
    loop {
        if control(ctx, id) != TaskState::Running {
            flush(&mut file).await?;
            return Ok(false);
        }
        // A state change cancels the pending read. Nothing is lost: whatever
        // was not written yet is simply re-requested from the new file length.
        let next = tokio::select! {
            biased;
            _ = changes.recv() => continue,
            result = tokio::time::timeout(Duration::from_secs(60), stream.next()) => result,
        };
        let chunk = match next {
            Err(_) => anyhow::bail!("media read timed out"),
            Ok(None) => break,
            Ok(Some(Err(error))) => return Err(error.into()),
            Ok(Some(Ok(chunk))) => chunk,
        };
        for piece in chunk.chunks(64 * 1024) {
            tokio::select! {
                biased;
                _ = stopped(ctx, id) => {
                    flush(&mut file).await?;
                    return Ok(false);
                }
                _ = ctx.download_limiter.acquire(
                    crate::runtime::download_limiter::DownloadModule::P91,
                    piece.len(),
                ) => {},
            }
            file.write_all(piece).await?;
            downloaded.fetch_add(piece.len() as u64, Ordering::Relaxed);
        }
    }
    flush(&mut file).await?;
    Ok(control(ctx, id) == TaskState::Running)
}

async fn flush(file: &mut tokio::fs::File) -> std::io::Result<()> {
    file.flush().await?;
    file.sync_data().await
}

/// Total body length, from `Content-Range` when resuming, else `Content-Length`.
fn content_total(response: &wreq::Response, offset: u64) -> Option<u64> {
    let headers = response.headers();
    if let Some(value) = headers.get("content-range").and_then(|v| v.to_str().ok())
        && let Some(total) = total_from_content_range(value)
    {
        return Some(total);
    }
    headers
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|value| total_from_content_length(value, offset))
}

/// `bytes 100-199/500` → `500`.
fn total_from_content_range(value: &str) -> Option<u64> {
    value.rsplit('/').next()?.trim().parse().ok()
}

fn total_from_content_length(value: &str, offset: u64) -> Option<u64> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .map(|length| length + offset)
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
    if let Err(error) = ctx.upsert_record(record).await {
        log::error!("cannot persist state: {error:#}");
    }
    ctx.update_task(&card.id, |t| {
        t.state = TaskState::Completed;
        t.title = title.to_string();
        t.downloaded_bytes = size;
        t.total_bytes = size;
        t.phase = "completed".into();
        t.total_segments = 1;
        t.done_segments = 1;
        t.speed_kbps = 0.0;
        t.message = "completed".into();
        t.path = path.to_string_lossy().to_string();
    });
}

async fn finish_failed(ctx: &Arc<AppCtx>, card: &VideoCard, message: &str) {
    log::error!("[{}] download failed: {message}", card.id);
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
    if let Err(error) = ctx.upsert_record(record).await {
        log::error!("cannot persist state: {error:#}");
    }
    let message = message.to_string();
    ctx.update_task(&card.id, |t| {
        t.state = TaskState::Failed;
        t.phase = "failed".into();
        t.speed_kbps = 0.0;
        t.message = message;
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_and_part_paths_are_stable() {
        let dir = PathBuf::from("/tmp/downloads");
        let final_path = output_path(&dir, "1950234389", "a/b:c");
        assert_eq!(
            final_path,
            PathBuf::from("/tmp/downloads/1950234389 - a_b_c.mp4")
        );
        assert_eq!(
            part_path(&final_path),
            PathBuf::from("/tmp/downloads/1950234389 - a_b_c.mp4.part")
        );
    }

    #[test]
    fn content_total_reads_content_range_first() {
        assert_eq!(total_from_content_range("bytes 100-199/500"), Some(500));
        assert_eq!(total_from_content_range("not a range"), None);
    }

    #[test]
    fn content_total_falls_back_to_content_length() {
        assert_eq!(total_from_content_length("250", 0), Some(250));
        assert_eq!(total_from_content_length("150", 100), Some(250));
        assert_eq!(total_from_content_length("", 0), None);
    }

    #[tokio::test]
    async fn existing_output_ignores_empty_files() {
        let dir = std::env::temp_dir().join(format!("p91d-out-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.mp4");
        std::fs::write(&path, b"").unwrap();
        assert!(existing_output(&path).await.is_none());
        std::fs::write(&path, b"data").unwrap();
        assert_eq!(existing_output(&path).await, Some((path.clone(), 4)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
