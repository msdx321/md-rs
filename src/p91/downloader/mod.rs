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

use crate::p91::app::{AppCtx, TaskState, now_rfc3339};
use crate::p91::source::http::Fetcher;
use crate::p91::source::resolver;
use crate::p91::source::scraper::VideoCard;
use crate::p91::storage::Record;
use crate::p91::util::{SpeedTracker, sanitize_filename};
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
    cfg: crate::p91::config::Config,
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
    log::debug!("[{id}] resolving prefer_hd={}", cfg.prefer_hd);
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
        let probe = tokio::select! {
            biased;
            _ = ctx.transfers.stopped(&id) => {
                return finish_stopped(&ctx, &card).await;
            }
            result = tokio::time::timeout(
                Duration::from_secs(20),
                crate::p91::source::mp4::dimensions(
                    &fetch, &resolved.source_url, &origin, &ctx.download_limiter,
                ),
            ) => result,
        };
        let dimensions = match probe {
            Ok(Ok(dimensions)) => dimensions,
            result => {
                log::warn!("[{id}] video resolution unknown; allowing download: {result:?}");
                None
            }
        };
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

    log::debug!("[{id}] resolved hd={}", resolved.hd);
    ctx.transfers.update_task(&id, |t| {
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
    let mut offset = tokio::fs::metadata(&part)
        .await
        .map(|m| m.len())
        .unwrap_or(0);

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
        BackgroundTask::spawn("p91 progress reporter", async move {
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
        |bytes| ctx.download_limiter.acquire(DownloadModule::P91, bytes),
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

    mod transfers {
        use super::*;
        use crate::p91::app::TaskInfo;
        use crate::test_support::http::{Server, response};

        async fn context() -> Arc<AppCtx> {
            let common =
                tokio::sync::watch::channel(crate::configuration::app::Config::default()).1;
            let ctx = Arc::new(
                AppCtx::new(
                    crate::p91::config::Config::default(),
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
            assert!(ctx.transfers.register_task(task));
            ctx
        }

        fn start(
            ctx: Arc<AppCtx>,
            url: String,
            path: PathBuf,
        ) -> tokio::task::JoinHandle<anyhow::Result<Outcome>> {
            tokio::spawn(async move {
                let fetch = Fetcher::new(
                    wreq::Client::builder().no_proxy().build().unwrap(),
                    String::new(),
                    ctx.session(),
                );
                download_file(&ctx, &fetch, "test", &url, &url, &path).await
            })
        }

        #[tokio::test]
        async fn download_file_resumes_206_and_restarts_on_200() {
            for (status, headers, body) in [
                (
                    "206 Partial Content",
                    "Content-Range: bytes 3-5/6\r\n",
                    b"def".as_slice(),
                ),
                ("200 OK", "", b"abcdef".as_slice()),
            ] {
                let mut server = Server::new().await;
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("video.mp4");
                tokio::fs::write(part_path(&path), b"abc").await.unwrap();
                let ctx = context().await;
                let run = start(ctx.clone(), server.url.clone(), path.clone());
                let request = server.next().await;
                assert!(
                    request
                        .head
                        .to_ascii_lowercase()
                        .contains("range: bytes=3-\r\n")
                );
                drop(request.respond(response(status, headers, body)));
                assert!(matches!(run.await.unwrap().unwrap(), Outcome::Done));
                assert_eq!(tokio::fs::read(&path).await.unwrap(), b"abcdef");
                assert!(!part_path(&path).exists());
                let task = ctx.transfers.task("test").unwrap();
                assert_eq!((task.downloaded_bytes, task.total_bytes), (6, 6));
            }
        }

        #[tokio::test]
        async fn download_file_accepts_416_only_for_exact_existing_length() {
            for total in [3, 4] {
                let mut server = Server::new().await;
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("video.mp4");
                tokio::fs::write(part_path(&path), b"abc").await.unwrap();
                let run = start(context().await, server.url.clone(), path.clone());
                let request = server.next().await;
                assert!(
                    request
                        .head
                        .to_ascii_lowercase()
                        .contains("range: bytes=3-")
                );
                drop(request.respond(response(
                    "416 Range Not Satisfiable",
                    &format!("Content-Range: bytes */{total}\r\n"),
                    b"",
                )));
                let result = run.await.unwrap();
                if total == 3 {
                    assert!(matches!(result.unwrap(), Outcome::Done));
                    assert_eq!(tokio::fs::read(&path).await.unwrap(), b"abc");
                } else {
                    assert!(result.is_err());
                    assert!(!path.exists());
                    assert_eq!(tokio::fs::read(part_path(&path)).await.unwrap(), b"abc");
                }
            }
        }

        #[tokio::test]
        async fn download_file_rejects_wrong_range_and_failed_responses_without_touching_partial() {
            for reply in [
                response(
                    "206 Partial Content",
                    "Content-Range: bytes 2-4/5\r\n",
                    b"def",
                ),
                response("503 Unavailable", "", b"unavailable"),
                response(
                    "200 OK",
                    "Content-Type: text/html\r\n",
                    b"<html>error</html>",
                ),
            ] {
                let mut server = Server::new().await;
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("video.mp4");
                tokio::fs::write(part_path(&path), b"abc").await.unwrap();
                let run = start(context().await, server.url.clone(), path.clone());
                drop(server.next().await.respond(reply));
                assert!(run.await.unwrap().is_err());
                assert!(!path.exists());
                assert_eq!(tokio::fs::read(part_path(&path)).await.unwrap(), b"abc");
            }
        }

        #[tokio::test]
        async fn download_file_rejects_truncated_body_and_retains_resumable_prefix() {
            for reply in [
                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 3-8/9\r\nContent-Length: 6\r\nConnection: close\r\n\r\ndef".to_vec(),
                // Body framing is valid, but the advertised full range is incomplete.
                response("206 Partial Content", "Content-Range: bytes 3-8/9\r\n", b"def"),
            ] {
                let mut server = Server::new().await;
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("video.mp4");
                tokio::fs::write(part_path(&path), b"abc").await.unwrap();
                let run = start(context().await, server.url.clone(), path.clone());
                drop(server.next().await.respond(reply));
                assert!(run.await.unwrap().is_err());
                assert!(!path.exists());
                let bytes = tokio::fs::read(part_path(&path)).await.unwrap();
                assert!(bytes.starts_with(b"abc"));
                assert!(b"abcdef".starts_with(&bytes));
            }
        }

        #[tokio::test]
        async fn download_file_pause_and_cancel_flush_prefix_while_body_is_pending() {
            for cancel in [false, true] {
                let mut server = Server::new().await;
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("video.mp4");
                tokio::fs::write(part_path(&path), b"abc").await.unwrap();
                let ctx = context().await;
                let run = start(ctx.clone(), server.url.clone(), path.clone());
                let held = server.next().await.respond(b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 3-8/9\r\nContent-Length: 6\r\n\r\ndef".to_vec());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while tokio::fs::metadata(part_path(&path)).await.unwrap().len() != 6 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                ctx.transfers.update_task("test", |task| {
                    if cancel {
                        task.state = TaskState::Cancelled;
                    } else {
                        task.phase = "pausing".into();
                    }
                });
                let outcome = tokio::time::timeout(Duration::from_secs(5), run)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert!(if cancel {
                    matches!(outcome, Outcome::Cancelled)
                } else {
                    matches!(outcome, Outcome::Paused)
                });
                assert_eq!(tokio::fs::read(part_path(&path)).await.unwrap(), b"abcdef");
                assert!(!path.exists());
                drop(held);
                // The outer download_video owns cancel deletion; download_file only flushes.
            }
        }

        async fn lifecycle_fixture(
            root: &Path,
        ) -> (Arc<AppCtx>, crate::storage::Database, VideoCard) {
            let common = tokio::sync::watch::channel(crate::configuration::app::Config {
                p91_download_path: root.join("output"),
                temp_path: root.join("partial"),
                ..Default::default()
            })
            .1;
            let database = crate::storage::Database::open(":memory:").await.unwrap();
            let ctx = Arc::new(
                AppCtx::new(
                    crate::p91::config::Config {
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
                vid: None,
                hd: false,
                original: false,
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
                    assert!(
                        ctx.transfers
                            .register_task(TaskInfo::new(&card.id, &card.url))
                    );
                    let slot = ctx.download_slot(&card.id).await.unwrap();
                    ctx.transfers
                        .update_task(&card.id, |task| task.state = TaskState::Running);
                    let cfg = ctx.config();
                    let cache = cfg.save_path.clone();
                    tokio::fs::create_dir_all(&cache).await.unwrap();
                    tokio::fs::write(
                        part_path(&output_path(&cache, "test", "fixture")),
                        b"cached",
                    )
                    .await
                    .unwrap();
                    // Hold the actual connection lock. The repository's write lock
                    // proves the finalizer has entered persistence, past cleanup.
                    let gate = database.connection().await;
                    if database_error {
                        gate.execute("DROP TABLE p91_records", ()).await.unwrap();
                    }
                    let owner_ctx = ctx.clone();
                    let run = ctx
                        .transfers
                        .jobs
                        .spawn(async move {
                            let _slot = slot;
                            if completed {
                                finish_completed(
                                    &owner_ctx,
                                    &card,
                                    &card.title,
                                    &cfg.save_path.join("fixture.mp4"),
                                    7,
                                )
                                .await;
                            } else {
                                finish_failed(&owner_ctx, &card, "fixture failure").await;
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
                    assert!(part_path(&output_path(&cache, "test", "fixture")).exists());
                    assert!(!ctx.transfers.stop_task("test", false));
                    assert!(!ctx.transfers.stop_task("test", true));
                    assert!(!crate::p91::scheduler::resume_task(ctx.clone(), "test"));
                    assert_eq!(ctx.transfers.task_state("test"), Some(TaskState::Running));
                    let shutdown = ctx.shutdown();
                    tokio::pin!(shutdown);
                    assert!(futures_util::poll!(&mut shutdown).is_pending());
                    assert!(!run.is_finished());
                    assert!(!ctx.transfers.register_task(TaskInfo::new("new", "u")));
                    drop(gate);
                    tokio::time::timeout(Duration::from_secs(5), &mut shutdown)
                        .await
                        .unwrap();
                    run.await.unwrap();
                    assert_eq!(
                        ctx.transfers.task_state("test"),
                        Some(if completed && !database_error {
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
                    assert!(
                        ctx.transfers
                            .register_task(TaskInfo::new(&card.id, &card.url))
                    );
                    let _slot = ctx.download_slot(&card.id).await.unwrap();
                    ctx.transfers
                        .update_task(&card.id, |task| task.state = TaskState::Running);
                    let cfg = ctx.config();
                    let cache = cfg.save_path.clone();
                    tokio::fs::create_dir_all(&cache).await.unwrap();
                    tokio::fs::write(
                        part_path(&output_path(&cache, "test", "fixture")),
                        b"cached",
                    )
                    .await
                    .unwrap();
                    assert!(ctx.transfers.stop_task("test", cancel));
                    if completed {
                        finish_completed(
                            &ctx,
                            &card,
                            &card.title,
                            &cfg.save_path.join("fixture.mp4"),
                            7,
                        )
                        .await;
                    } else {
                        finish_failed(&ctx, &card, "fixture failure").await;
                    }
                    assert_eq!(
                        ctx.transfers.task_state("test"),
                        Some(if cancel {
                            TaskState::Cancelled
                        } else {
                            TaskState::Paused
                        })
                    );
                    assert!(ctx.history().is_empty());
                    assert_eq!(
                        part_path(&output_path(&cache, "test", "fixture")).exists(),
                        !cancel
                    );
                    if !cancel {
                        assert_eq!(
                            tokio::fs::read(part_path(&output_path(&cache, "test", "fixture")))
                                .await
                                .unwrap(),
                            b"cached"
                        );
                    }
                }
            }
        }
    }
}
