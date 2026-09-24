use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use grammers_client::Client;
use grammers_client::media::Media;
use indicatif::{MultiProgress, ProgressBar};
use log::{debug, info, warn};
use rustc_hash::FxHashMap as HashMap;
use tokio::sync::Mutex;

use crate::telegram::api::ApiState;
use crate::telegram::app::{Shutdown, sleep_cancellable, wait_paused};
use crate::telegram::config::Config;
use crate::telegram::format::format_byte;

use super::chunks::{download_concurrent, download_unknown_size};
use super::finalize::{discard_partial, finalize_download};
use super::paths::build_media_paths;
use super::progress::{DownloadProgress, progress_style, resume_offset};

const RETRY_LIMIT: u32 = 3;
const RETRY_DELAY_SECS: u64 = 5;

pub(crate) async fn download_media_inner(
    client: &Client,
    msg: &grammers_client::message::Message,
    cfg: &Config,
    file_ids: &Arc<Mutex<HashMap<String, u64>>>,
    mp: &MultiProgress,
    web_state: &Arc<ApiState>,
    shutdown: &Shutdown,
) -> anyhow::Result<bool> {
    let media = match msg.media() {
        Some(m) => m,
        None => return Ok(false),
    };
    let msg_id = msg.id();
    if let Media::Document(doc) = &media
        && doc
            .mime_type()
            .is_some_and(|mime| mime.starts_with("video/"))
        && let Some(reason) = crate::runtime::video_resolution::rejection(
            web_state.minimum_video_resolution(),
            doc.resolution().and_then(|(width, height)| {
                Some((u64::try_from(width).ok()?, u64::try_from(height).ok()?))
            }),
        )
    {
        info!("msg={msg_id}: {reason}");
        return Ok(false);
    }
    let source_name = msg
        .peer()
        .and_then(|peer| peer.name())
        .unwrap_or_default()
        .to_string();
    let paths = build_media_paths(msg, &media, cfg)?;
    let temp_path = &paths.temp;
    let final_path = &paths.final_path;

    // Check file_unique_id cache
    let fid = match &media {
        Media::Photo(p) => p.id().to_string(),
        Media::Document(d) => d.id().to_string(),
        _ => String::new(),
    };
    if !fid.is_empty() {
        let mut cache = file_ids.lock().await;
        if cache
            .get(&fid)
            .is_some_and(|time| *time > web_state.history_cutoff())
        {
            // History predates stored media IDs. A durable path override lets
            // forgotten entries be requested again without resetting other media.
            let forgotten = crate::telegram::storage::history::was_forgotten(
                &*web_state.database.connection().await,
                &final_path.to_string_lossy(),
            )
            .await?;
            if !forgotten {
                debug!("msg={msg_id}: already downloaded (file_unique_id), skipped");
                return Ok(false);
            }
            cache.remove(&fid);
            drop(cache);
            if let Err(error) =
                crate::telegram::storage::forget_file_id(&web_state.database, &fid).await
            {
                warn!("msg={msg_id}: cannot forget file id: {error:#}");
            }
        }
    }

    // Already fully downloaded?
    match tokio::fs::metadata(final_path).await {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file(),
                "download destination is not a file: {}",
                final_path.display()
            );
            debug!("msg={msg_id}: file already exists, marking complete");
            super::finalize::remember_file_id(msg_id, &fid, file_ids, web_state).await;
            let size = metadata.len();
            web_state
                .download_started(msg_id, final_path, size, size, &source_name)
                .await;
            web_state.download_finished(msg_id, size, true).await;
            return Ok(true);
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    tokio::fs::create_dir_all(temp_path.parent().unwrap_or(Path::new("."))).await?;

    paths.validate_temp()?;
    paths.validate_final()?;
    crate::migration::relocate_partial(final_path, temp_path, &web_state.database).await?;

    let total = match &media {
        Media::Photo(p) => p.size().unwrap_or(0) as u64,
        Media::Document(d) => d.size().unwrap_or(0) as u64,
        _ => 0,
    };
    let existing = resume_offset(temp_path, total, &web_state.database).await?;
    web_state
        .download_started(msg_id, final_path, existing, total, &source_name)
        .await;

    // The database checkpoint marks a prior run as fully fetched but not yet renamed into
    // place. Promote it directly instead of re-fetching the whole file.
    if total > 0 && existing == total {
        info!(
            "msg={msg_id}: already complete, finalizing {}",
            format_byte(total as f64)
        );
        finalize_download(msg_id, &fid, file_ids, &paths, total, web_state).await?;
        return Ok(true);
    }

    if existing > 0 {
        info!(
            "msg={msg_id}: resuming from {} of {}",
            format_byte(existing as f64),
            format_byte(total as f64)
        );
    } else {
        info!(
            "msg={msg_id}: downloading {} -> {}",
            format_byte(total as f64),
            final_path.display()
        );
    }

    let mut last_err = None;
    for attempt in 0..RETRY_LIMIT {
        if shutdown.is_cancelled() {
            break;
        }
        if attempt > 0 {
            info!("msg={msg_id}: retry {}/{}", attempt + 1, RETRY_LIMIT);
            if sleep_cancellable(shutdown, Duration::from_secs(RETRY_DELAY_SECS)).await {
                break;
            }
        }

        paths.validate_temp()?;
        // Re-evaluate the resume offset each attempt: a failed attempt leaves
        // the valid prefix on disk, so we only re-fetch what is missing.
        let downloaded = if attempt == 0 {
            existing
        } else {
            resume_offset(temp_path, total, &web_state.database).await?
        };

        let pb = mp.add(ProgressBar::new(total));
        pb.set_style(progress_style());
        pb.set_message(format!("{msg_id}"));
        if downloaded > 0 {
            pb.set_position(downloaded);
        }

        if total == 0 {
            if !wait_paused(web_state, shutdown).await {
                pb.finish_and_clear();
                break;
            }
            let progress = DownloadProgress {
                pb: &pb,
                web_state,
                msg_id,
            };
            let outcome =
                download_unknown_size(client, &media, temp_path, &progress, shutdown).await;
            if let Err(e) = outcome {
                last_err = Some(e);
                pb.finish_and_clear();
                drop(pb);
                if shutdown.is_cancelled() {
                    break;
                }
                discard_partial(&paths, &web_state.database).await;
                continue;
            }
            pb.set_position(total);
        } else if total > downloaded {
            let progress = DownloadProgress {
                pb: &pb,
                web_state,
                msg_id,
            };
            if let Err(e) = download_concurrent(
                client,
                &media,
                temp_path,
                downloaded..total,
                cfg.download_connections.max(1) as u64,
                &progress,
                shutdown,
            )
            .await
            {
                // Keep the partial file so the next attempt resumes; only the
                // unfetched chunks need re-downloading.
                if !shutdown.is_cancelled() {
                    warn!("msg={msg_id}: {e:#}");
                }
                last_err = Some(e);
                pb.finish_and_clear();
                drop(pb);
                continue;
            }
        } else {
            pb.set_position(downloaded);
        }

        pb.finish_and_clear();
        drop(pb);

        let actual = tokio::fs::metadata(temp_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        if total > 0 && actual != total {
            warn!("msg={msg_id}: size mismatch ({actual} vs {total}) - retrying");
            discard_partial(&paths, &web_state.database).await;
            last_err = Some(anyhow::anyhow!("size mismatch"));
            continue;
        }

        finalize_download(msg_id, &fid, file_ids, &paths, actual, web_state).await?;
        return Ok(true);
    }

    // On shutdown, keep the `.part` file so the download resumes next run. On a
    // real failure (retries exhausted), delete it so we start fresh.
    if !shutdown.is_cancelled() {
        discard_partial(&paths, &web_state.database).await;
    }
    web_state.download_finished(msg_id, 0, false).await;
    if shutdown.is_cancelled() {
        return Err(anyhow::anyhow!("download interrupted"));
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("download failed")))
}
