mod auth;
mod cancellation;
mod scan;
pub(crate) mod setup;
mod shutdown;
mod state;

use std::sync::{Arc, atomic::Ordering};
use std::time::Instant;

use anyhow::Context;
use grammers_client::Client;
use grammers_mtsender::SenderPool;
use grammers_session::storages::SqliteSession;
use grammers_session::types::PeerRef;
use indicatif::MultiProgress;
use log::{debug, info};
use rustc_hash::FxHashMap as HashMap;
use tokio::sync::{Mutex, Semaphore, mpsc};

use crate::api::{ApiState, ChatRequest, ChatTarget};
use crate::config::{ChatConfig, Config, FILE};
use crate::storage::ChatData;

use auth::authorize_web;
use scan::{DownloadRuntime, run_check_cycle};
pub(crate) use shutdown::{Shutdown, flood_wait_secs, sleep_cancellable, wait_paused};
use state::{log_config_summary, log_shutdown_summary, persist_state};

const SESSION_FILE: &str = "sessions/tmd.session";

pub(crate) async fn run_downloader(
    mut cfg: Config,
    web_state: Arc<ApiState>,
    download_rx: &mut mpsc::Receiver<ChatRequest>,
    shutdown: Shutdown,
    schedule: tokio::sync::watch::Receiver<media_config::app::Config>,
) -> anyhow::Result<()> {
    cfg.save_path = schedule.borrow().telegram_download_path.clone();
    cfg.temp_path = schedule.borrow().temp_path.join("telegram");
    web_state.set_status("running").await;

    let data = crate::storage::load(&web_state.database).await?;
    std::fs::create_dir_all(&cfg.save_path)?;
    std::fs::create_dir_all("sessions")?;

    let session = Arc::new(SqliteSession::open(SESSION_FILE).await?);
    let SenderPool {
        runner,
        handle: pool_handle,
        ..
    } = SenderPool::new(session.clone(), cfg.api_id);
    let client = Client::new(pool_handle);
    let _runner_handle = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(runner.run()));

    if !client.is_authorized().await.unwrap_or(false) {
        tokio::select! {
            result = authorize_web(&client, &cfg.api_hash, &web_state) => result?,
            _ = shutdown.cancelled() => return Ok(()),
        }
        info!("Session saved to {SESSION_FILE}");
    }
    web_state
        .login_status("ready", "Connected to Telegram")
        .await;
    info!("Authorized - ready");

    let file_ids: Arc<Mutex<HashMap<String, u64>>> =
        Arc::new(Mutex::new(data.downloaded_file_ids.into_iter().collect()));

    let mut data_chats: HashMap<String, ChatData> = data
        .chat
        .into_iter()
        .map(|d| (d.chat_id.clone(), d))
        .collect();

    let concurrency = cfg.max_download_task;
    let dl_semaphore = Arc::new(Semaphore::new(concurrency));
    let mp = Arc::new(MultiProgress::new());
    let mut runtime = DownloadRuntime {
        file_ids: file_ids.clone(),
        dl_sem: dl_semaphore,
        mp,
        web_state: web_state.clone(),
    };

    log_config_summary(&cfg, &data_chats, concurrency);

    let mut settings_rx = web_state.settings_changed.subscribe();
    let started = Instant::now();
    let mut timer = media_runtime::schedule::Timer::new(
        schedule.clone(),
        media_runtime::schedule::Module::Telegram,
    );
    let mut scan_due = false;
    let mut cycle_no: u64 = 0;
    loop {
        if web_state.cancelling.load(Ordering::Relaxed) {
            let _cancellation = web_state.download_cancel.lock().await;
            while download_rx.try_recv().is_ok() {}
            cancellation::discard_pending(&web_state.database, &mut data_chats).await?;
            persist_state(
                &web_state.database,
                &file_ids,
                &data_chats,
                web_state.history_cutoff(),
            )
            .await?;
            web_state.cancelling.store(false, Ordering::Relaxed);
            web_state
                .set_status("Cancelled. Partial files and retry data deleted.")
                .await;
        }
        let work_shutdown = shutdown.with_work(web_state.download_cancel.lock().await.clone());
        if !wait_paused(&web_state, &work_shutdown).await {
            if shutdown.is_cancelled() {
                break;
            }
            if web_state.cancelling.load(Ordering::Relaxed) {
                continue;
            }
            // A cancelled token stays cancelled until Resume. Do not spin while paused.
            tokio::select! {
                _ = shutdown.cancelled() => break,
                _ = web_state.wait_if_paused() => {},
            }
            continue;
        }
        settings_rx.borrow_and_update();
        // Each scan drains its transfers before returning. A fresh semaphore
        // applies concurrency changes without disturbing an active download.
        {
            let _edit = web_state.config_update.lock().await;
            cfg = FILE.load_optional()?.unwrap_or(cfg);
            cfg.save_path = schedule.borrow().telegram_download_path.clone();
            cfg.temp_path = schedule.borrow().temp_path.join("telegram");
            for (chat_id, cursor) in crate::storage::cursors::apply(&web_state.database).await? {
                let chat = data_chats
                    .entry(chat_id.clone())
                    .or_insert_with(|| ChatData {
                        chat_id,
                        ..ChatData::default()
                    });
                chat.last_read_message_id = cursor;
            }
        }
        runtime.dl_sem = Arc::new(Semaphore::new(cfg.max_download_task));
        std::fs::create_dir_all(&cfg.save_path)?;
        if scan_due {
            scan_due = false;
            cycle_no += 1;
            let cycle_started = Instant::now();
            let completed =
                run_check_cycle(&client, &mut cfg, &runtime, &mut data_chats, &work_shutdown)
                    .await?;

            // Persist after every cycle (full or interrupted) so SQLite tracks
            // the live file-id cache and per-chat retry sets even on shutdown.
            persist_state(
                &web_state.database,
                &file_ids,
                &data_chats,
                web_state.history_cutoff(),
            )
            .await?;
            debug!(
                "cycle {cycle_no}: persisted state to SQLite ({} file ids, {} chats pending)",
                file_ids.lock().await.len(),
                data_chats
                    .values()
                    .map(|c| c.ids_to_retry.len())
                    .sum::<usize>()
            );

            if shutdown.is_cancelled() {
                break;
            }
            if !completed || work_shutdown.is_cancelled() {
                continue;
            }
            info!(
                "cycle {cycle_no} complete in {:.1}s",
                cycle_started.elapsed().as_secs_f64()
            );
            timer.finished();
        }
        web_state.set_schedule(timer.next).await;
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = work_shutdown.cancelled() => continue,
            _ = settings_rx.changed() => {},
            due = timer.tick() => { scan_due = due; }
            request = download_rx.recv() => {
                cfg = FILE.load_optional()?.unwrap_or(cfg);
            cfg.save_path = schedule.borrow().telegram_download_path.clone();
            cfg.temp_path = schedule.borrow().temp_path.join("telegram");
                runtime.dl_sem = Arc::new(Semaphore::new(cfg.max_download_task));
                std::fs::create_dir_all(&cfg.save_path)?;
                match request {
                    Some(ChatRequest::Scan) => { scan_due = true; }
                    Some(ChatRequest::Once(target, message_id)) => {
                        let label = target.label();
                        web_state.set_status("running").await;
                        web_state
                            .set_request_status(&format!("Opening message {message_id} from {label}"))
                            .await;
                        match download_message(
                            &client,
                            &cfg,
                            &runtime,
                            target,
                            message_id,
                            &work_shutdown,
                        )
                        .await
                        {
                            Ok(chat_id) => {
                                persist_state(&web_state.database, &file_ids, &data_chats, web_state.history_cutoff()).await?;
                                web_state
                                    .set_request_status(&format!(
                                        "Finished downloading message {message_id} from {label} as {chat_id}"
                                    ))
                                    .await;
                            }
                            Err(error) => web_state
                                .set_request_status(&format!(
                                    "Could not download message {message_id} from {label}: {error:#}"
                                ))
                                .await,
                        }
                    }
                    Some(ChatRequest::Subscribe(target)) => {
                        let label = target.label();
                        web_state
                            .set_request_status(&format!("Opening {label}"))
                            .await;
                        match subscribe_chat(&client, &mut cfg, target, &web_state).await {
                            Ok((chat_id, true)) => web_state
                                .set_request_status(&format!("Subscribed to {label} as {chat_id}"))
                                .await,
                            Ok((_, false)) => web_state
                                .set_request_status(&format!("Already subscribed to {label}"))
                                .await,
                            Err(error) => web_state
                                .set_request_status(&format!("Could not subscribe to {label}: {error:#}"))
                                .await,
                        }
                    }
                    None => {}
                }
            }
        }
        web_state.set_status("running").await;
    }

    web_state.set_status("shutting down").await;
    log_shutdown_summary(&cfg, &data_chats, &file_ids, started).await;
    info!("graceful shutdown complete");
    Ok(())
}

async fn download_message(
    client: &Client,
    cfg: &Config,
    runtime: &DownloadRuntime,
    target: ChatTarget,
    message_id: i32,
    shutdown: &Shutdown,
) -> anyhow::Result<String> {
    let (chat_id, peer) = resolve_target(client, target).await?;
    scan::run_message_download(client, cfg, runtime, peer, message_id, shutdown).await?;
    Ok(chat_id)
}

async fn subscribe_chat(
    client: &Client,
    cfg: &mut Config,
    target: ChatTarget,
    state: &ApiState,
) -> anyhow::Result<(String, bool)> {
    if let ChatTarget::Username(username) = &target
        && cfg.chat.iter().any(|chat| chat.chat_id == *username)
    {
        return Ok((username.clone(), false));
    }

    let (chat_id, _) = resolve_target(client, target).await?;
    let _edit = state.config_update.lock().await;
    let mut next = FILE.load_optional()?.unwrap_or_else(|| cfg.clone());
    if next.chat.iter().any(|chat| chat.chat_id == chat_id) {
        return Ok((chat_id, false));
    }
    next.chat.push(ChatConfig {
        chat_id: chat_id.clone(),
        download_filter: None,
    });
    FILE.save(&next)?;
    *cfg = next;
    Ok((chat_id, true))
}

async fn resolve_target(client: &Client, target: ChatTarget) -> anyhow::Result<(String, PeerRef)> {
    if let ChatTarget::DialogId(id) = target {
        return Ok((
            id.to_string(),
            scan::resolve_chat(client, &id.to_string()).await?,
        ));
    }

    let peer = match target {
        ChatTarget::Username(username) => client
            .resolve_username(&username)
            .await?
            .with_context(|| format!("@{username} was not found"))?,
        ChatTarget::Invite(link) => client
            .accept_invite_link(&link)
            .await?
            .context("the invite did not return a chat")?,
        ChatTarget::DialogId(_) => unreachable!(),
    };
    let chat_id = peer.id().to_string();
    let peer = peer
        .to_ref()
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .context("peer not found")?;
    Ok((chat_id, peer))
}

#[cfg(test)]
mod tests {
    use super::flood_wait_secs;

    #[test]
    fn flood_wait_parses_grammers_value_form() {
        // The exact format grammers emitted in the wild.
        let err = "request error: rpc error 420: FLOOD_WAIT caused by \
                   auth.sendCode (value: 225)";
        assert_eq!(flood_wait_secs(err), Some(225));
    }

    #[test]
    fn flood_wait_parses_raw_error_type() {
        assert_eq!(flood_wait_secs("rpc error 420: FLOOD_WAIT_90"), Some(90));
    }

    #[test]
    fn flood_wait_non_flood_error_is_none() {
        assert_eq!(
            flood_wait_secs("rpc error 401: AUTH_KEY_UNREGISTERED"),
            None
        );
    }

    #[test]
    fn flood_wait_unreadable_defaults_to_60() {
        assert_eq!(flood_wait_secs("FLOOD_WAIT (no number given)"), Some(60));
    }
}
