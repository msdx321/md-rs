use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use grammers_client::Client;
use grammers_session::types::PeerRef;
use indicatif::MultiProgress;
use log::{debug, error, info, warn};
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::api::ApiState;
use crate::config::{ChatConfig, Config};
use crate::downloader::{
    download_media_inner, file_extension_value, media_duration_value, media_file_name_value,
    media_file_size_value, media_matches_config, media_resolution_value, media_type_value,
};
use crate::filter::{Parser, Value, VarLookup};
use crate::format::replace_date_time;
use crate::storage::ChatData;

use super::shutdown::{Shutdown, flood_wait_secs, sleep_cancellable, wait_paused};
use super::state::{persist_state, sync_retry_set};

/// Outcome of scanning one chat.
///
/// `completed` is false when the scan was interrupted by shutdown; in that case
/// `last_id` is the chat's existing `last_read_message_id` (unchanged) so the
/// unscanned range is revisited next run, and any in-flight downloads are
/// captured in the live retry set.
struct ChatOutcome {
    completed: bool,
    last_id: i32,
}

pub(super) struct DownloadRuntime {
    pub(super) file_ids: Arc<Mutex<HashMap<String, u64>>>,
    pub(super) dl_sem: Arc<Semaphore>,
    pub(super) mp: Arc<MultiProgress>,
    pub(super) web_state: Arc<ApiState>,
}

/// Counters for the per-chat scan summary, updated by spawned download tasks.
#[derive(Default)]
struct ChatStats {
    scanned: AtomicU64,
    downloaded: AtomicU64,
    skipped: AtomicU64,
    failed: AtomicU64,
}

/// Run one scan pass over every configured chat.
///
/// Returns `true` if the whole cycle ran to completion, `false` if it was cut
/// short by shutdown. In either case `data_chats` is left holding the live
/// retry sets, which `run_downloader` persists immediately after this returns.
pub(super) async fn run_check_cycle(
    client: &Client,
    cfg: &mut Config,
    runtime: &DownloadRuntime,
    data_chats: &mut HashMap<String, ChatData>,
    shutdown: &Shutdown,
) -> anyhow::Result<bool> {
    let chats = cfg.chat.clone();
    for chat_cfg in &chats {
        if shutdown.is_cancelled() {
            return Ok(false);
        }
        let chat_id = chat_cfg.chat_id.clone();
        // Retry ids live in SQLite (data_chats). The scan mutates this set
        // in place as downloads succeed or fail, so an interrupt still leaves a
        // correct resume set behind.
        let old_retry: HashSet<i32> = data_chats
            .get(&chat_id)
            .map(|dc| dc.ids_to_retry.iter().copied().collect())
            .unwrap_or_default();
        let live_retry = Arc::new(Mutex::new(old_retry));

        match process_chat(
            client,
            cfg,
            chat_cfg,
            data_chats
                .get(&chat_id)
                .map_or(0, |data| data.last_read_message_id),
            live_retry.clone(),
            runtime,
            shutdown,
        )
        .await
        {
            Ok(outcome) => {
                sync_retry_set(data_chats, &chat_id, &live_retry).await;

                if outcome.completed || shutdown.work_cancelled() {
                    // The cursor and retry IDs must become durable together.
                    if let Some(data) = data_chats.get_mut(&chat_id) {
                        data.last_read_message_id = data.last_read_message_id.max(outcome.last_id);
                    }
                    persist_state(
                        &runtime.web_state.database,
                        &runtime.file_ids,
                        data_chats,
                        runtime.web_state.history_cutoff(),
                    )
                    .await?;
                    info!(
                        "chat {}: scan complete - last_read advanced to {}, {} id(s) pending retry",
                        chat_cfg.chat_id,
                        outcome.last_id,
                        data_chats
                            .get(&chat_cfg.chat_id)
                            .map(|d| d.ids_to_retry.len())
                            .unwrap_or(0)
                    );
                } else {
                    info!(
                        "chat {}: scan interrupted at msg {} - state preserved for resume",
                        chat_cfg.chat_id, outcome.last_id
                    );
                    return Ok(false);
                }
            }
            Err(e) => {
                // A real error (peer resolution, etc.). Still flush the live
                // retry set so partial progress survives, then keep going unless
                // we were also asked to shut down.
                sync_retry_set(data_chats, &chat_id, &live_retry).await;
                error!("chat {chat_id}: {e:#}");
                if shutdown.is_cancelled() {
                    return Ok(false);
                }
            }
        }
    }
    Ok(true)
}

async fn process_chat(
    client: &Client,
    cfg: &Config,
    chat_cfg: &ChatConfig,
    last_read_message_id: i32,
    live_retry: Arc<Mutex<HashSet<i32>>>,
    runtime: &DownloadRuntime,
    shutdown: &Shutdown,
) -> anyhow::Result<ChatOutcome> {
    // `retry_ids` mirrors the live set purely for scan-control: it tracks which
    // retry ids have NOT yet been seen so the scan knows when it can stop. The
    // live set itself is mutated by the download tasks (success removes, failure
    // inserts) and is what gets persisted as `ids_to_retry`.
    let mut retry_ids = live_retry.lock().await.clone();
    let mut last_id = last_read_message_id;
    let stats = Arc::new(ChatStats::default());

    info!(
        "chat {}: beginning scan (from msg {}, {} id(s) to retry)",
        chat_cfg.chat_id,
        last_id,
        retry_ids.len()
    );

    let peer = resolve_chat(client, &chat_cfg.chat_id).await?;

    let mut messages = client.iter_messages(peer);

    let filter_fn = build_filter_fn(chat_cfg, cfg);
    let task_cfg = Arc::new(cfg.clone());
    let mut tasks = JoinSet::new();

    loop {
        if shutdown.is_cancelled() {
            break;
        }
        if !wait_paused(&runtime.web_state, shutdown).await {
            break;
        }

        let msg = tokio::select! {
            r = messages.next() => match r {
                Ok(Some(m)) => m,
                Ok(None) => break,
                Err(e) => {
                    let err_str = e.to_string();
                    let (secs, label) = match flood_wait_secs(&err_str) {
                        Some(s) => (s, "FLOOD_WAIT"),
                        None => (1, "iterator error"),
                    };
                    warn!("chat {}: {label} - sleeping {secs}s", chat_cfg.chat_id);
                    if sleep_cancellable(shutdown, Duration::from_secs(secs)).await {
                        break;
                    }
                    continue;
                }
            },
            _ = shutdown.cancelled() => break,
        };

        stats.scanned.fetch_add(1, Ordering::Relaxed);
        let msg_id = msg.id();

        if msg_id <= last_read_message_id && !retry_ids.remove(&msg_id) {
            if retry_ids.is_empty() {
                break;
            }
            continue;
        }

        if msg_id > last_id {
            last_id = msg_id;
        }

        let Some(media) = msg.media() else {
            continue;
        };
        if !media_matches_config(&media, cfg) {
            debug!("msg {msg_id}: media type not in config, skip");
            continue;
        }

        if !filter_fn(&msg) {
            debug!("msg {msg_id}: filtered out");
            continue;
        }

        let client = client.clone();
        let cfg = task_cfg.clone();
        let file_ids = runtime.file_ids.clone();
        let live_retry = live_retry.clone();
        let stats = stats.clone();
        let shutdown = shutdown.clone();
        let permit = tokio::select! {
            p = runtime.dl_sem.clone().acquire_owned() => p?,
            _ = shutdown.cancelled() => break,
        };

        let mp = runtime.mp.clone();
        let web_state = runtime.web_state.clone();
        tasks.spawn(async move {
            let _permit = permit;
            match download_media_inner(
                &client,
                &msg,
                &cfg,
                &file_ids,
                mp.as_ref(),
                &web_state,
                &shutdown,
            )
            .await
            {
                Ok(true) => {
                    stats.downloaded.fetch_add(1, Ordering::Relaxed);
                    live_retry.lock().await.remove(&msg.id());
                }
                Ok(false) => {
                    stats.skipped.fetch_add(1, Ordering::Relaxed);
                    live_retry.lock().await.remove(&msg.id());
                }
                Err(e) => {
                    if shutdown.is_cancelled() {
                        debug!("msg {}: interrupted - kept for resume", msg.id());
                    } else {
                        warn!("msg {}: download failed - {e:#}", msg.id());
                        stats.failed.fetch_add(1, Ordering::Relaxed);
                        live_retry.lock().await.insert(msg.id());
                    }
                }
            }
        });
        drain_finished_tasks(&mut tasks);
    }

    // Process shutdown preserves partial files. User cancellation drains writers
    // cooperatively before the worker deletes their files and checkpoints.
    if shutdown.is_cancelled() && !shutdown.work_cancelled() {
        tasks.abort_all();
    }
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(e) => warn!("download task panicked: {e}"),
        }
    }

    let completed = !shutdown.is_cancelled();
    let last_id = if completed || shutdown.work_cancelled() {
        last_id
    } else {
        last_read_message_id
    };
    info!(
        "chat {}: scanned {} msg(s) - downloaded {}, skipped {}, failed {}, last_id={}{}",
        chat_cfg.chat_id,
        stats.scanned.load(Ordering::Relaxed),
        stats.downloaded.load(Ordering::Relaxed),
        stats.skipped.load(Ordering::Relaxed),
        stats.failed.load(Ordering::Relaxed),
        last_id,
        if completed { "" } else { " [interrupted]" }
    );

    Ok(ChatOutcome { completed, last_id })
}

pub(super) async fn run_message_download(
    client: &Client,
    cfg: &Config,
    runtime: &DownloadRuntime,
    peer: PeerRef,
    message_id: i32,
    shutdown: &Shutdown,
) -> anyhow::Result<()> {
    if !wait_paused(&runtime.web_state, shutdown).await {
        anyhow::bail!("download interrupted");
    }

    let message = client
        .get_messages_by_id(peer, &[message_id])
        .await?
        .into_iter()
        .next()
        .flatten()
        .with_context(|| format!("message {message_id} was not found"))?;
    let media = message
        .media()
        .with_context(|| format!("message {message_id} has no media"))?;
    if !media_matches_config(&media, cfg) {
        anyhow::bail!("message {message_id} media type is disabled by config");
    }

    let _permit = tokio::select! {
        permit = runtime.dl_sem.clone().acquire_owned() => permit?,
        _ = shutdown.cancelled() => anyhow::bail!("download interrupted"),
    };
    download_media_inner(
        client,
        &message,
        cfg,
        &runtime.file_ids,
        runtime.mp.as_ref(),
        &runtime.web_state,
        shutdown,
    )
    .await?;
    Ok(())
}

pub(super) async fn resolve_chat(client: &Client, chat_id: &str) -> anyhow::Result<PeerRef> {
    if let Ok(dialog_id) = chat_id.parse::<i64>() {
        let mut dialogs = client.iter_dialogs();
        while let Some(dialog) = dialogs.next().await? {
            if dialog.peer_id().bot_api_dialog_id() == Some(dialog_id) {
                return Ok(dialog.peer_ref());
            }
        }
        anyhow::bail!("chat {chat_id} is not accessible to the signed-in account");
    }

    client
        .resolve_username(chat_id)
        .await?
        .context("cannot resolve peer")?
        .to_ref()
        .await
        .map_err(|error| anyhow::anyhow!("{error}"))?
        .context("peer not found")
}

fn drain_finished_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.try_join_next() {
        match result {
            Ok(()) => {}
            Err(e) if e.is_cancelled() => {}
            Err(e) => warn!("download task panicked: {e}"),
        }
    }
}

fn build_filter_fn(
    chat_cfg: &ChatConfig,
    cfg: &Config,
) -> Box<dyn Fn(&grammers_client::message::Message) -> bool + Send + Sync> {
    let filter_str = match &chat_cfg.download_filter {
        Some(fs) => replace_date_time(fs, &cfg.date_format),
        None => return Box::new(|_| true),
    };
    let parser = Parser::new(&filter_str);

    Box::new(move |msg| {
        let vars = MessageVars(msg);
        match parser.parse(&vars) {
            Ok(Value::Bool(b)) => b,
            Ok(_) => false,
            Err(e) => {
                warn!("filter parse error for msg {}: {e}", msg.id());
                false
            }
        }
    })
}

struct MessageVars<'a>(&'a grammers_client::message::Message);

impl VarLookup for MessageVars<'_> {
    fn get_var(&self, name: &str) -> Option<Value> {
        let msg = self.0;
        Some(match name {
            "message_date" => Value::DateTime(msg.date().naive_utc()),
            "message_id" | "id" => Value::Int(msg.id() as i64),
            "message_caption" | "caption" => Value::Str(msg.text().to_string()),
            "sender_name" => Value::Str(
                msg.peer()
                    .and_then(|p| p.name())
                    .map(str::to_string)
                    .unwrap_or_default(),
            ),
            "sender_id" | "message_thread_id" | "topic_id" => Value::Int(0),
            "reply_to_message_id" => Value::Int(msg.reply_to_message_id().unwrap_or(0) as i64),
            "media_type" => Value::Str(media_type_value(msg)),
            "file_extension" => Value::Str(file_extension_value(msg)),
            "media_file_name" | "file_name" => Value::Str(media_file_name_value(msg)),
            "media_file_size" | "file_size" => Value::Int(media_file_size_value(msg)),
            "media_duration" => Value::Int(media_duration_value(msg)),
            "media_width" => Value::Int(media_resolution_value(msg).0),
            "media_height" => Value::Int(media_resolution_value(msg).1),
            _ => return None,
        })
    }
}
