use std::sync::Arc;
use std::time::Instant;

use log::info;
use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};
use tokio::sync::Mutex;

use crate::telegram::config::Config;
use crate::telegram::storage::{AppData, ChatData};

/// Commit the dedup cache, retry sets, and scan cursors in one transaction.
pub(super) async fn persist_state(
    database: &crate::storage::Database,
    file_ids: &Arc<Mutex<HashMap<String, u64>>>,
    data_chats: &HashMap<String, ChatData>,
    cutoff: u64,
) -> anyhow::Result<()> {
    file_ids.lock().await.retain(|_, time| *time > cutoff);
    let mut data = AppData::default();
    let mut ids: Vec<(String, u64)> = file_ids
        .lock()
        .await
        .iter()
        .map(|(id, time)| (id.clone(), *time))
        .collect();
    ids.sort();
    data.downloaded_file_ids = ids;
    let mut chats: Vec<ChatData> = data_chats.values().cloned().collect();
    chats.sort_by(|a, b| a.chat_id.cmp(&b.chat_id));
    data.chat = chats;
    crate::telegram::storage::save(database, &data).await
}

pub(super) async fn sync_retry_set(
    data_chats: &mut HashMap<String, ChatData>,
    chat_id: &str,
    live_retry: &Arc<Mutex<HashSet<i32>>>,
) {
    let mut retry: Vec<i32> = live_retry.lock().await.iter().copied().collect();
    retry.sort_unstable();
    let chat = data_chats
        .entry(chat_id.to_string())
        .or_insert_with(|| ChatData {
            chat_id: chat_id.to_string(),
            ..ChatData::default()
        });
    chat.ids_to_retry = retry;
}

/// Log a one-time summary of the loaded configuration at startup.
pub(super) fn log_config_summary(
    cfg: &Config,
    data_chats: &HashMap<String, ChatData>,
    concurrency: usize,
) {
    let pending: usize = data_chats.values().map(|c| c.ids_to_retry.len()).sum();
    info!(
        "config: {} chat(s), {concurrency} parallel download slot(s), save_path={}",
        cfg.chat.len(),
        cfg.save_path.display()
    );
    info!("config: media_types=[{}]", cfg.media_types.join(","));
    for c in &cfg.chat {
        let retry = data_chats
            .get(&c.chat_id)
            .map(|d| d.ids_to_retry.len())
            .unwrap_or(0);
        info!(
            "config: chat '{}' from msg {} ({} id(s) queued for retry){}",
            c.chat_id,
            data_chats
                .get(&c.chat_id)
                .map_or(0, |data| data.last_read_message_id),
            retry,
            c.download_filter
                .as_deref()
                .map(|f| format!(" filter='{f}'"))
                .unwrap_or_default()
        );
    }
    if pending > 0 {
        info!("config: resuming with {pending} message id(s) pending retry across all chats");
    }
}

/// Log what was preserved across the run so the user knows resume is safe.
pub(super) async fn log_shutdown_summary(
    cfg: &Config,
    data_chats: &HashMap<String, ChatData>,
    file_ids: &Arc<Mutex<HashMap<String, u64>>>,
    started: Instant,
) {
    let file_id_count = file_ids.lock().await.len();
    let pending: usize = data_chats.values().map(|c| c.ids_to_retry.len()).sum();
    info!(
        "shutdown: ran for {:.1}s; {file_id_count} file id(s) cached, {pending} message id(s) pending retry",
        started.elapsed().as_secs_f64()
    );
    for c in &cfg.chat {
        let retry = data_chats
            .get(&c.chat_id)
            .map(|d| d.ids_to_retry.len())
            .unwrap_or(0);
        info!(
            "shutdown: chat '{}' last_read={} ({} id(s) to retry) - partial downloads kept as .part for resume",
            c.chat_id,
            data_chats
                .get(&c.chat_id)
                .map_or(0, |data| data.last_read_message_id),
            retry
        );
    }
    info!("shutdown: runtime state flushed to SQLite");
}
