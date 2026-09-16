use super::{imported, mark_imported};
use anyhow::Context;
use media_config::load as read_yaml;
use media_storage::{Connection, params};
use serde::Deserialize;
use serde_yaml::Value;
use std::{collections::BTreeMap, fs, path::Path};

pub(super) async fn import(tx: &Connection, telegram: Option<&Path>) -> anyhow::Result<()> {
    let old_telegram = imported(tx, "telegram-state").await?;
    let mut chats = BTreeMap::<String, Chat>::new();
    let mut markers = Vec::new();
    for path in [
        telegram,
        Some(Path::new(media_config::TELEGRAM_FILE)),
        Some(Path::new("telegram.yaml")),
    ]
    .into_iter()
    .flatten()
    {
        if old_telegram && path == Path::new("telegram.yaml") {
            continue;
        }
        let marker = format!("cursor:{}", path.display());
        if path.exists() && !imported(tx, &marker).await? {
            let value: Value = read_yaml(path)?;
            if let Some(items) = value.get("chat").and_then(Value::as_sequence) {
                for item in items {
                    if item.get("last_read_message_id").is_some() {
                        let chat: Chat = serde_yaml::from_value(item.clone())
                            .with_context(|| format!("invalid cursor in {}", path.display()))?;
                        merge_chat(&mut chats, chat);
                    }
                }
            }
            markers.push(marker);
        }
    }
    for path in [
        "telegram-data.yaml",
        "data.yaml",
        "config/telegram-data.yaml",
        "config/data.yaml",
        "config/telegram/data.yaml",
    ] {
        if old_telegram && path == "telegram-data.yaml" {
            continue;
        }
        if Path::new(path).exists() && !imported(tx, path).await? {
            let data: TelegramData = read_yaml(Path::new(path))?;
            for id in data.downloaded_file_ids {
                tx.execute(
                    "INSERT OR IGNORE INTO telegram_files(file_id,downloaded_at) VALUES (?,unixepoch()*1000)",
                    [id],
                )
                .await?;
            }
            for chat in data.chat {
                merge_chat(&mut chats, chat);
            }
            markers.push(path.into());
        }
    }
    for chat in chats.into_values() {
        // Preserve a newer database cursor, but merge retry IDs from newly supplied
        // data files even when the config created this chat on an earlier startup.
        tx.execute(
            "INSERT OR IGNORE INTO telegram_chats(chat_id,last_read_message_id) VALUES (?,?)",
            (chat.chat_id.clone(), chat.last_read_message_id),
        )
        .await?;
        for id in chat.ids_to_retry {
            tx.execute(
                "INSERT OR IGNORE INTO telegram_retries(chat_id,message_id) VALUES (?,?)",
                (chat.chat_id.clone(), id),
            )
            .await?;
        }
    }
    for path in [
        "sessions/download-history.json",
        "config/sessions/download-history.json",
        "config/telegram/sessions/download-history.json",
    ] {
        if Path::new(path).exists() && !imported(tx, path).await? {
            let history: Vec<Completed> = serde_json::from_slice(&fs::read(path)?)
                .with_context(|| format!("invalid history in {path}"))?;
            for item in history {
                tx.execute("INSERT INTO telegram_history(message_id,file_name,path,bytes,completed_at) SELECT ?,?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM telegram_history WHERE message_id=? AND path=? AND completed_at=?)", params![item.msg_id,item.file_name,item.path.clone(),i64::try_from(item.bytes)?,i64::try_from(item.completed_at)?,item.msg_id,item.path,i64::try_from(item.completed_at)?]).await?;
            }
            markers.push(path.into());
        }
    }
    for marker in markers {
        mark_imported(tx, &marker).await?;
    }
    Ok(())
}

fn merge_chat(chats: &mut BTreeMap<String, Chat>, chat: Chat) {
    let entry = chats.entry(chat.chat_id.clone()).or_insert_with(|| Chat {
        chat_id: chat.chat_id.clone(),
        ..Chat::default()
    });
    entry.last_read_message_id = entry.last_read_message_id.max(chat.last_read_message_id);
    entry.ids_to_retry.extend(chat.ids_to_retry);
}

#[derive(Default, Deserialize)]
struct Chat {
    chat_id: String,
    #[serde(default)]
    ids_to_retry: Vec<i32>,
    #[serde(default)]
    last_read_message_id: i32,
}
#[derive(Deserialize)]
struct TelegramData {
    #[serde(default)]
    chat: Vec<Chat>,
    #[serde(default)]
    downloaded_file_ids: Vec<String>,
}
#[derive(Deserialize)]
struct Completed {
    msg_id: i32,
    file_name: String,
    path: String,
    bytes: u64,
    completed_at: u64,
}
