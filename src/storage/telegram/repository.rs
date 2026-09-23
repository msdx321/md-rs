use super::{AppData, ChatData};
use crate::storage::Database;

pub async fn load(db: &Database) -> anyhow::Result<AppData> {
    let conn = db.connection().await;
    let mut data = AppData::default();
    let mut rows = conn
        .query("SELECT file_id,downloaded_at FROM telegram_files", ())
        .await?;
    while let Some(row) = rows.next().await? {
        data.downloaded_file_ids
            .push((row.get(0)?, u64::try_from(row.get::<i64>(1)?)?));
    }
    let mut rows = conn
        .query(
            "SELECT chat_id,last_read_message_id FROM telegram_chats",
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let mut chat = ChatData {
            chat_id: row.get(0)?,
            last_read_message_id: row.get(1)?,
            ids_to_retry: Vec::new(),
        };
        let mut retries = conn
            .query(
                "SELECT message_id FROM telegram_retries WHERE chat_id=?",
                [chat.chat_id.clone()],
            )
            .await?;
        while let Some(row) = retries.next().await? {
            chat.ids_to_retry.push(row.get(0)?);
        }
        data.chat.push(chat);
    }
    Ok(data)
}

/// Commit scan cursors and retry sets; file IDs are recorded as they complete.
pub async fn save(db: &Database, chats: &[ChatData]) -> anyhow::Result<()> {
    let tx = db.transaction().await?;
    tx.execute("DELETE FROM telegram_retries", ()).await?;
    let upsert_chat = tx.prepare("INSERT INTO telegram_chats(chat_id,last_read_message_id) VALUES (?,?) ON CONFLICT(chat_id) DO UPDATE SET last_read_message_id=excluded.last_read_message_id").await?;
    let insert_retry = tx
        .prepare("INSERT INTO telegram_retries(chat_id,message_id) VALUES (?,?)")
        .await?;
    // Local libsql statements must be reset before binding the next row.
    for chat in chats {
        upsert_chat.reset();
        upsert_chat
            .execute((chat.chat_id.as_str(), chat.last_read_message_id))
            .await?;
        for id in &chat.ids_to_retry {
            insert_retry.reset();
            insert_retry.execute((chat.chat_id.as_str(), *id)).await?;
        }
    }
    tx.commit().await?;
    Ok(())
}

/// Record one completed file ID, removing the entry it evicted from the cache.
pub async fn record_file_id(
    db: &Database,
    id: &str,
    downloaded_at: u64,
    evicted: Option<&str>,
) -> anyhow::Result<()> {
    let tx = db.transaction().await?;
    if let Some(evicted) = evicted {
        tx.execute("DELETE FROM telegram_files WHERE file_id=?", [evicted])
            .await?;
    }
    tx.execute(
        "INSERT INTO telegram_files(file_id,downloaded_at) VALUES (?,?) ON CONFLICT(file_id) DO UPDATE SET downloaded_at=excluded.downloaded_at",
        (id, i64::try_from(downloaded_at)?),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

pub async fn forget_file_id(db: &Database, id: &str) -> anyhow::Result<()> {
    db.connection()
        .await
        .execute("DELETE FROM telegram_files WHERE file_id=?", [id])
        .await?;
    Ok(())
}
