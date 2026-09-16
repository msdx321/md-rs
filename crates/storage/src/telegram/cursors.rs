//! Explicit cursor edits survive restarts and are consumed between scans.
use crate::Database;
use std::collections::HashMap;

pub async fn load(database: &Database) -> anyhow::Result<HashMap<String, i32>> {
    let conn = database.connection().await;
    let mut rows = conn.query("SELECT chat_id,last_read_message_id FROM telegram_cursor_overrides UNION ALL SELECT chat_id,last_read_message_id FROM telegram_chats WHERE chat_id NOT IN (SELECT chat_id FROM telegram_cursor_overrides)", ()).await?;
    let mut values = HashMap::new();
    while let Some(row) = rows.next().await? {
        values.insert(row.get(0)?, row.get(1)?);
    }
    Ok(values)
}

pub async fn queue(database: &Database, updates: &[(String, i32)]) -> anyhow::Result<()> {
    let tx = database.transaction().await?;
    for (chat, cursor) in updates {
        tx.execute("INSERT INTO telegram_cursor_overrides(chat_id,last_read_message_id) VALUES (?,?) ON CONFLICT(chat_id) DO UPDATE SET last_read_message_id=excluded.last_read_message_id", (chat.as_str(), *cursor)).await?;
    }
    tx.commit().await
}

pub async fn apply(database: &Database) -> anyhow::Result<HashMap<String, i32>> {
    let tx = database.transaction().await?;
    let mut rows = tx
        .query(
            "SELECT chat_id,last_read_message_id FROM telegram_cursor_overrides",
            (),
        )
        .await?;
    let mut values = HashMap::new();
    while let Some(row) = rows.next().await? {
        let chat: String = row.get(0)?;
        let cursor: i32 = row.get(1)?;
        tx.execute("INSERT INTO telegram_chats(chat_id,last_read_message_id) VALUES (?,?) ON CONFLICT(chat_id) DO UPDATE SET last_read_message_id=excluded.last_read_message_id", (chat.as_str(), cursor)).await?;
        values.insert(chat, cursor);
    }
    tx.execute("DELETE FROM telegram_cursor_overrides", ())
        .await?;
    tx.commit().await?;
    Ok(values)
}
