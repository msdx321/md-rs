use super::{AppData, ChatData};
use crate::Database;

pub async fn load(db: &Database) -> anyhow::Result<AppData> {
    let conn = db.connection().await;
    let mut data = AppData::default();
    let mut rows = conn.query("SELECT file_id FROM telegram_files", ()).await?;
    while let Some(row) = rows.next().await? {
        data.downloaded_file_ids.push(row.get(0)?);
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

pub async fn save(db: &Database, data: &AppData) -> anyhow::Result<()> {
    let tx = db.transaction().await?;
    tx.execute("DELETE FROM telegram_files", ()).await?;
    tx.execute("DELETE FROM telegram_retries", ()).await?;
    for id in &data.downloaded_file_ids {
        tx.execute(
            "INSERT INTO telegram_files(file_id) VALUES (?)",
            [id.as_str()],
        )
        .await?;
    }
    for chat in &data.chat {
        tx.execute("INSERT INTO telegram_chats(chat_id,last_read_message_id) VALUES (?,?) ON CONFLICT(chat_id) DO UPDATE SET last_read_message_id=excluded.last_read_message_id", (chat.chat_id.clone(),chat.last_read_message_id)).await?;
        for id in &chat.ids_to_retry {
            tx.execute(
                "INSERT INTO telegram_retries(chat_id,message_id) VALUES (?,?)",
                (chat.chat_id.clone(), *id),
            )
            .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}
