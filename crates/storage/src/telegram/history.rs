use crate::{Connection, Database};

pub fn retention_ms(days: u32) -> u64 {
    u64::from(days) * 24 * 60 * 60 * 1000
}

pub struct CompletedDownload {
    pub msg_id: i32,
    pub file_name: String,
    pub path: String,
    pub bytes: u64,
    pub completed_at: u64,
}

pub async fn load(db: &Database, now: u64, days: u32) -> anyhow::Result<Vec<CompletedDownload>> {
    let conn = db.connection().await;
    delete_expired(&conn, now, days).await?;
    let mut rows = conn.query("SELECT message_id,file_name,path,bytes,completed_at FROM telegram_history ORDER BY completed_at,id", ()).await?;
    let mut history = Vec::new();
    while let Some(row) = rows.next().await? {
        history.push(CompletedDownload {
            msg_id: row.get(0)?,
            file_name: row.get(1)?,
            path: row.get(2)?,
            bytes: u64::try_from(row.get::<i64>(3)?)?,
            completed_at: u64::try_from(row.get::<i64>(4)?)?,
        });
    }
    Ok(history)
}

pub async fn insert(conn: &Connection, item: &CompletedDownload) -> anyhow::Result<()> {
    conn.execute("INSERT INTO telegram_history(message_id,file_name,path,bytes,completed_at) VALUES (?,?,?,?,?)", (item.msg_id,item.file_name.clone(),item.path.clone(),i64::try_from(item.bytes)?,i64::try_from(item.completed_at)?)).await?;
    Ok(())
}

pub async fn delete_expired(conn: &Connection, now: u64, days: u32) -> anyhow::Result<()> {
    conn.execute(
        "DELETE FROM telegram_history WHERE completed_at<=?",
        [i64::try_from(now.saturating_sub(retention_ms(days)))?],
    )
    .await?;
    Ok(())
}
