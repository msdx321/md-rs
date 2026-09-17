use crate::storage::{Connection, Database};

pub fn retention_ms(days: u32) -> u64 {
    u64::from(days) * 24 * 60 * 60 * 1000
}

pub struct CompletedDownload {
    pub id: i64,
    pub msg_id: i32,
    pub file_name: String,
    pub path: String,
    pub bytes: u64,
    pub completed_at: u64,
}

impl CompletedDownload {
    pub fn key(&self) -> String {
        // SQLite may reuse row IDs after deletion; include the completion time
        // so a stale browser cannot forget a later download with the same ID.
        format!("{}:{}", self.id, self.completed_at)
    }
}

pub async fn load(db: &Database, now: u64, days: u32) -> anyhow::Result<Vec<CompletedDownload>> {
    let conn = db.connection().await;
    delete_expired(&conn, now, days).await?;
    let mut rows = conn.query("SELECT message_id,file_name,path,bytes,completed_at,id FROM telegram_history ORDER BY completed_at,id", ()).await?;
    let mut history = Vec::new();
    while let Some(row) = rows.next().await? {
        history.push(CompletedDownload {
            id: row.get(5)?,
            msg_id: row.get(0)?,
            file_name: row.get(1)?,
            path: row.get(2)?,
            bytes: u64::try_from(row.get::<i64>(3)?)?,
            completed_at: u64::try_from(row.get::<i64>(4)?)?,
        });
    }
    Ok(history)
}

pub async fn insert(conn: &Connection, item: &mut CompletedDownload) -> anyhow::Result<()> {
    let tx = conn.transaction().await?;
    tx.execute("INSERT INTO telegram_history(message_id,file_name,path,bytes,completed_at) VALUES (?,?,?,?,?)", (item.msg_id,item.file_name.clone(),item.path.clone(),i64::try_from(item.bytes)?,i64::try_from(item.completed_at)?)).await?;
    let id = tx.last_insert_rowid();
    tx.execute(
        "DELETE FROM telegram_forgotten WHERE path=?",
        [item.path.as_str()],
    )
    .await?;
    tx.commit().await?;
    item.id = id;
    Ok(())
}

/// Called within the same transaction as the history removal.
pub async fn forget(conn: &Connection, item: &CompletedDownload, now: u64) -> anyhow::Result<()> {
    conn.execute(
        "INSERT INTO telegram_forgotten(path,forgotten_at) VALUES (?,?) ON CONFLICT(path) DO UPDATE SET forgotten_at=excluded.forgotten_at",
        (item.path.as_str(), i64::try_from(now)?),
    ).await?;
    conn.execute("DELETE FROM telegram_history WHERE id=?", [item.id])
        .await?;
    Ok(())
}

pub async fn was_forgotten(conn: &Connection, path: &str) -> anyhow::Result<bool> {
    Ok(conn
        .query("SELECT 1 FROM telegram_forgotten WHERE path=?", [path])
        .await?
        .next()
        .await?
        .is_some())
}

pub async fn delete_expired(conn: &Connection, now: u64, days: u32) -> anyhow::Result<()> {
    conn.execute(
        "DELETE FROM telegram_forgotten WHERE forgotten_at<=?",
        [i64::try_from(now.saturating_sub(retention_ms(days)))?],
    )
    .await?;
    conn.execute(
        "DELETE FROM telegram_history WHERE completed_at<=?",
        [i64::try_from(now.saturating_sub(retention_ms(days)))?],
    )
    .await?;
    Ok(())
}
