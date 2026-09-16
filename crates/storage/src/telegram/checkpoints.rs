//! Durable byte offsets for preallocated partial downloads.
use std::path::Path;

pub async fn paths(database: &crate::Database) -> anyhow::Result<Vec<std::path::PathBuf>> {
    let conn = database.connection().await;
    let mut rows = conn.query("SELECT path FROM telegram_progress", ()).await?;
    let mut paths = Vec::new();
    while let Some(row) = rows.next().await? {
        paths.push(std::path::PathBuf::from(row.get::<String>(0)?));
    }
    Ok(paths)
}

pub async fn delete(temp_path: &Path, database: &crate::Database) -> anyhow::Result<()> {
    let key = std::path::absolute(temp_path)?
        .to_string_lossy()
        .into_owned();
    database
        .connection()
        .await
        .execute("DELETE FROM telegram_progress WHERE path=?", [key])
        .await?;
    Ok(())
}

pub async fn load(conn: &crate::Connection, key: &str) -> anyhow::Result<Option<u64>> {
    let mut rows = conn
        .query(
            "SELECT downloaded_bytes FROM telegram_progress WHERE path=?",
            [key],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok(u64::try_from(row.get::<i64>(0)?)?))
        .transpose()
}

/// Commit only after the caller has synced the file data.
pub async fn write_progress(
    temp_path: &Path,
    n: u64,
    database: &crate::Database,
) -> anyhow::Result<()> {
    let key = std::path::absolute(temp_path)?
        .to_string_lossy()
        .into_owned();
    database.connection().await.execute("INSERT INTO telegram_progress(path,downloaded_bytes) VALUES (?,?) ON CONFLICT(path) DO UPDATE SET downloaded_bytes=excluded.downloaded_bytes", (key,i64::try_from(n)?)).await?;
    Ok(())
}

pub async fn clear_progress(temp_path: &Path, database: &crate::Database) -> anyhow::Result<()> {
    // Keep a zero checkpoint so a leftover legacy sidecar cannot resurrect stale progress.
    write_progress(temp_path, 0, database).await?;
    Ok(())
}
