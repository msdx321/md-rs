use crate::storage::Connection;
use std::{
    fs,
    path::{Path, PathBuf},
};

/// Compatibility hook used only when a download has no database checkpoint.
/// Caller holds the shared connection lock, so import and subsequent reads serialize.
pub async fn resume_offset(
    temp_path: &Path,
    database: &crate::storage::Database,
) -> anyhow::Result<u64> {
    let key = std::path::absolute(temp_path)?
        .to_string_lossy()
        .into_owned();
    let conn = database.connection().await;
    if let Some(offset) = crate::storage::telegram::checkpoints::load(&conn, &key).await? {
        return Ok(offset);
    }
    import_progress(&conn, temp_path, &key).await
}

async fn import_progress(conn: &Connection, temp_path: &Path, key: &str) -> anyhow::Result<u64> {
    let mut path = temp_path.as_os_str().to_owned();
    path.push(".progress");
    let n = match fs::read(PathBuf::from(path)) {
        Ok(bytes) => String::from_utf8_lossy(&bytes)
            .trim()
            .parse::<u64>()
            .unwrap_or(0),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
        Err(error) => return Err(error.into()),
    };
    conn.execute(
        "INSERT INTO telegram_progress(path,downloaded_bytes) VALUES (?,?)",
        (key, i64::try_from(n)?),
    )
    .await?;
    Ok(n)
}

/// Move the old adjacent partial file and its resume checkpoint into shared temp storage.
pub async fn relocate_partial(
    final_path: &Path,
    temp_path: &Path,
    database: &crate::storage::Database,
) -> anyhow::Result<()> {
    let mut old = final_path.as_os_str().to_owned();
    old.push(".part");
    let old = PathBuf::from(old);
    if old == temp_path || temp_path.exists() || !old.exists() {
        return Ok(());
    }
    let offset = resume_offset(&old, database).await?;
    // Keep the original until both the new file and checkpoint are durable.
    fs::copy(&old, temp_path)?;
    fs::File::open(temp_path)?.sync_all()?;
    crate::storage::telegram::checkpoints::write_progress(temp_path, offset, database).await?;
    fs::remove_file(&old)?;
    crate::storage::telegram::checkpoints::delete(&old, database).await?;
    let mut sidecar = old.as_os_str().to_owned();
    sidecar.push(".progress");
    match fs::remove_file(sidecar) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}
