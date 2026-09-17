//! Temporary legacy compatibility. Remove the startup and checkpoint hooks to retire it.
mod common;
mod config;
mod jav;
mod progress;
mod telegram;

use crate::storage::{Connection, Database};
pub use progress::{relocate_partial, resume_offset};
use std::path::Path;

/// Import in one transaction before either engine starts, then install configs
/// without overwriting existing files. Source files remain untouched.
pub async fn run(db: &Database) -> anyhow::Result<()> {
    let telegram = config::discover_config("telegram", crate::configuration::TELEGRAM_FILE)?;
    let jav = config::discover_config("jav", crate::configuration::JAV_FILE)?;
    let tx = db.transaction().await?;
    telegram::import(&tx, telegram.as_deref()).await?;
    jav::import(&tx).await?;
    tx.commit().await?;
    for (source, destination) in [
        (telegram, crate::configuration::TELEGRAM_FILE),
        (jav, crate::configuration::JAV_FILE),
    ] {
        if let Some(source) = source {
            config::install_config(&source, Path::new(destination))?;
        }
    }
    common::run()
}

async fn imported(conn: &Connection, source: &str) -> anyhow::Result<bool> {
    Ok(conn
        .query("SELECT 1 FROM legacy_imports WHERE source=?", [source])
        .await?
        .next()
        .await?
        .is_some())
}

async fn mark_imported(conn: &Connection, source: &str) -> anyhow::Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO legacy_imports(source) VALUES (?)",
        [source],
    )
    .await?;
    Ok(())
}
