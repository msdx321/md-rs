//! Pornhub download ledger and scheduler state.
pub use super::video::{HistorySummary, Record};
pub use super::video_repository::{Provider, Repository};

use crate::storage::params;

pub async fn subscription(ledger: &Repository, source: &str) -> anyhow::Result<Option<String>> {
    let conn = ledger.database.connection().await;
    let mut rows = conn
        .query(
            "SELECT state FROM ph_subscriptions WHERE source=?",
            [source],
        )
        .await?;
    Ok(rows.next().await?.map(|row| row.get(0)).transpose()?)
}

pub async fn save_subscription(
    ledger: &Repository,
    source: &str,
    state: &str,
) -> anyhow::Result<()> {
    ledger.database.connection().await.execute(
            "INSERT INTO ph_subscriptions(source,state) VALUES (?,?) ON CONFLICT(source) DO UPDATE SET state=excluded.state",
            params![source, state],
        ).await?;
    Ok(())
}
