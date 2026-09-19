//! Application persistence: shared database lifecycle and provider repositories.
pub mod jav;
pub mod p91;
pub mod telegram;
#[cfg(test)]
mod tests {
    //! Cross-provider persistence regressions using only a fresh synthetic database.
    use super::{Database, jav, p91, video::Record};

    fn record(id: &str, status: &str, finished_at: String) -> Record {
        Record {
            id: id.into(),
            url: format!("https://example.test/{id}"),
            title: id.into(),
            rank: Some(1),
            status: status.into(),
            path: format!("/synthetic/{id}.mp4"),
            size: 10,
            finished_at,
            error: None,
        }
    }

    async fn record_ids(db: &Database, query: &str) -> Vec<String> {
        let connection = db.connection().await;
        let mut rows = connection.query(query, ()).await.unwrap();
        let mut ids = Vec::new();
        while let Some(row) = rows.next().await.unwrap() {
            ids.push(row.get(0).unwrap());
        }
        ids
    }

    #[tokio::test]
    async fn clear_history_resets_each_delete_and_reload_keeps_provider_tables_isolated() {
        let dir = std::env::temp_dir().join(format!(
            "md-rs-history-clear-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("synthetic.db");
        let db = Database::open(&path).await.unwrap();
        let jav = jav::Repository::load(db.clone()).await.unwrap();
        let p91 = p91::Repository::load(db.clone()).await.unwrap();
        let now = chrono::Utc::now();
        let recent = (now - chrono::Duration::hours(1)).to_rfc3339();
        let fixtures = [
            record("recent-1", "completed", recent.clone()),
            record("recent-2", "failed", recent.clone()),
            record("recent-3", "other-status", recent.clone()),
            record(
                "old",
                "completed",
                (now - chrono::Duration::days(30)).to_rfc3339(),
            ),
            record(
                "future",
                "completed",
                (now + chrono::Duration::days(30)).to_rfc3339(),
            ),
            record("invalid", "completed", "not-a-date".into()),
        ];
        for fixture in fixtures {
            jav.upsert_record(fixture.clone()).await.unwrap();
            p91.upsert_record(fixture).await.unwrap();
        }
        jav.mark_daily_run("2026-01-01").await.unwrap();
        p91.mark_daily_run("2026-01-02").await.unwrap();
        let p91_history = serde_json::to_value(p91.history(7)).unwrap();
        assert_eq!(p91.history_summary(7).completed, 1);
        assert_eq!(p91.history_summary(7).failed, 2);

        assert_eq!(jav.clear_history(7).await.unwrap(), 3);
        assert!(jav.history(7).is_empty());
        assert!(!jav.is_completed("recent-1"));
        assert_eq!(serde_json::to_value(p91.history(7)).unwrap(), p91_history);
        drop(jav);
        drop(p91);
        drop(db);

        // New connection and caches expose missed deletes from prepared-statement reuse.
        let db = Database::open(&path).await.unwrap();
        let jav = jav::Repository::load(db.clone()).await.unwrap();
        let p91 = p91::Repository::load(db.clone()).await.unwrap();
        assert!(jav.history(7).is_empty());
        assert_eq!(serde_json::to_value(p91.history(7)).unwrap(), p91_history);
        assert_eq!(
            record_ids(&db, "SELECT id FROM jav_records ORDER BY id").await,
            ["future", "invalid", "old"]
        );
        for id in ["old", "future", "invalid"] {
            assert!(jav.is_completed(id));
            assert!(p91.is_completed(id));
        }
        // Same key in the opposite table must survive the second provider's clear.
        jav.upsert_record(record("recent-1", "completed", recent))
            .await
            .unwrap();
        let jav_history = serde_json::to_value(jav.history(7)).unwrap();
        assert_eq!(p91.clear_history(7).await.unwrap(), 3);
        assert_eq!(p91.clear_history(7).await.unwrap(), 0);
        assert!(p91.history(7).is_empty());
        assert_eq!(serde_json::to_value(jav.history(7)).unwrap(), jav_history);
        drop(jav);
        drop(p91);
        drop(db);

        let db = Database::open(&path).await.unwrap();
        let jav = jav::Repository::load(db.clone()).await.unwrap();
        let p91 = p91::Repository::load(db.clone()).await.unwrap();
        assert_eq!(serde_json::to_value(jav.history(7)).unwrap(), jav_history);
        assert!(p91.history(7).is_empty());
        assert!(!p91.is_completed("recent-1"));
        assert_eq!(
            record_ids(&db, "SELECT id FROM p91_records ORDER BY id").await,
            ["future", "invalid", "old"]
        );
        assert_eq!(
            jav.history_summary(7).last_daily_run.as_deref(),
            Some("2026-01-01")
        );
        assert_eq!(
            p91.history_summary(7).last_daily_run.as_deref(),
            Some("2026-01-02")
        );
        drop(jav);
        drop(p91);
        drop(db);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

mod video;
use std::{path::Path, sync::Arc};

use anyhow::Context;
use tokio::sync::{Mutex, MutexGuard};

pub use libsql::{Connection, params};
pub const DATABASE_FILE: &str = "media-downloader.db";

#[derive(Clone)]
pub struct Database(Arc<Mutex<Connection>>);

impl Database {
    pub async fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = libsql::Builder::new_local(path).build().await?;
        let connection = db.connect()?;
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;").await?;
        let tx = connection.transaction().await?;
        let mut rows = tx.query("PRAGMA user_version", ()).await?;
        let version: i64 = rows
            .next()
            .await?
            .context("missing schema version")?
            .get(0)?;
        anyhow::ensure!(
            version <= 6,
            "database schema version {version} is newer than this app supports"
        );
        if version == 0 {
            tx.execute_batch(include_str!("schema.sql")).await?;
            tx.execute_batch("PRAGMA user_version=1").await?;
        }
        if version < 2 {
            tx.execute_batch(include_str!("schema-v2.sql")).await?;
            tx.execute_batch("PRAGMA user_version=2").await?;
        }
        if version < 3 {
            tx.execute_batch(include_str!("schema-v3.sql")).await?;
            tx.execute_batch("PRAGMA user_version=3").await?;
        }
        if version < 4 {
            tx.execute_batch(include_str!("schema-v4.sql")).await?;
            tx.execute_batch("PRAGMA user_version=4").await?;
        }
        if version < 5 {
            tx.execute_batch(include_str!("schema-v5.sql")).await?;
            tx.execute_batch("PRAGMA user_version=5").await?;
        }
        if version < 6 {
            tx.execute_batch(include_str!("schema-v6.sql")).await?;
            tx.execute_batch("PRAGMA user_version=6").await?;
        }
        tx.commit().await?;
        Ok(Self(Arc::new(Mutex::new(connection))))
    }

    /// Keep the connection exclusively owned until commit or rollback on drop.
    pub async fn transaction(&self) -> anyhow::Result<Transaction<'_>> {
        let guard = self.connection().await;
        let inner = guard.transaction().await?;
        Ok(Transaction { inner, guard })
    }

    /// Hold this guard for the complete transaction; engines cannot interleave writes.
    pub async fn connection(&self) -> MutexGuard<'_, Connection> {
        self.0.lock().await
    }
}

/// The transaction is dropped before its connection lock is released.
pub struct Transaction<'a> {
    inner: libsql::Transaction,
    guard: MutexGuard<'a, Connection>,
}

impl Transaction<'_> {
    pub async fn commit(self) -> anyhow::Result<()> {
        let result = self.inner.commit().await;
        drop(self.guard);
        Ok(result?)
    }
}

impl std::ops::Deref for Transaction<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
