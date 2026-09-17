//! Application persistence: shared database lifecycle and provider repositories.
pub mod jav;
pub mod telegram;
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
            version <= 5,
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
