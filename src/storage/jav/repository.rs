//! Serializes durable mutations and publishes cache changes only after commit.
use super::{HistorySummary, Record, State};
use crate::storage::{Connection, Database, params};
use std::sync::Mutex;

pub struct Repository {
    state: Mutex<State>,
    database: Database,
    state_write: tokio::sync::Mutex<()>,
}

impl Repository {
    pub async fn load(database: Database) -> anyhow::Result<Self> {
        let state = Self::load_state(&database).await?;
        Ok(Self {
            state: Mutex::new(state),
            database,
            state_write: tokio::sync::Mutex::new(()),
        })
    }

    #[cfg(test)]
    pub(crate) fn write_in_progress(&self) -> bool {
        self.state_write.try_lock().is_err()
    }

    pub fn history(&self, days: u32) -> Vec<Record> {
        self.state
            .lock()
            .expect("state lock poisoned")
            .history(days)
    }

    pub fn history_limited(&self, days: u32, limit: usize) -> (Vec<Record>, usize) {
        self.state
            .lock()
            .expect("state lock poisoned")
            .history_limited(days, limit)
    }

    pub fn history_summary(&self, days: u32) -> HistorySummary {
        self.state
            .lock()
            .expect("state lock poisoned")
            .history_summary(days)
    }

    pub fn is_completed(&self, id: &str) -> bool {
        self.state
            .lock()
            .expect("state lock poisoned")
            .is_completed(id)
    }

    /// Commit before updating the cache so failed writes never look durable.
    pub async fn upsert_record(&self, record: Record) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        let tx = self.database.transaction().await?;
        store_record(&tx, &record).await?;
        tx.commit().await?;
        self.state
            .lock()
            .expect("state lock poisoned")
            .upsert(record);
        Ok(())
    }

    pub async fn forget_record(&self, id: &str) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        let tx = self.database.transaction().await?;
        tx.execute("DELETE FROM jav_records WHERE id=?", [id])
            .await?;
        tx.commit().await?;
        self.state.lock().expect("state lock poisoned").forget(id);
        Ok(())
    }

    pub async fn clear_history(&self, days: u32) -> anyhow::Result<usize> {
        let _write = self.state_write.lock().await;
        let ids: std::collections::HashSet<_> = self
            .state
            .lock()
            .expect("state lock poisoned")
            .recent_records(days)
            .map(|(_, record)| record.id.clone())
            .collect();
        let tx = self.database.transaction().await?;
        let delete = tx.prepare("DELETE FROM jav_records WHERE id=?").await?;
        for id in &ids {
            delete.reset();
            delete.execute([id.as_str()]).await?;
        }
        tx.commit().await?;
        let mut state = self.state.lock().expect("state lock poisoned");
        state.records.retain(|record| !ids.contains(&record.id));
        Ok(ids.len())
    }

    pub async fn prune_history(&self, days: u32) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
        let ids: std::collections::HashSet<_> = self
            .state
            .lock()
            .expect("state lock poisoned")
            .records
            .iter()
            .filter(|record| {
                !chrono::DateTime::parse_from_rfc3339(&record.finished_at)
                    .is_ok_and(|time| time > cutoff)
            })
            .map(|record| record.id.clone())
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        // Delete by the indexed identity, not an SQL function over every timestamp.
        let tx = self.database.transaction().await?;
        let delete = tx.prepare("DELETE FROM jav_records WHERE id=?").await?;
        for id in &ids {
            delete.reset();
            delete.execute([id.as_str()]).await?;
        }
        tx.commit().await?;
        self.state
            .lock()
            .expect("state lock poisoned")
            .records
            .retain(|record| !ids.contains(&record.id));
        Ok(())
    }

    pub async fn mark_daily_run(&self, date: &str) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        self.database.connection().await.execute("INSERT INTO jav_scheduler(id,last_daily_run) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET last_daily_run=excluded.last_daily_run", [date]).await?;
        self.state
            .lock()
            .expect("state lock poisoned")
            .last_daily_run = Some(date.to_string());
        Ok(())
    }
}

impl Repository {
    async fn load_state(db: &Database) -> anyhow::Result<State> {
        let conn = db.connection().await;
        let mut state = State::default();
        let mut rows = conn.query("SELECT id,url,title,rank,status,path,size,finished_at,error FROM jav_records ORDER BY rowid", ()).await?;
        while let Some(row) = rows.next().await? {
            state.records.push(Record {
                id: row.get(0)?,
                url: row.get(1)?,
                title: row.get(2)?,
                rank: row
                    .get::<Option<i64>>(3)?
                    .map(usize::try_from)
                    .transpose()?,
                status: row.get(4)?,
                path: row.get(5)?,
                size: u64::try_from(row.get::<i64>(6)?)?,
                finished_at: row.get(7)?,
                error: row.get(8)?,
            });
        }
        if let Some(row) = conn
            .query("SELECT last_daily_run FROM jav_scheduler WHERE id=1", ())
            .await?
            .next()
            .await?
        {
            state.last_daily_run = row.get(0)?;
        }
        Ok(state)
    }
}

pub async fn store_record(conn: &Connection, record: &Record) -> anyhow::Result<()> {
    conn.execute("INSERT INTO jav_records(id,url,title,rank,status,path,size,finished_at,error) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET url=excluded.url,title=excluded.title,rank=excluded.rank,status=excluded.status,path=excluded.path,size=excluded.size,finished_at=excluded.finished_at,error=excluded.error",
        params![record.id.clone(),record.url.clone(),record.title.clone(),record.rank.map(i64::try_from).transpose()?,record.status.clone(),record.path.clone(),i64::try_from(record.size)?,record.finished_at.clone(),record.error.clone()]).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, status: &str) -> Record {
        Record {
            id: id.into(),
            url: format!("https://missav.ai/cn/{id}"),
            title: format!("video {id}"),
            rank: Some(1),
            status: status.into(),
            path: format!("/tmp/{id}.mp4"),
            size: 10,
            finished_at: "2026-01-01T00:00:00Z".into(),
            error: None,
        }
    }

    #[test]
    fn upsert_replaces_by_id() {
        let mut state = State::default();
        state.upsert(record("1", "failed"));
        state.upsert(record("1", "completed"));
        assert_eq!(state.records.len(), 1);
        assert!(state.is_completed("1"));
    }

    #[test]
    fn forget_removes_the_dedup_entry() {
        let mut state = State::default();
        state.upsert(record("1", "completed"));
        assert!(state.is_completed("1"));
        assert!(state.forget("1"));
        assert!(!state.is_completed("1"));
        assert!(!state.forget("1"));
    }

    #[test]
    fn failed_records_are_not_dedup_hits() {
        let mut state = State::default();
        state.upsert(record("9", "failed"));
        assert!(!state.is_completed("9"));
    }

    #[tokio::test]
    async fn round_trips_through_disk() {
        let dir = std::env::temp_dir().join(format!("javd-state-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.db");
        let path = path.to_str().unwrap();

        let db = Database::open(path).await.unwrap();
        let repository = Repository::load(db.clone()).await.unwrap();
        repository
            .upsert_record(record("1", "completed"))
            .await
            .unwrap();
        repository.mark_daily_run("2026-01-01").await.unwrap();
        drop(repository);
        drop(db);
        let db = Database::open(path).await.unwrap();

        let loaded = Repository::load_state(&db).await.unwrap();
        assert_eq!(loaded.records.len(), 1);
        assert_eq!(loaded.last_daily_run.as_deref(), Some("2026-01-01"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_database_loads_empty() {
        let db = Database::open(":memory:").await.unwrap();
        let state = Repository::load_state(&db).await.unwrap();
        assert!(state.records.is_empty());
    }
}
