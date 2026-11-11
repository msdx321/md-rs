//! Serializes durable mutations and publishes cache changes only after commit.
use super::{HistorySummary, Record, State};
use crate::storage::{Connection, Database, params};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Status snapshots are rebuilt on every task transition, for every connected
/// client, and each rebuild parses a timestamp per retained record. Reusing a
/// recent result removes that scan; the retention cutoff moves with wall-clock
/// time, which this window keeps invisible.
const SUMMARY_TTL: Duration = Duration::from_secs(1);

struct CachedSummary {
    days: u32,
    computed_at: Instant,
    summary: HistorySummary,
}

pub struct Repository {
    state: Mutex<State>,
    /// Dropped by every durable write, so it can only be stale by `SUMMARY_TTL`.
    summary: Mutex<Option<CachedSummary>>,
    database: Database,
    state_write: tokio::sync::Mutex<()>,
}

impl Repository {
    pub async fn load(database: Database) -> anyhow::Result<Self> {
        let state = Self::load_state(&database).await?;
        Ok(Self {
            state: Mutex::new(state),
            summary: Mutex::new(None),
            database,
            state_write: tokio::sync::Mutex::new(()),
        })
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
        {
            let cached = self.summary.lock().expect("summary lock poisoned");
            if let Some(entry) = cached.as_ref()
                && entry.days == days
                && entry.computed_at.elapsed() < SUMMARY_TTL
            {
                return entry.summary.clone();
            }
        }
        let summary = self
            .state
            .lock()
            .expect("state lock poisoned")
            .history_summary(days);
        *self.summary.lock().expect("summary lock poisoned") = Some(CachedSummary {
            days,
            computed_at: Instant::now(),
            summary: summary.clone(),
        });
        summary
    }

    fn invalidate_summary(&self) {
        *self.summary.lock().expect("summary lock poisoned") = None;
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
        self.invalidate_summary();
        Ok(())
    }

    pub async fn forget_record(&self, id: &str) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        let tx = self.database.transaction().await?;
        tx.execute("DELETE FROM ph_records WHERE id=?", [id])
            .await?;
        tx.commit().await?;
        self.state.lock().expect("state lock poisoned").forget(id);
        self.invalidate_summary();
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
        let delete = tx.prepare("DELETE FROM ph_records WHERE id=?").await?;
        for id in &ids {
            delete.reset();
            delete.execute([id.as_str()]).await?;
        }
        tx.commit().await?;
        let mut state = self.state.lock().expect("state lock poisoned");
        state.records.retain(|record| !ids.contains(&record.id));
        drop(state);
        self.invalidate_summary();
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
        let delete = tx.prepare("DELETE FROM ph_records WHERE id=?").await?;
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
        self.invalidate_summary();
        Ok(())
    }

    pub async fn subscription(&self, source: &str) -> anyhow::Result<Option<String>> {
        let conn = self.database.connection().await;
        let mut rows = conn
            .query(
                "SELECT state FROM ph_subscriptions WHERE source=?",
                [source],
            )
            .await?;
        Ok(rows.next().await?.map(|row| row.get(0)).transpose()?)
    }

    pub async fn save_subscription(&self, source: &str, state: &str) -> anyhow::Result<()> {
        self.database.connection().await.execute(
            "INSERT INTO ph_subscriptions(source,state) VALUES (?,?) ON CONFLICT(source) DO UPDATE SET state=excluded.state",
            params![source, state],
        ).await?;
        Ok(())
    }

    pub async fn mark_daily_run(&self, date: &str) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        self.database.connection().await.execute("INSERT INTO ph_scheduler(id,last_daily_run) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET last_daily_run=excluded.last_daily_run", [date]).await?;
        self.state
            .lock()
            .expect("state lock poisoned")
            .last_daily_run = Some(date.to_string());
        self.invalidate_summary();
        Ok(())
    }
}

impl Repository {
    async fn load_state(db: &Database) -> anyhow::Result<State> {
        let conn = db.connection().await;
        let mut state = State::default();
        let mut rows = conn.query("SELECT id,url,title,rank,status,path,size,finished_at,error FROM ph_records ORDER BY rowid", ()).await?;
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
            .query("SELECT last_daily_run FROM ph_scheduler WHERE id=1", ())
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
    conn.execute("INSERT INTO ph_records(id,url,title,rank,status,path,size,finished_at,error) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET url=excluded.url,title=excluded.title,rank=excluded.rank,status=excluded.status,path=excluded.path,size=excluded.size,finished_at=excluded.finished_at,error=excluded.error",
        params![record.id.clone(),record.url.clone(),record.title.clone(),record.rank.map(i64::try_from).transpose()?,record.status.clone(),record.path.clone(),i64::try_from(record.size)?,record.finished_at.clone(),record.error.clone()]).await?;
    Ok(())
}
