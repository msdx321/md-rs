//! Serializes durable mutations and publishes cache changes only after commit.
use super::video::{HistorySummary, Record, State};
use crate::storage::{Database, params};
use std::collections::HashSet;
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

#[derive(Clone, Copy)]
pub enum Provider {
    Jav,
    P91,
    Ph,
}

impl Provider {
    fn prefix(self) -> &'static str {
        match self {
            Self::Jav => "jav",
            Self::P91 => "p91",
            Self::Ph => "ph",
        }
    }
}

struct Cache {
    completed: HashSet<String>,
    state: State,
    summary: Option<CachedSummary>,
}

pub struct Repository {
    cache: Mutex<Cache>,
    pub(super) database: Database,
    provider: Provider,
    state_write: tokio::sync::Mutex<()>,
}

impl Repository {
    pub async fn load(database: Database, provider: Provider) -> anyhow::Result<Self> {
        let state = Self::load_state(&database, provider).await?;
        Ok(Self {
            cache: Mutex::new(Cache {
                completed: state
                    .records
                    .iter()
                    .filter(|r| r.is_completed())
                    .map(|r| r.id.clone())
                    .collect(),
                state,
                summary: None,
            }),
            database,
            provider,
            state_write: tokio::sync::Mutex::new(()),
        })
    }

    #[cfg(test)]
    pub(crate) fn write_in_progress(&self) -> bool {
        self.state_write.try_lock().is_err()
    }

    pub fn history(&self, days: u32) -> Vec<Record> {
        self.cache
            .lock()
            .expect("cache lock poisoned")
            .state
            .history(days)
    }

    pub fn history_limited(&self, days: u32, limit: usize) -> (Vec<Record>, usize) {
        self.cache
            .lock()
            .expect("cache lock poisoned")
            .state
            .history_limited(days, limit)
    }

    pub fn history_summary(&self, days: u32) -> HistorySummary {
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        if let Some(entry) = &cache.summary
            && entry.days == days
            && entry.computed_at.elapsed() < SUMMARY_TTL
        {
            return entry.summary.clone();
        }
        let summary = cache.state.history_summary(days);
        cache.summary = Some(CachedSummary {
            days,
            computed_at: Instant::now(),
            summary: summary.clone(),
        });
        summary
    }

    pub fn is_completed(&self, id: &str) -> bool {
        self.cache
            .lock()
            .expect("cache lock poisoned")
            .completed
            .contains(id)
    }

    /// Commit before updating the cache so failed writes never look durable.
    pub async fn upsert_record(&self, record: Record) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        let tx = self.database.transaction().await?;
        let sql = format!(
            "INSERT INTO {}_records(id,url,title,rank,status,path,size,finished_at,error)
             VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
             url=excluded.url,title=excluded.title,rank=excluded.rank,status=excluded.status,
             path=excluded.path,size=excluded.size,finished_at=excluded.finished_at,error=excluded.error",
            self.provider.prefix()
        );
        tx.execute(
            &sql,
            params![
                record.id.as_str(),
                record.url.as_str(),
                record.title.as_str(),
                record.rank.map(i64::try_from).transpose()?,
                record.status.as_str(),
                record.path.as_str(),
                i64::try_from(record.size)?,
                record.finished_at.as_str(),
                record.error.clone(),
            ],
        )
        .await?;
        tx.commit().await?;
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        if record.is_completed() {
            cache.completed.insert(record.id.clone());
        } else {
            cache.completed.remove(&record.id);
        }
        cache.state.upsert(record);
        cache.summary = None;
        Ok(())
    }

    pub async fn forget_record(&self, id: &str) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        let tx = self.database.transaction().await?;
        tx.execute(
            &format!("DELETE FROM {}_records WHERE id=?", self.provider.prefix()),
            [id],
        )
        .await?;
        tx.commit().await?;
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        cache.completed.remove(id);
        cache.state.forget(id);
        cache.summary = None;
        Ok(())
    }

    pub async fn clear_history(&self, days: u32) -> anyhow::Result<usize> {
        let _write = self.state_write.lock().await;
        let ids: HashSet<_> = self
            .cache
            .lock()
            .expect("cache lock poisoned")
            .state
            .recent_records(days)
            .map(|(_, record)| record.id.clone())
            .collect();
        self.delete_records(&ids).await?;
        Ok(ids.len())
    }

    pub async fn prune_history(&self, days: u32) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
        let ids: HashSet<_> = self
            .cache
            .lock()
            .expect("cache lock poisoned")
            .state
            .records
            .iter()
            .filter(|record| {
                !chrono::DateTime::parse_from_rfc3339(&record.finished_at)
                    .is_ok_and(|time| time > cutoff)
            })
            .map(|record| record.id.clone())
            .collect();
        self.delete_records(&ids).await
    }

    async fn delete_records(&self, ids: &HashSet<String>) -> anyhow::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        // Delete by the indexed identity, not an SQL function over every timestamp.
        let tx = self.database.transaction().await?;
        let delete = tx
            .prepare(&format!(
                "DELETE FROM {}_records WHERE id=?",
                self.provider.prefix()
            ))
            .await?;
        for id in ids {
            delete.reset();
            delete.execute([id.as_str()]).await?;
        }
        tx.commit().await?;
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        for id in ids {
            cache.completed.remove(id);
        }
        cache
            .state
            .records
            .retain(|record| !ids.contains(&record.id));
        cache.summary = None;
        Ok(())
    }

    pub async fn mark_daily_run(&self, date: &str) -> anyhow::Result<()> {
        let _write = self.state_write.lock().await;
        self.database.connection().await.execute(&format!("INSERT INTO {}_scheduler(id,last_daily_run) VALUES (1,?) ON CONFLICT(id) DO UPDATE SET last_daily_run=excluded.last_daily_run", self.provider.prefix()), [date]).await?;
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        cache.state.last_daily_run = Some(date.to_string());
        cache.summary = None;
        Ok(())
    }
}

impl Repository {
    pub(super) async fn load_state(db: &Database, provider: Provider) -> anyhow::Result<State> {
        let conn = db.connection().await;
        let mut state = State::default();
        let mut rows = conn.query(&format!("SELECT id,url,title,rank,status,path,size,finished_at,error FROM {}_records ORDER BY rowid", provider.prefix()), ()).await?;
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
            .query(
                &format!(
                    "SELECT last_daily_run FROM {}_scheduler WHERE id=1",
                    provider.prefix()
                ),
                (),
            )
            .await?
            .next()
            .await?
        {
            state.last_daily_run = row.get(0)?;
        }
        Ok(state)
    }
}
