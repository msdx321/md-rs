use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Video slug (`fc2-ppv-4968310`) — the dedup key.
    pub id: String,
    pub url: String,
    pub title: String,
    #[serde(default)]
    /// Listing rank (1 = top of the requested sort) at the time it was taken.
    pub rank: Option<usize>,
    /// `completed` or `failed`.
    pub status: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub size: u64,
    pub finished_at: String,
    #[serde(default)]
    pub error: Option<String>,
}

impl Record {
    pub fn is_completed(&self) -> bool {
        self.status == "completed"
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct State {
    #[serde(default)]
    pub records: Vec<Record>,
    /// Date (`YYYY-MM-DD`) of the last successful daily run.
    #[serde(default)]
    pub last_daily_run: Option<String>,
}

impl State {
    /// True when this post has a completed download on record.
    pub fn is_completed(&self, id: &str) -> bool {
        self.records.iter().any(|r| r.id == id && r.is_completed())
    }

    /// Insert or replace the record for `record.id`.
    pub fn upsert(&mut self, record: Record) {
        match self.records.iter_mut().find(|r| r.id == record.id) {
            Some(existing) => *existing = record,
            None => self.records.push(record),
        }
    }

    /// Drop a record, allowing the post to be downloaded again.
    pub fn forget(&mut self, id: &str) -> bool {
        let before = self.records.len();
        self.records.retain(|r| r.id != id);
        self.records.len() != before
    }

    /// Time-limited history, newest first.
    pub fn history(&self, days: u32) -> Vec<Record> {
        let now = chrono::Utc::now();
        let cutoff = now - chrono::Duration::days(i64::from(days));
        let mut recent: Vec<_> = self
            .records
            .iter()
            .filter_map(|record| {
                let finished = chrono::DateTime::parse_from_rfc3339(&record.finished_at).ok()?;
                (finished > cutoff && finished <= now).then_some((finished, record))
            })
            .collect();
        recent.sort_by(|(a, _), (b, _)| b.cmp(a));
        recent
            .into_iter()
            .map(|(_, record)| record.clone())
            .collect()
    }
}
