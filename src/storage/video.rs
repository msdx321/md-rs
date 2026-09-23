use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Provider-specific video ID, used as the dedup key.
    pub id: String,
    pub url: String,
    pub title: String,
    #[serde(default)]
    /// Listing position (1 = top) at the time it was taken.
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

#[derive(Debug, Default)]
pub struct HistorySummary {
    pub completed: usize,
    pub failed: usize,
    pub total_bytes: u64,
    pub last_daily_run: Option<String>,
}

impl State {
    /// True when this video has a completed download on record.
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

    /// Drop a record, allowing the video to be downloaded again.
    pub fn forget(&mut self, id: &str) -> bool {
        let before = self.records.len();
        self.records.retain(|r| r.id != id);
        self.records.len() != before
    }

    /// Share the retention window and timestamp validation across history operations.
    pub(super) fn recent_records(
        &self,
        days: u32,
    ) -> impl Iterator<Item = (chrono::DateTime<chrono::FixedOffset>, &Record)> {
        let now = chrono::Utc::now();
        let cutoff = now - chrono::Duration::days(i64::from(days));
        self.records.iter().filter_map(move |record| {
            let finished = chrono::DateTime::parse_from_rfc3339(&record.finished_at).ok()?;
            (finished > cutoff && finished <= now).then_some((finished, record))
        })
    }

    /// Aggregate status without cloning or sorting the history records.
    pub fn history_summary(&self, days: u32) -> HistorySummary {
        let mut summary = HistorySummary {
            last_daily_run: self.last_daily_run.clone(),
            ..HistorySummary::default()
        };
        for (_, record) in self.recent_records(days) {
            if record.is_completed() {
                summary.completed += 1;
                summary.total_bytes += record.size;
            } else {
                summary.failed += 1;
            }
        }
        summary
    }

    /// Time-limited history, newest first.
    pub fn history(&self, days: u32) -> Vec<Record> {
        self.history_limited(days, usize::MAX).0
    }

    /// Select only the requested newest entries before sorting and cloning.
    pub fn history_limited(&self, days: u32, limit: usize) -> (Vec<Record>, usize) {
        let mut recent: Vec<_> = self.recent_records(days).enumerate().collect();
        let count = recent.len();
        // Preserve insertion order for equal timestamps, just like the full list.
        let newest =
            |(ai, (a, _)): &(usize, (chrono::DateTime<chrono::FixedOffset>, &Record)),
             (bi, (b, _)): &(usize, (chrono::DateTime<chrono::FixedOffset>, &Record))| {
                b.cmp(a).then_with(|| ai.cmp(bi))
            };
        if limit < recent.len() {
            recent.select_nth_unstable_by(limit, newest);
            recent.truncate(limit);
        }
        recent.sort_unstable_by(newest);
        (
            recent
                .into_iter()
                .map(|(_, (_, record))| record.clone())
                .collect(),
            count,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, status: &str, finished_at: String) -> Record {
        Record {
            id: id.into(),
            url: String::new(),
            title: id.into(),
            rank: None,
            status: status.into(),
            path: String::new(),
            size: 1024,
            finished_at,
            error: None,
        }
    }

    #[test]
    fn history_preserves_retention_order_and_unknown_statuses() {
        let now = chrono::Utc::now();
        let recent = now - chrono::Duration::hours(1);
        let mut state = State::default();
        for (id, status, time) in [
            ("first", "completed", recent.to_rfc3339()),
            (
                "second",
                "custom_failure",
                recent
                    .with_timezone(&chrono::FixedOffset::east_opt(3600).unwrap())
                    .to_rfc3339(),
            ),
            (
                "old",
                "completed",
                (now - chrono::Duration::days(31)).to_rfc3339(),
            ),
            (
                "future",
                "completed",
                (now + chrono::Duration::days(1)).to_rfc3339(),
            ),
            ("invalid", "completed", "not a date".into()),
        ] {
            state.upsert(record(id, status, time));
        }
        let history = state.history(30);
        assert_eq!(
            history.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(),
            ["first", "second"]
        );
        let summary = state.history_summary(30);
        assert_eq!(
            (summary.completed, summary.failed, summary.total_bytes),
            (1, 1, 1024)
        );
        assert!(state.is_completed("old"));
        assert!(state.is_completed("invalid"));
        assert!(state.history(0).is_empty());
    }

    #[test]
    fn upsert_and_forget_keep_existing_position_and_dedup_semantics() {
        let time = (chrono::Utc::now() - chrono::Duration::minutes(1)).to_rfc3339();
        let mut state = State::default();
        state.upsert(record("one", "failed", time.clone()));
        state.upsert(record("two", "completed", time.clone()));
        state.upsert(record("one", "completed", time));
        assert_eq!(state.records.len(), 2);
        assert_eq!(state.records[0].id, "one");
        assert!(state.is_completed("one"));
        assert!(state.forget("one"));
        assert!(!state.forget("one"));
        assert!(!state.is_completed("one"));
    }

    #[test]
    fn legacy_record_defaults_and_provider_serialization_match() {
        let json = r#"{"records":[{"id":"legacy","url":"url","title":"title","status":"custom_failure","finished_at":"2026-01-01T00:00:00Z"}]}"#;
        let jav: crate::storage::jav::State = serde_json::from_str(json).unwrap();
        let p91: crate::storage::p91::State = serde_json::from_str(json).unwrap();
        assert_eq!(
            serde_json::to_value(&jav).unwrap(),
            serde_json::to_value(&p91).unwrap()
        );
        let entry = &jav.records[0];
        assert_eq!(entry.size, 0);
        assert!(entry.path.is_empty());
        assert!(entry.rank.is_none());
        assert!(entry.error.is_none());
        assert!(!entry.is_completed());
        assert!(jav.last_daily_run.is_none());
    }
}
