//! JAV download ledger and scheduler state.
pub use super::video::{HistorySummary, Record};
pub use super::video_repository::{Provider, Repository};

pub mod cookie;

#[cfg(test)]
pub use super::video::State;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Database;

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
        let repository = Repository::load(db.clone(), Provider::Jav).await.unwrap();
        repository
            .upsert_record(record("1", "completed"))
            .await
            .unwrap();
        repository.mark_daily_run("2026-01-01").await.unwrap();
        drop(repository);
        drop(db);
        let db = Database::open(path).await.unwrap();

        let loaded = Repository::load_state(&db, Provider::Jav).await.unwrap();
        assert_eq!(loaded.records.len(), 1);
        assert_eq!(loaded.last_daily_run.as_deref(), Some("2026-01-01"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_database_loads_empty() {
        let db = Database::open(":memory:").await.unwrap();
        let state = Repository::load_state(&db, Provider::Jav).await.unwrap();
        assert!(state.records.is_empty());
    }
}
