//! Shared application context: configuration, HTTP client, dedup ledger,
//! live task registry and the SSE fan-out channel.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::p91::config::{Config, FILE};
use crate::p91::source::http::{DEFAULT_USER_AGENT, Fetcher, Session, build_client};
use crate::p91::storage::{HistorySummary, Record, Repository};
use crate::storage::Database;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Paused,
    Completed,
    Failed,
    Cancelled,
}

/// Live state of one video download, mirrored to the UI over SSE.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub url: String,
    pub title: String,
    #[serde(default)]
    pub vid: Option<String>,
    #[serde(default)]
    pub source_url: String,
    pub state: TaskState,
    /// Human-readable stage: `resolving`, `downloading`, …
    pub phase: String,
    pub done_segments: usize,
    pub total_segments: usize,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub speed_kbps: f64,
    pub message: String,
    pub path: String,
    pub updated_at: String,
}

impl TaskInfo {
    pub fn new(id: &str, url: &str) -> Self {
        Self {
            id: id.to_string(),
            url: url.to_string(),
            title: String::new(),
            vid: None,
            source_url: String::new(),
            state: TaskState::Queued,
            phase: "queued".into(),
            done_segments: 0,
            total_segments: 0,
            downloaded_bytes: 0,
            total_bytes: 0,
            speed_kbps: 0.0,
            message: String::new(),
            path: String::new(),
            updated_at: now_rfc3339(),
        }
    }

    /// True when the task is in a state that will not progress on its own.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TaskState::Completed | TaskState::Failed | TaskState::Cancelled
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerStatus {
    pub enabled: bool,
    pub daily_time: String,
    pub next_run_at: Option<String>,
    pub last_run_at: Option<String>,
    pub last_result: String,
    pub running: bool,
}

impl Default for SchedulerStatus {
    fn default() -> Self {
        Self {
            enabled: true,
            daily_time: "03:30".into(),
            next_run_at: None,
            last_run_at: None,
            last_result: String::new(),
            running: false,
        }
    }
}

pub struct AppCtx {
    cfg: RwLock<Config>,
    common: watch::Receiver<crate::configuration::app::Config>,
    pub(crate) download_limiter: Arc<crate::runtime::download_limiter::DownloadLimiter>,
    client: wreq::Client,
    session: Session,
    pub(crate) config_update: tokio::sync::Mutex<()>,
    ledger: Repository,
    tasks: Mutex<HashMap<String, TaskInfo>>,
    downloads: Mutex<HashSet<String>>,
    events: broadcast::Sender<TaskInfo>,
    status_events: watch::Sender<()>,
    scheduler: Mutex<SchedulerStatus>,
    /// Serialises the daily job so a manual trigger cannot race the timer.
    pub daily_lock: tokio::sync::Mutex<()>,
    /// Set to ask a running daily job to stop after the current video.
    pub daily_cancel: AtomicBool,
}

impl AppCtx {
    pub async fn new(
        cfg: Config,
        database: Database,
        common: watch::Receiver<crate::configuration::app::Config>,
        download_limiter: Arc<crate::runtime::download_limiter::DownloadLimiter>,
    ) -> anyhow::Result<Self> {
        let ledger = Repository::load(database).await?;
        let (events, _) = broadcast::channel(256);
        let session = Session::new();
        session.set_configured_cookies(cfg.request_cookie().as_deref(), &cfg.site_base);
        let client = build_client(session.jar())?;
        Ok(Self {
            cfg: RwLock::new(cfg),
            common,
            download_limiter,
            client,
            session,
            config_update: tokio::sync::Mutex::new(()),
            ledger,
            tasks: Mutex::new(HashMap::new()),
            downloads: Mutex::new(HashSet::new()),
            events,
            status_events: watch::channel(()).0,
            scheduler: Mutex::new(SchedulerStatus::default()),
            daily_lock: tokio::sync::Mutex::new(()),
            daily_cancel: AtomicBool::new(false),
        })
    }

    // ── configuration ────────────────────────────────────────────────────

    /// Snapshot of the current configuration.
    pub fn config(&self) -> Config {
        let mut config = self.cfg.read().expect("config lock poisoned").clone();
        let common = self.common.borrow();
        config.save_path = common.p91_download_path.clone();
        config.temp_path = common.temp_path.clone();
        config
    }

    /// Persist settings and re-seed the session when its credentials change.
    pub async fn update_config(&self, cfg: Config) -> anyhow::Result<()> {
        let previous = self.config();
        FILE.save(&cfg)?;
        if cfg.cookie != previous.cookie || cfg.site_base != previous.site_base {
            self.session
                .set_configured_cookies(cfg.request_cookie().as_deref(), &cfg.site_base);
        }
        *self.cfg.write().expect("config lock poisoned") = cfg;
        self.status_events.send_replace(());
        Ok(())
    }

    pub fn fetcher(&self) -> Fetcher {
        let cfg = self.config();
        let user_agent = if cfg.user_agent.trim().is_empty() {
            DEFAULT_USER_AGENT.to_string()
        } else {
            cfg.user_agent.clone()
        };
        Fetcher::new(self.client.clone(), user_agent)
    }

    /// The shared cookie jar, so callers can mark the session warmed.
    pub fn session(&self) -> &Session {
        &self.session
    }

    // ── dedup ledger ─────────────────────────────────────────────────────

    pub fn history_retention_days(&self) -> u32 {
        self.common.borrow().history_retention_days
    }

    pub async fn prune_history(&self) -> anyhow::Result<()> {
        let days = self.history_retention_days();
        self.ledger.prune_history(days).await?;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
        self.tasks
            .lock()
            .expect("tasks lock poisoned")
            .retain(|_, task| {
                !task.is_terminal()
                    || chrono::DateTime::parse_from_rfc3339(&task.updated_at)
                        .is_ok_and(|time| time > cutoff)
            });
        self.status_events.send_replace(());
        Ok(())
    }

    pub fn history(&self) -> Vec<Record> {
        self.ledger.history(self.history_retention_days())
    }

    pub fn history_summary(&self) -> HistorySummary {
        self.ledger.history_summary(self.history_retention_days())
    }

    pub fn is_completed(&self, id: &str) -> bool {
        self.ledger.is_completed(id)
    }

    pub async fn upsert_record(&self, record: Record) -> anyhow::Result<()> {
        self.ledger.upsert_record(record).await?;
        self.status_events.send_replace(());
        Ok(())
    }

    pub async fn forget_record(&self, id: &str) -> anyhow::Result<()> {
        self.ledger.forget_record(id).await?;
        self.status_events.send_replace(());
        Ok(())
    }

    pub async fn clear_history(&self) -> anyhow::Result<usize> {
        let removed = self
            .ledger
            .clear_history(self.history_retention_days())
            .await?;
        self.status_events.send_replace(());
        Ok(removed)
    }

    pub async fn mark_daily_run(&self, date: &str) -> anyhow::Result<()> {
        self.ledger.mark_daily_run(date).await?;
        self.status_events.send_replace(());
        Ok(())
    }

    // ── task registry + events ───────────────────────────────────────────

    /// All entry points share the same limit, including manual and resumed jobs.
    pub async fn download_slot(self: &Arc<Self>, id: &str) -> Option<DownloadSlot> {
        let mut changes = self.subscribe_status();
        loop {
            {
                let mut downloads = self.downloads.lock().expect("downloads lock poisoned");
                if !self.task(id).is_some_and(|t| t.state == TaskState::Queued) {
                    return None;
                }
                if !downloads.contains(id)
                    && downloads.len() < self.config().concurrent_videos.clamp(1, 8)
                {
                    downloads.insert(id.to_string());
                    return Some(DownloadSlot {
                        ctx: Arc::clone(self),
                        id: id.to_string(),
                    });
                }
            }
            if changes.changed().await.is_err() {
                return None;
            }
        }
    }

    pub fn tasks(&self) -> Vec<TaskInfo> {
        let guard = self.tasks.lock().expect("task lock poisoned");
        let mut out: Vec<TaskInfo> = guard.values().cloned().collect();
        out.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        out
    }

    pub fn task(&self, id: &str) -> Option<TaskInfo> {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .get(id)
            .cloned()
    }

    /// Apply `f` to a task and broadcast the result.
    pub fn update_task(&self, id: &str, f: impl FnOnce(&mut TaskInfo)) {
        let mut state_changed = false;
        let updated = {
            let mut guard = self.tasks.lock().expect("task lock poisoned");
            match guard.get_mut(id) {
                Some(task) => {
                    let previous = task.state;
                    f(task);
                    state_changed = previous != task.state;
                    task.updated_at = now_rfc3339();
                    Some(task.clone())
                }
                None => None,
            }
        };
        if let Some(task) = updated {
            let _ = self.events.send(task);
        }
        if state_changed {
            self.status_events.send_replace(());
        }
    }

    /// Register a new task. Returns false when one is already active for `id`.
    pub fn register_task(&self, info: TaskInfo) -> bool {
        let downloads = self.downloads.lock().expect("downloads lock poisoned");
        if downloads.contains(&info.id) {
            return false;
        }
        let mut guard = self.tasks.lock().expect("task lock poisoned");
        if let Some(existing) = guard.get(&info.id)
            && !existing.is_terminal()
            && existing.state != TaskState::Paused
        {
            return false;
        }
        let _ = self.events.send(info.clone());
        guard.insert(info.id.clone(), info);
        true
    }

    pub fn is_downloading(&self, id: &str) -> bool {
        self.downloads
            .lock()
            .expect("downloads lock poisoned")
            .contains(id)
    }

    /// Remove only failed entries under one lock so a concurrent retry is preserved.
    pub fn clear_failed_tasks(&self) -> usize {
        let mut tasks = self.tasks.lock().expect("task lock poisoned");
        let before = tasks.len();
        tasks.retain(|_, task| task.state != TaskState::Failed);
        before - tasks.len()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskInfo> {
        self.events.subscribe()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<()> {
        self.status_events.subscribe()
    }

    /// Forget a finished task so it disappears from the UI.
    pub fn drop_task(&self, id: &str) -> bool {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .remove(id)
            .is_some()
    }

    // ── scheduler ────────────────────────────────────────────────────────

    pub fn scheduler_status(&self) -> SchedulerStatus {
        self.scheduler
            .lock()
            .expect("scheduler lock poisoned")
            .clone()
    }

    pub fn set_scheduler<F: FnOnce(&mut SchedulerStatus)>(&self, f: F) {
        let mut guard = self.scheduler.lock().expect("scheduler lock poisoned");
        f(&mut guard);
        drop(guard);
        self.status_events.send_replace(());
    }

    pub fn request_daily_cancel(&self) {
        self.daily_cancel.store(true, Ordering::SeqCst);
    }

    pub fn take_daily_cancel(&self) -> bool {
        self.daily_cancel.swap(false, Ordering::SeqCst)
    }
}

pub struct DownloadSlot {
    ctx: Arc<AppCtx>,
    id: String,
}

impl Drop for DownloadSlot {
    fn drop(&mut self) {
        self.ctx
            .downloads
            .lock()
            .expect("downloads lock poisoned")
            .remove(&self.id);
        self.ctx.status_events.send_replace(());
    }
}

pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> AppCtx {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let db = Database::open(":memory:").await.unwrap();
            AppCtx::new(
                Config::default(),
                db,
                watch::channel(crate::configuration::app::Config::default()).1,
                Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                    watch::channel(crate::configuration::app::Config::default()).1,
                )),
            )
            .await
            .expect("context builds")
        })
    }

    #[test]
    fn refuses_duplicate_running_tasks() {
        let ctx = ctx();
        let mut info = TaskInfo::new("1", "u");
        info.state = TaskState::Running;
        assert!(ctx.register_task(info));
        assert!(!ctx.register_task(TaskInfo::new("1", "u")));
    }

    #[test]
    fn allows_retry_after_terminal_state() {
        let ctx = ctx();
        let mut info = TaskInfo::new("1", "u");
        info.state = TaskState::Failed;
        assert!(ctx.register_task(info));
        assert!(ctx.register_task(TaskInfo::new("1", "u")));
    }

    #[test]
    fn daily_cancel_flag_is_one_shot() {
        let ctx = ctx();
        assert!(!ctx.take_daily_cancel());
        ctx.request_daily_cancel();
        assert!(ctx.take_daily_cancel());
        assert!(!ctx.take_daily_cancel());
    }
}
