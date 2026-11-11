//! Shared application context: configuration, HTTP client, dedup ledger,
//! live task registry and the SSE fan-out channel.

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::ph::config::{Config, FILE};
use crate::ph::source::http::{DEFAULT_USER_AGENT, Fetcher, Session, build_client};
use crate::ph::storage::{HistorySummary, Record, Repository};
use crate::runtime::download_slots::{DownloadSlot, DownloadSlots};
use crate::storage::Database;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Paused,
    Completed,
    Failed,
    Skipped,
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
            TaskState::Completed | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
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

#[derive(Default)]
struct TaskRegistry {
    tasks: HashMap<String, TaskInfo>,
    committing: HashSet<String>,
    pausing: HashSet<String>,
}

pub struct AppCtx {
    cfg: RwLock<Config>,
    common: watch::Receiver<crate::configuration::app::Config>,
    pub(crate) download_limiter: Arc<crate::runtime::download_limiter::DownloadLimiter>,
    client: wreq::Client,
    session: Session,
    pub(crate) config_update: tokio::sync::Mutex<()>,
    ledger: Repository,
    library_revision: AtomicU64,
    tasks: Mutex<TaskRegistry>,
    downloads: Arc<DownloadSlots>,
    pub(crate) jobs: crate::runtime::http_lifecycle::HttpLifecycle,
    /// Task JSON, serialized once per update for every SSE subscriber.
    events: broadcast::Sender<Arc<str>>,
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
        session.set_configured(
            cfg.request_cookie().as_deref(),
            &cfg.username,
            &cfg.password,
            &cfg.site_base,
        );
        let client = build_client()?;
        Ok(Self {
            cfg: RwLock::new(cfg),
            common,
            download_limiter,
            client,
            session,
            config_update: tokio::sync::Mutex::new(()),
            ledger,
            library_revision: AtomicU64::new(0),
            tasks: Mutex::new(TaskRegistry::default()),
            downloads: Arc::new(DownloadSlots::new()),
            jobs: Default::default(),
            events,
            scheduler: Mutex::new(SchedulerStatus::default()),
            daily_lock: tokio::sync::Mutex::new(()),
            daily_cancel: AtomicBool::new(false),
        })
    }

    // ── configuration ────────────────────────────────────────────────────

    /// Snapshot of the current configuration.
    pub fn config(&self) -> Config {
        self.config_with_paths(&self.cfg.read().expect("config lock poisoned"))
    }

    fn config_with_paths(&self, cfg: &Config) -> Config {
        let mut config = cfg.clone();
        let common = self.common.borrow();
        config.save_path = common.ph_download_path.clone();
        config.temp_path = common.temp_path.clone();
        config
    }

    /// Persist settings and re-seed the session when its credentials change.
    pub async fn update_config(&self, cfg: Config) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.jobs.is_closed(),
            "service shutting down; settings rejected"
        );
        let cfg = FILE.save_off_thread(cfg).await?;
        self.apply_config(cfg);
        Ok(())
    }

    /// Publish configuration and its cookie generation under the same lock.
    fn apply_config(&self, cfg: Config) {
        let mut current = self.cfg.write().expect("config lock poisoned");
        if cfg.cookie != current.cookie
            || cfg.site_base != current.site_base
            || cfg.username != current.username
            || cfg.password != current.password
        {
            self.session.set_configured(
                cfg.request_cookie().as_deref(),
                &cfg.username,
                &cfg.password,
                &cfg.site_base,
            );
        }
        *current = cfg;
        drop(current);
        self.downloads.notify();
    }

    /// Bind one operation's configuration and credentials atomically.
    pub fn request_context(&self) -> (Config, Fetcher) {
        let cfg = self.cfg.read().expect("config lock poisoned");
        let user_agent = if cfg.user_agent.trim().is_empty() {
            DEFAULT_USER_AGENT.to_string()
        } else {
            cfg.user_agent.clone()
        };
        let fetch = Fetcher::new(self.client.clone(), user_agent, &self.session);
        (self.config_with_paths(&cfg), fetch)
    }

    /// Cookie generations and listing warm-up state shared by all operations.
    pub fn session(&self) -> &Session {
        &self.session
    }

    // ── dedup ledger ─────────────────────────────────────────────────────

    pub fn minimum_video_resolution(&self) -> u32 {
        self.common
            .borrow()
            .minimum_video_resolution(crate::runtime::download_limiter::DownloadModule::Ph)
    }

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
            .tasks
            .retain(|_, task| {
                !task.is_terminal()
                    || chrono::DateTime::parse_from_rfc3339(&task.updated_at)
                        .is_ok_and(|time| time > cutoff)
            });
        self.library_changed();
        Ok(())
    }

    pub fn history(&self) -> Vec<Record> {
        self.ledger.history(self.history_retention_days())
    }

    pub fn history_limited(&self, limit: usize) -> (Vec<Record>, usize) {
        self.ledger
            .history_limited(self.history_retention_days(), limit)
    }

    pub fn library_revision(&self) -> u64 {
        self.library_revision.load(Ordering::Relaxed)
    }

    fn library_changed(&self) {
        self.library_revision.fetch_add(1, Ordering::Relaxed);
        self.downloads.notify();
    }

    pub fn history_summary(&self) -> HistorySummary {
        self.ledger.history_summary(self.history_retention_days())
    }

    pub fn is_completed(&self, id: &str) -> bool {
        self.ledger.is_completed(id)
    }

    pub async fn upsert_record(&self, record: Record) -> anyhow::Result<()> {
        self.ledger.upsert_record(record).await?;
        self.library_changed();
        Ok(())
    }

    pub async fn forget_record(&self, id: &str) -> anyhow::Result<()> {
        self.ledger.forget_record(id).await?;
        self.library_changed();
        Ok(())
    }

    pub async fn clear_history(&self) -> anyhow::Result<usize> {
        let removed = self
            .ledger
            .clear_history(self.history_retention_days())
            .await?;
        self.library_changed();
        Ok(removed)
    }

    pub async fn subscription(&self, source: &str) -> anyhow::Result<Option<String>> {
        self.ledger.subscription(source).await
    }

    pub async fn save_subscription(&self, source: &str, state: &str) -> anyhow::Result<()> {
        self.ledger.save_subscription(source, state).await
    }

    pub async fn mark_daily_run(&self, date: &str) -> anyhow::Result<()> {
        self.ledger.mark_daily_run(date).await?;
        self.downloads.notify();
        Ok(())
    }

    // ── task registry + events ───────────────────────────────────────────

    /// All entry points share the same limit, including manual and resumed jobs.
    pub async fn download_slot(self: &Arc<Self>, id: &str) -> Option<DownloadSlot> {
        self.downloads
            .acquire(
                Some(id),
                || {
                    (!self.jobs.is_closed() && self.task_state(id) == Some(TaskState::Queued)).then(
                        || {
                            self.cfg
                                .read()
                                .expect("config lock poisoned")
                                .concurrent_videos
                                .clamp(1, 8)
                        },
                    )
                },
                pending(),
            )
            .await
    }

    pub fn tasks(&self) -> Vec<TaskInfo> {
        self.tasks_matching(|_| true)
    }

    pub fn visible_tasks(&self) -> Vec<TaskInfo> {
        self.tasks_matching(|task| task.state != TaskState::Completed)
    }

    fn tasks_matching(&self, include: impl Fn(&TaskInfo) -> bool) -> Vec<TaskInfo> {
        let guard = self.tasks.lock().expect("task lock poisoned");
        let mut out: Vec<TaskInfo> = guard
            .tasks
            .values()
            .filter(|task| include(task))
            .cloned()
            .collect();
        out.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        out
    }

    pub fn task(&self, id: &str) -> Option<TaskInfo> {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .tasks
            .get(id)
            .cloned()
    }

    pub fn task_state(&self, id: &str) -> Option<TaskState> {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .tasks
            .get(id)
            .map(|task| task.state)
    }

    /// Pause is an unwind request before the registry publishes Paused. Read
    /// both fields under one lock without cloning URLs, titles or messages.
    pub fn task_control(&self, id: &str) -> Option<TaskState> {
        let registry = self.tasks.lock().expect("task lock poisoned");
        registry.tasks.get(id).map(|task| {
            if registry.pausing.contains(id)
                || (task.phase == "pausing" && task.state == TaskState::Running)
            {
                TaskState::Paused
            } else {
                task.state
            }
        })
    }

    pub fn running_task_count(&self) -> usize {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .tasks
            .values()
            .filter(|task| task.state == TaskState::Running)
            .count()
    }

    /// Apply `f` to a task and broadcast the result.
    pub fn update_task(&self, id: &str, f: impl FnOnce(&mut TaskInfo)) {
        let mut state_changed = false;
        let updated = {
            let mut guard = self.tasks.lock().expect("task lock poisoned");
            match guard.tasks.get_mut(id) {
                Some(task) => {
                    // A "pausing" phase is a control change too (see task_control).
                    let control = |task: &TaskInfo| (task.state, task.phase == "pausing");
                    let previous = control(task);
                    f(task);
                    state_changed = previous != control(task);
                    task.updated_at = now_rfc3339();
                    Some(task.clone())
                }
                None => None,
            }
        };
        if let Some(task) = updated {
            self.publish(&task);
        }
        if state_changed {
            self.downloads.notify();
        }
    }

    /// Register a new task. Returns false when one is already active for `id`.
    pub fn register_task(&self, info: TaskInfo) -> bool {
        let id = info.id.clone();
        self.downloads
            .while_idle(&id, || {
                let mut guard = self.tasks.lock().expect("task lock poisoned");
                if self.jobs.is_closed() {
                    return false;
                }
                if let Some(existing) = guard.tasks.get(&info.id)
                    && !existing.is_terminal()
                    && existing.state != TaskState::Paused
                {
                    return false;
                }
                self.publish(&info);
                guard.pausing.remove(&info.id);
                guard.tasks.insert(info.id.clone(), info);
                true
            })
            .unwrap_or(false)
    }

    pub fn is_downloading(&self, id: &str) -> bool {
        self.downloads.contains(id)
    }

    /// Remove only failed entries under one lock so a concurrent retry is preserved.
    pub fn clear_failed_tasks(&self) -> usize {
        let mut tasks = self.tasks.lock().expect("task lock poisoned");
        let before = tasks.tasks.len();
        tasks
            .tasks
            .retain(|_, task| task.state != TaskState::Failed);
        let removed = before - tasks.tasks.len();
        drop(tasks);
        if removed > 0 {
            self.library_changed();
        }
        removed
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<str>> {
        self.events.subscribe()
    }

    fn publish(&self, task: &TaskInfo) {
        if self.events.receiver_count() == 0 {
            return;
        }
        match serde_json::to_string(task) {
            Ok(json) => {
                let _ = self.events.send(json.into());
            }
            Err(error) => log::warn!("cannot serialize task {}: {error}", task.id),
        }
    }

    /// Wakes after every task state transition (and other slot changes), but
    /// not after progress-only updates.
    pub fn subscribe_status(&self) -> watch::Receiver<()> {
        self.downloads.subscribe()
    }

    /// Forget a finished task so it disappears from the UI.
    pub fn drop_task(&self, id: &str) -> bool {
        let removed = self
            .downloads
            .while_idle(id, || {
                let mut registry = self.tasks.lock().expect("task lock poisoned");
                if registry
                    .tasks
                    .get(id)
                    .is_none_or(|task| !task.is_terminal())
                {
                    return false;
                }
                registry.pausing.remove(id);
                registry.tasks.remove(id).is_some()
            })
            .unwrap_or(false);
        if removed {
            self.library_changed();
        }
        removed
    }

    /// Preserve the HTTP engine's Running/pausing unwind contract while serializing stops
    /// against irreversible publication and history commits.
    pub fn stop_task(&self, id: &str, cancel: bool) -> bool {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        if registry.committing.contains(id) {
            return false;
        }
        let Some(task) = registry.tasks.get_mut(id) else {
            return false;
        };
        if if cancel {
            task.is_terminal()
        } else {
            !matches!(task.state, TaskState::Running | TaskState::Queued)
        } {
            return false;
        }
        if cancel {
            task.phase = if task.state == TaskState::Queued {
                "cancelled"
            } else {
                "cancelling"
            }
            .into();
            task.state = TaskState::Cancelled;
            task.message = "cancelled by user".into();
        } else {
            if task.state == TaskState::Queued {
                task.state = TaskState::Paused;
            }
            task.phase = if task.state == TaskState::Paused {
                "paused"
            } else {
                "pausing"
            }
            .into();
            task.message = "paused by user".into();
        }
        task.updated_at = now_rfc3339();
        self.publish(task);
        if cancel {
            registry.pausing.remove(id);
        } else {
            registry.pausing.insert(id.to_string());
        }
        drop(registry);
        self.downloads.notify();
        true
    }

    pub(crate) fn begin_terminal_commit(&self, id: &str) -> bool {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        if registry.committing.contains(id) {
            return true;
        }
        if registry.pausing.contains(id)
            || registry
                .tasks
                .get(id)
                .is_none_or(|task| task.state != TaskState::Running || task.phase == "pausing")
        {
            return false;
        }
        registry.committing.insert(id.to_string());
        true
    }

    pub(crate) fn end_terminal_commit(&self, id: &str) {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .committing
            .remove(id);
    }

    pub fn begin_shutdown(&self) {
        self.jobs.close();
        self.request_daily_cancel();
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        let TaskRegistry {
            tasks,
            committing,
            pausing,
        } = &mut *registry;
        for (id, task) in tasks {
            if !committing.contains(id)
                && matches!(task.state, TaskState::Running | TaskState::Queued)
            {
                task.phase = if task.state == TaskState::Queued {
                    "paused"
                } else {
                    "pausing"
                }
                .into();
                if task.state == TaskState::Queued {
                    task.state = TaskState::Paused;
                }
                task.message = "service shutting down; partials retained".into();
                pausing.insert(id.clone());
                self.publish(task);
            }
        }
        drop(registry);
        self.downloads.notify();
    }

    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.jobs.drain().await;
    }

    // ── login state ──────────────────────────────────────────────────────

    pub fn access_configured(&self) -> bool {
        let cfg = self.cfg.read().expect("config lock poisoned");
        cfg.access_configured()
    }

    pub fn account_configured(&self) -> bool {
        let cfg = self.cfg.read().expect("config lock poisoned");
        cfg.request_cookie().is_none() && cfg.account().is_some()
    }

    pub fn session_ready(&self) -> bool {
        self.session.is_warm()
    }

    pub fn logged_in(&self) -> bool {
        self.session.is_logged_in()
    }

    pub fn login_error(&self) -> Option<String> {
        self.session.login_error()
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
        self.downloads.notify();
    }

    pub fn request_daily_cancel(&self) {
        self.daily_cancel.store(true, Ordering::SeqCst);
    }

    pub fn take_daily_cancel(&self) -> bool {
        self.daily_cancel.swap(false, Ordering::SeqCst)
    }
}

pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
