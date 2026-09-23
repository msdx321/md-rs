//! Shared application context: configuration, HTTP client, dedup ledger,
//! live task registry and the SSE fan-out channel.

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::p91::config::{Config, FILE};
use crate::p91::source::http::{DEFAULT_USER_AGENT, Fetcher, Session, build_client};
use crate::p91::storage::{HistorySummary, Record, Repository};
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
        session.set_configured_cookies(cfg.request_cookie().as_deref(), &cfg.site_base);
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
        config.save_path = common.p91_download_path.clone();
        config.temp_path = common.temp_path.clone();
        config
    }

    /// Persist settings and re-seed the session when its credentials change.
    pub async fn update_config(&self, cfg: Config) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.jobs.is_closed(),
            "service shutting down; settings rejected"
        );
        FILE.save(&cfg)?;
        self.apply_config(cfg);
        Ok(())
    }

    /// Publish configuration and its cookie generation under the same lock.
    fn apply_config(&self, cfg: Config) {
        let mut current = self.cfg.write().expect("config lock poisoned");
        if cfg.cookie != current.cookie || cfg.site_base != current.site_base {
            self.session
                .set_configured_cookies(cfg.request_cookie().as_deref(), &cfg.site_base);
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

    #[cfg(test)]
    pub(crate) fn history_write_in_progress(&self) -> bool {
        self.ledger.write_in_progress()
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

    /// Preserve p91's Running/pausing unwind contract while serializing stops
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
    fn slots_observe_live_capacity_and_queued_pause() {
        let ctx = Arc::new(ctx());
        ctx.cfg.write().unwrap().concurrent_videos = 1;
        for id in ["1", "2", "3"] {
            assert!(ctx.register_task(TaskInfo::new(id, "u")));
        }
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let first = ctx.download_slot("1").await.unwrap();
            let second = ctx.download_slot("2");
            tokio::pin!(second);
            assert!(futures_util::poll!(&mut second).is_pending());
            ctx.cfg.write().unwrap().concurrent_videos = 2;
            ctx.downloads.notify();
            let second = second.await.unwrap();
            let third = ctx.download_slot("3");
            tokio::pin!(third);
            assert!(futures_util::poll!(&mut third).is_pending());
            ctx.update_task("3", |task| task.state = TaskState::Paused);
            assert!(third.await.is_none());
            drop(first);
            drop(second);
        });
    }

    #[test]
    fn registration_stays_blocked_until_paused_owner_releases() {
        let ctx = Arc::new(ctx());
        assert!(ctx.register_task(TaskInfo::new("1", "u")));
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let owner = ctx.download_slot("1").await.unwrap();
            ctx.update_task("1", |task| task.state = TaskState::Paused);
            assert!(ctx.is_downloading("1"));
            assert!(!ctx.register_task(TaskInfo::new("1", "u")));
            let mut status = ctx.subscribe_status();
            drop(owner);
            assert!(status.has_changed().unwrap());
            status.borrow_and_update();
            assert!(!ctx.is_downloading("1"));
            assert!(ctx.register_task(TaskInfo::new("1", "u")));
            let resumed = ctx.download_slot("1").await.unwrap();
            drop(resumed);
            assert!(status.has_changed().unwrap());
        });
    }

    #[test]
    fn copy_control_preserves_pausing_phase_and_running_count() {
        let ctx = ctx();
        assert!(ctx.register_task(TaskInfo::new("1", "u")));
        assert_eq!(ctx.task_state("missing"), None);
        assert_eq!(ctx.task_control("missing"), None);
        ctx.update_task("1", |task| {
            task.state = TaskState::Running;
            task.phase = "pausing".into();
        });
        assert_eq!(ctx.task_state("1"), Some(TaskState::Running));
        assert_eq!(ctx.task_control("1"), Some(TaskState::Paused));
        assert_eq!(ctx.running_task_count(), 1);
        ctx.update_task("1", |task| task.state = TaskState::Cancelled);
        assert_eq!(ctx.task_control("1"), Some(TaskState::Cancelled));
        assert_eq!(ctx.running_task_count(), 0);
    }

    #[test]
    fn daily_cancel_flag_is_one_shot() {
        let ctx = ctx();
        assert!(!ctx.take_daily_cancel());
        ctx.request_daily_cancel();
        assert!(ctx.take_daily_cancel());
        assert!(!ctx.take_daily_cancel());
    }
    async fn shutdown_fixture(root: &std::path::Path, base: &str) -> Arc<AppCtx> {
        let common = watch::channel(crate::configuration::app::Config {
            p91_download_path: root.join("output"),
            temp_path: root.join("partial"),
            ..Default::default()
        })
        .1;
        let mut ctx = AppCtx::new(
            Config {
                site_base: base.into(),
                concurrent_videos: 1,
                ..Default::default()
            },
            Database::open(":memory:").await.unwrap(),
            common.clone(),
            Arc::new(crate::runtime::download_limiter::DownloadLimiter::new(
                common,
            )),
        )
        .await
        .unwrap();
        ctx.client = wreq::Client::builder().no_proxy().build().unwrap();
        Arc::new(ctx)
    }

    #[tokio::test]
    async fn daily_reconfiguration_rejects_old_next_page_after_held_child() {
        use crate::test_support::http::{Server, response};
        use std::time::Duration;

        let root = tempfile::tempdir().unwrap();
        let mut old_site = Server::new().await;
        let mut new_site = Server::new().await;
        // Listing identity validation requires a 91porn.com host. Resolve both
        // fixture origins explicitly to loopback; never contact the provider.
        let old_url = old_site.url.replace("127.0.0.1", "old.91porn.com");
        let new_url = new_site.url.replace("127.0.0.1", "new.91porn.com");
        let mut ctx = shutdown_fixture(root.path(), &old_url).await;
        Arc::get_mut(&mut ctx).unwrap().client = wreq::Client::builder()
            .no_proxy()
            .resolve(
                "old.91porn.com",
                old_site
                    .url
                    .strip_prefix("http://")
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
            .resolve(
                "new.91porn.com",
                new_site
                    .url
                    .strip_prefix("http://")
                    .unwrap()
                    .parse()
                    .unwrap(),
            )
            .build()
            .unwrap();
        ctx.apply_config(Config {
            top_n: 1,
            max_pages: 2,
            prefer_hd: false,
            ..ctx.config()
        });
        let (old_cfg, old_fetch) = ctx.request_context();
        let run_ctx = ctx.clone();
        let daily = tokio::spawn(async move {
            crate::p91::scheduler::run_daily(run_ctx, "generation fixture").await
        });
        let listing = old_site.next().await;
        assert!(listing.head.starts_with("GET /v.php?"));
        let html = format!(
            "<div class='well well-sm videos-text-align'><a href='{}/view_video.php?viewkey=fixture'><div class='thumb-overlay' id='playvthumb_123'><img src='{}/thumb/123.jpg'></div><span class='video-title'>fixture</span></a></div>",
            old_url, old_url
        );
        drop(listing.respond(response(
            "200 OK",
            "Content-Type: text/html\r\n",
            html.as_bytes(),
        )));
        // The first daily child is genuinely resolving a video. Its failure
        // leaves quota to fill, so the old queue must try its next-page path.
        let held_child = old_site.next().await;
        assert!(
            held_child
                .head
                .starts_with("GET /view_video.php?viewkey=fixture ")
        );
        ctx.apply_config(Config {
            site_base: new_url.clone(),
            cookie: "account=new".into(),
            ..ctx.config()
        });
        assert!(!ctx.session().is_warm());
        let (new_cfg, new_fetch) = ctx.request_context();
        assert_eq!(old_cfg.site_base, old_url);
        assert_eq!(new_cfg.site_base, new_url);
        assert!(old_fetch.check_current().is_err());
        assert!(new_fetch.check_current().is_ok());
        drop(held_child.respond(response(
            "500 Internal Server Error",
            "",
            b"fixture failure",
        )));
        let report = tokio::time::timeout(Duration::from_secs(5), daily)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.failed, 1);
        assert!(
            ctx.scheduler_status()
                .last_result
                .contains("91Porn session changed")
        );
        assert!(
            !ctx.session().is_warm(),
            "an old daily queue must not warm the new site"
        );

        let session = ctx.session().clone();
        let warm = tokio::spawn(async move {
            session
                .ensure(&new_cfg.popular_url(1), &new_cfg.site_base, &new_fetch)
                .await
        });
        let listing = new_site.next().await;
        assert!(listing.head.starts_with("GET /v.php?"));
        assert!(
            listing
                .head
                .to_ascii_lowercase()
                .contains("cookie: account=new")
        );
        assert!(!ctx.session().is_warm());
        drop(listing.respond(response(
            "200 OK",
            "Content-Type: text/html\r\n",
            b"new site listing",
        )));
        warm.await.unwrap().unwrap();
        assert!(ctx.session().is_warm());
    }

    #[tokio::test]
    async fn shutdown_drains_held_daily_listing_and_idle_scheduler_without_abort() {
        let root = tempfile::tempdir().unwrap();
        let mut server = crate::test_support::http::Server::new().await;
        let ctx = shutdown_fixture(root.path(), &server.url).await;
        let scheduled = crate::p91::scheduler::run(ctx.clone(), ctx.common.clone());
        let scheduler = crate::runtime::BackgroundTask::spawn("fixture scheduler", scheduled);
        let owner_ctx = ctx.clone();
        let daily = ctx
            .jobs
            .spawn(async move { crate::p91::scheduler::run_daily(owner_ctx, "fixture").await })
            .unwrap();
        let held = server.next().await;
        tokio::time::timeout(std::time::Duration::from_secs(5), ctx.shutdown())
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), scheduler.finish())
            .await
            .unwrap();
        assert_eq!(daily.await.unwrap().unwrap().attempted, 0);
        assert!(!ctx.scheduler_status().running);
        assert_eq!(
            crate::p91::scheduler::run_daily(ctx.clone(), "late")
                .await
                .unwrap()
                .attempted,
            0
        );
        assert!(!crate::p91::scheduler::resume_task(ctx.clone(), "missing"));
        drop(held);
    }
    #[tokio::test]
    async fn shutdown_drains_outer_held_body_retains_prefix_and_blocks_queued_manual_resume() {
        let root = tempfile::tempdir().unwrap();
        let mut server = crate::test_support::http::Server::new().await;
        let ctx = shutdown_fixture(root.path(), &server.url).await;
        ctx.session().mark_warm(&ctx.request_context().1).unwrap();
        let card = crate::p91::source::scraper::VideoCard {
            id: "fixture".into(),
            url: format!("{}/view_video.php?viewkey=fixture", server.url),
            title: "fixture".into(),
            image_url: String::new(),
            duration_secs: None,
            rank: None,
            vid: None,
            hd: false,
            original: false,
        };
        let output = ctx.config().save_path;
        tokio::fs::create_dir_all(&output).await.unwrap();
        let part = output.join("fixture - fixture.mp4.part");
        tokio::fs::write(&part, b"abc").await.unwrap();
        let mut task = TaskInfo::new(&card.id, &card.url);
        task.title = card.title.clone();
        assert!(ctx.register_task(task));
        let owner_ctx = ctx.clone();
        let run = ctx
            .jobs
            .spawn(crate::p91::downloader::download_video(owner_ctx, card))
            .unwrap();
        let page = server.next().await;
        let html = format!("<source src='{}/media.mp4'>", server.url);
        drop(page.respond(crate::test_support::http::response(
            "200 OK",
            "Content-Type: text/html\r\n",
            html.as_bytes(),
        )));
        let media = server.next().await;
        assert!(media.head.to_ascii_lowercase().contains("range: bytes=3-"));
        let held = media.respond(b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 3-8/9\r\nContent-Length: 6\r\n\r\ndef".to_vec());
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while tokio::fs::metadata(&part).await.unwrap().len() != 6 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(ctx.register_task(TaskInfo::new("queued", &format!("{}/queued", server.url))));
        let queued_ctx = ctx.clone();
        let queued = ctx
            .jobs
            .spawn(async move { queued_ctx.download_slot("queued").await.is_none() })
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), ctx.shutdown())
            .await
            .unwrap();
        run.await.unwrap();
        assert!(queued.await.unwrap());
        assert_eq!(ctx.task_state("fixture"), Some(TaskState::Paused));
        assert_eq!(ctx.task_state("queued"), Some(TaskState::Paused));
        assert_eq!(tokio::fs::read(&part).await.unwrap(), b"abcdef");
        assert!(!output.join("fixture - fixture.mp4").exists());
        assert!(!ctx.register_task(TaskInfo::new("late", "http://127.0.0.1/late")));
        assert!(!crate::p91::scheduler::resume_task(ctx.clone(), "fixture"));
        assert!(!ctx.is_downloading("fixture"));
        assert!(ctx.history().is_empty());
        drop(held);
    }
}
