//! Shared application context: configuration, HTTP client, dedup ledger,
//! live task registry and the SSE fan-out channel.

use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::jav::config::{Config, FILE};
use crate::jav::source::browser::{BrowserMinter, BrowserOptions};
use crate::jav::source::cf::{CookieSnapshot, CookieStore, MintFn};
use crate::jav::source::http::{Fetcher, build_client};
use crate::jav::storage::{HistorySummary, Record, Repository};
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
    pub source_url: String,
    pub state: TaskState,
    /// Human-readable stage: `resolving`, `downloading`, `merging`, …
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

/// Identifies a submission, including time spent waiting for the previous owner.
#[derive(Clone)]
pub struct DownloadRequest(Arc<()>);

struct RequestState {
    token: DownloadRequest,
    restart: Option<TaskInfo>,
}

#[derive(Default)]
struct TaskRegistry {
    tasks: HashMap<String, TaskInfo>,
    requests: HashMap<String, RequestState>,
    committing: HashSet<String>,
}

impl TaskRegistry {
    fn matches(&self, id: &str, request: &DownloadRequest) -> bool {
        self.requests
            .get(id)
            .is_some_and(|current| Arc::ptr_eq(&current.token.0, &request.0))
    }
}

pub struct AppCtx {
    cfg: RwLock<Config>,
    common: watch::Receiver<crate::configuration::app::Config>,
    pub(crate) download_limiter: Arc<crate::runtime::download_limiter::DownloadLimiter>,
    client: wreq::Client,
    /// Live `cf_clearance` cookie, shared with every `Fetcher`.
    cookies: Arc<CookieStore>,
    /// Headless browser that mints new cookies; `None` only when the user
    /// explicitly disabled automatic minting.
    browser: RwLock<Option<Arc<BrowserMinter>>>,
    pub(crate) config_update: tokio::sync::Mutex<()>,
    ledger: Repository,
    database: Database,
    tasks: Mutex<TaskRegistry>,
    downloads: Arc<DownloadSlots>,
    pub(crate) jobs: crate::runtime::http_lifecycle::HttpLifecycle,
    events: broadcast::Sender<TaskInfo>,
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
        let client = build_client()?;
        let ledger = Repository::load(database.clone()).await?;
        let (events, _) = broadcast::channel(256);
        let scheduler = SchedulerStatus::default();
        let (cookies, browser) = build_cookie_plumbing(&cfg, &database).await?;
        Ok(Self {
            cfg: RwLock::new(cfg),
            common,
            download_limiter,
            client,
            cookies,
            browser: RwLock::new(browser),
            config_update: tokio::sync::Mutex::new(()),
            ledger,
            database,
            tasks: Mutex::new(TaskRegistry::default()),
            downloads: Arc::new(DownloadSlots::new()),
            jobs: Default::default(),
            events,
            scheduler: Mutex::new(scheduler),
            daily_lock: tokio::sync::Mutex::new(()),
            daily_cancel: AtomicBool::new(false),
        })
    }

    // ── configuration ────────────────────────────────────────────────────

    /// Snapshot of the current configuration.
    pub fn config(&self) -> Config {
        let mut config = self.cfg.read().expect("config lock poisoned").clone();
        let common = self.common.borrow();
        config.save_path = common.jav_download_path.clone();
        config.temp_path = common.temp_path.clone();
        config
    }

    /// Persist settings and apply changes to the live cookie/browser state.
    pub async fn update_config(&self, cfg: Config) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.jobs.is_closed(),
            "service shutting down; settings rejected"
        );
        let previous = self.config();
        let browser_changed = cfg.browser_enabled != previous.browser_enabled
            || cfg.browser_path != previous.browser_path
            || cfg.browser_profile_dir != previous.browser_profile_dir
            || cfg.site_base != previous.site_base
            || cfg.popular_path != previous.popular_path
            || cfg.links != previous.links
            || cfg.user_agent != previous.user_agent;
        let credentials_changed = cfg.cookie != previous.cookie
            || cfg.user_agent != previous.user_agent
            || cfg.site_base != previous.site_base;
        FILE.save(&cfg)?;
        if browser_changed || credentials_changed {
            let mut gate = self.cookies.gate.lock().await;
            if browser_changed {
                let old = self.browser.read().expect("browser lock poisoned").clone();
                if let Some(browser) = old {
                    browser.shutdown().await;
                }
                *self.browser.write().expect("browser lock poisoned") = build_browser(&cfg);
            }
            let minter = self
                .browser
                .read()
                .expect("browser lock poisoned")
                .as_ref()
                .map(|b| persistent_minter(b, &cfg, &self.database));
            let credentials =
                credentials_changed.then(|| (cfg.cookie.clone(), cfg.user_agent.clone()));
            self.cookies.reconfigure(&mut gate, minter, credentials);
        }
        *self.cfg.write().expect("config lock poisoned") = cfg;
        self.downloads.notify();
        Ok(())
    }

    pub fn fetcher(&self) -> Fetcher {
        Fetcher::new(
            self.client.clone(),
            Arc::clone(&self.cookies),
            self.config().site_base,
        )
    }

    pub fn cookie_snapshot(&self) -> CookieSnapshot {
        self.cookies.snapshot()
    }

    /// True when a browser is available for automatic cookie minting.
    pub fn cookie_minting_available(&self) -> bool {
        self.cookies.can_mint()
    }

    /// True when the minting browser currently has a live process.
    pub fn browser_running(&self) -> bool {
        self.browser
            .read()
            .expect("browser lock poisoned")
            .as_ref()
            .is_some_and(|b| b.is_running())
    }

    /// Close every submission path before asking owners to retain partials.
    pub fn begin_shutdown(&self) {
        self.jobs.close();
        self.request_daily_cancel();
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        registry.requests.clear();
        let TaskRegistry {
            tasks, committing, ..
        } = &mut *registry;
        for (id, task) in tasks {
            if !committing.contains(id)
                && matches!(task.state, TaskState::Queued | TaskState::Running)
            {
                task.phase = if task.state == TaskState::Queued {
                    "paused"
                } else {
                    "pausing"
                }
                .into();
                task.state = TaskState::Paused;
                task.message = "service shutting down; partials retained".into();
                let _ = self.events.send(task.clone());
            }
        }
        drop(registry);
        self.downloads.notify();
    }

    /// Drain transfer/finalizer owners before tearing down their browser.
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.jobs.drain().await;
        let _config = self.config_update.lock().await;
        self.cookies.shutdown().await;
        let browser = self.browser.read().expect("browser lock poisoned").clone();
        if let Some(browser) = browser {
            browser.shutdown().await;
        }
    }

    // ── dedup ledger ─────────────────────────────────────────────────────

    pub fn history_retention_days(&self) -> u32 {
        self.common.borrow().history_retention_days
    }

    pub async fn prune_history(&self) -> anyhow::Result<()> {
        let days = self.history_retention_days();
        self.ledger.prune_history(days).await?;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
        let mut registry = self.tasks.lock().expect("tasks lock poisoned");
        let TaskRegistry {
            tasks, requests, ..
        } = &mut *registry;
        tasks.retain(|id, task| {
            !task.is_terminal()
                || requests
                    .get(id)
                    .is_some_and(|request| request.restart.is_some())
                || chrono::DateTime::parse_from_rfc3339(&task.updated_at)
                    .is_ok_and(|time| time > cutoff)
        });
        requests.retain(|id, _| tasks.contains_key(id));
        drop(registry);
        self.downloads.notify();
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

    #[cfg(test)]
    pub(crate) fn history_write_in_progress(&self) -> bool {
        self.ledger.write_in_progress()
    }

    pub async fn upsert_record(&self, record: Record) -> anyhow::Result<()> {
        self.ledger.upsert_record(record).await?;
        self.downloads.notify();
        Ok(())
    }

    pub async fn forget_record(&self, id: &str) -> anyhow::Result<()> {
        self.ledger.forget_record(id).await?;
        self.downloads.notify();
        Ok(())
    }

    pub async fn clear_history(&self) -> anyhow::Result<usize> {
        let removed = self
            .ledger
            .clear_history(self.history_retention_days())
            .await?;
        self.downloads.notify();
        Ok(removed)
    }

    pub async fn mark_daily_run(&self, date: &str) -> anyhow::Result<()> {
        self.ledger.mark_daily_run(date).await?;
        self.downloads.notify();
        Ok(())
    }

    // ── task registry + events ───────────────────────────────────────────

    /// All entry points share the same limit, including manual and resumed jobs.
    pub async fn download_slot(
        self: &Arc<Self>,
        id: &str,
        request: &DownloadRequest,
    ) -> Option<DownloadSlot> {
        let mut changes = self.downloads.subscribe();
        loop {
            // Keep the stop signal until the previous owner has finished all
            // publication and cleanup, then activate the accepted restart.
            if let Some(eligible) = self.downloads.while_idle(id, || {
                let mut registry = self.tasks.lock().expect("task lock poisoned");
                if self.jobs.is_closed() || !registry.matches(id, request) {
                    return false;
                }
                let restart = registry.requests.get_mut(id).unwrap().restart.take();
                if let Some(task) = restart {
                    if registry
                        .tasks
                        .get(id)
                        .is_none_or(|old| old.state == TaskState::Completed)
                    {
                        registry.requests.remove(id);
                        return false;
                    }
                    let _ = self.events.send(task.clone());
                    registry.tasks.insert(id.to_string(), task);
                }
                true
            }) {
                if !eligible {
                    return None;
                }
                break;
            }
            if !self
                .tasks
                .lock()
                .expect("task lock poisoned")
                .matches(id, request)
            {
                return None;
            }
            changes.changed().await.expect("slot sender is alive");
        }
        self.downloads
            .acquire(
                Some(id),
                || {
                    let registry = self.tasks.lock().expect("task lock poisoned");
                    (!self.jobs.is_closed()
                        && registry.matches(id, request)
                        && registry
                            .tasks
                            .get(id)
                            .is_some_and(|task| task.state == TaskState::Queued))
                    .then(|| {
                        self.cfg
                            .read()
                            .expect("config lock poisoned")
                            .concurrent_videos
                            .clamp(1, 8)
                    })
                },
                pending(),
            )
            .await
    }

    pub fn tasks(&self) -> Vec<TaskInfo> {
        let guard = self.tasks.lock().expect("task lock poisoned");
        let mut out: Vec<TaskInfo> = guard.tasks.values().cloned().collect();
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

    /// Read control state without cloning task strings on every segment/chunk.
    pub fn task_state(&self, id: &str) -> Option<TaskState> {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .tasks
            .get(id)
            .map(|task| task.state)
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
            self.downloads.notify();
        }
    }

    /// Register only after any previous owner has finished cleanup.
    pub fn register_task(&self, info: TaskInfo) -> Option<DownloadRequest> {
        let request = self
            .downloads
            .while_idle(&info.id.clone(), || {
                let mut registry = self.tasks.lock().expect("task lock poisoned");
                if self.jobs.is_closed()
                    || registry.committing.contains(&info.id)
                    || registry
                        .requests
                        .get(&info.id)
                        .is_some_and(|request| request.restart.is_some())
                    || registry.tasks.get(&info.id).is_some_and(|existing| {
                        !existing.is_terminal() && existing.state != TaskState::Paused
                    })
                {
                    return None;
                }
                let token = DownloadRequest(Arc::new(()));
                registry.requests.insert(
                    info.id.clone(),
                    RequestState {
                        token: token.clone(),
                        restart: None,
                    },
                );
                let _ = self.events.send(info.clone());
                registry.tasks.insert(info.id.clone(), info);
                Some(token)
            })
            .flatten();
        if request.is_some() {
            self.downloads.notify();
        }
        request
    }

    /// Accept one restart without changing the old owner's stop signal.
    pub fn request_resume(&self, id: &str) -> Option<(TaskInfo, DownloadRequest)> {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        if self.jobs.is_closed() || registry.committing.contains(id) {
            return None;
        }
        let mut task = registry.tasks.get(id)?.clone();
        if !matches!(
            task.state,
            TaskState::Paused | TaskState::Failed | TaskState::Cancelled
        ) || registry
            .requests
            .get(id)
            .is_some_and(|request| request.restart.is_some())
        {
            return None;
        }
        task.state = TaskState::Queued;
        task.phase = "queued".into();
        task.message = "resuming".into();
        task.updated_at = now_rfc3339();
        let token = DownloadRequest(Arc::new(()));
        registry.requests.insert(
            id.to_string(),
            RequestState {
                token: token.clone(),
                restart: Some(task.clone()),
            },
        );
        drop(registry);
        self.downloads.notify();
        Some((task, token))
    }

    /// User stops invalidate queued submissions and pending restarts atomically.
    pub fn stop_task(&self, id: &str, cancel: bool) -> bool {
        let updated = {
            let mut registry = self.tasks.lock().expect("task lock poisoned");
            if registry.committing.contains(id) {
                return false;
            }
            let pending = registry
                .requests
                .get(id)
                .is_some_and(|request| request.restart.is_some());
            let Some(task) = registry.tasks.get_mut(id) else {
                return false;
            };
            if !pending
                && if cancel {
                    task.is_terminal()
                } else {
                    !matches!(task.state, TaskState::Running | TaskState::Queued)
                }
            {
                return false;
            }
            let queued = task.state == TaskState::Queued;
            task.state = if cancel {
                TaskState::Cancelled
            } else {
                TaskState::Paused
            };
            task.phase = match (cancel, queued) {
                (true, true) => "cancelled",
                (true, false) => "cancelling",
                (false, true) => "paused",
                (false, false) => "pausing",
            }
            .into();
            task.message = if cancel {
                "cancelled by user"
            } else {
                "paused by user"
            }
            .into();
            task.updated_at = now_rfc3339();
            let updated = task.clone();
            registry.requests.remove(id);
            updated
        };
        let _ = self.events.send(updated);
        self.downloads.notify();
        true
    }

    /// Linearization point shared with user stops, before cleanup or persistence.
    pub(crate) fn begin_terminal_commit(&self, id: &str) -> bool {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        if registry.committing.contains(id) {
            return true;
        }
        if registry
            .tasks
            .get(id)
            .is_none_or(|task| task.state != TaskState::Running)
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

    /// Scheduler bookkeeping must not publish over a newer submission.
    pub fn update_request(
        &self,
        id: &str,
        request: &DownloadRequest,
        f: impl FnOnce(&mut TaskInfo),
    ) {
        let updated = {
            let mut registry = self.tasks.lock().expect("task lock poisoned");
            if self.jobs.is_closed() || !registry.matches(id, request) {
                return;
            }
            let Some(task) = registry.tasks.get_mut(id) else {
                return;
            };
            f(task);
            task.updated_at = now_rfc3339();
            task.clone()
        };
        let _ = self.events.send(updated);
        self.downloads.notify();
    }

    /// Remove only failed entries under one lock so a concurrent retry is preserved.
    pub fn clear_failed_tasks(&self) -> usize {
        let mut tasks = self.tasks.lock().expect("task lock poisoned");
        let before = tasks.tasks.len();
        let TaskRegistry {
            tasks: entries,
            requests,
            ..
        } = &mut *tasks;
        entries.retain(|id, task| {
            task.state != TaskState::Failed
                || requests
                    .get(id)
                    .is_some_and(|request| request.restart.is_some())
        });
        requests.retain(|id, _| entries.contains_key(id));
        let removed = before - tasks.tasks.len();
        drop(tasks);
        self.downloads.notify();
        removed
    }

    pub fn subscribe(&self) -> broadcast::Receiver<TaskInfo> {
        self.events.subscribe()
    }

    pub fn subscribe_status(&self) -> watch::Receiver<()> {
        self.downloads.subscribe()
    }

    pub fn subscribe_cookie(&self) -> watch::Receiver<()> {
        self.cookies.subscribe()
    }

    /// Forget a finished task so it disappears from the UI.
    pub fn drop_task(&self, id: &str) -> bool {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        if registry.committing.contains(id)
            || registry
                .tasks
                .get(id)
                .is_none_or(|task| !task.is_terminal())
            || registry
                .requests
                .get(id)
                .is_some_and(|request| request.restart.is_some())
        {
            return false;
        }
        registry.requests.remove(id);
        let removed = registry.tasks.remove(id).is_some();
        drop(registry);
        self.downloads.notify();
        removed
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

/// Build the shared cookie store and its browser minter.
///
/// Automatic cookie minting is always available; `browser_enabled: false` only
/// turns the *use* of it off, which is the escape hatch for someone who would
/// rather paste a `cf_clearance` value by hand.
async fn build_cookie_plumbing(
    cfg: &Config,
    database: &Database,
) -> anyhow::Result<(Arc<CookieStore>, Option<Arc<BrowserMinter>>)> {
    let browser = build_browser(cfg);
    let store = CookieStore::new(
        cfg.cookie.clone(),
        cfg.user_agent.clone(),
        browser
            .as_ref()
            .map(|b| persistent_minter(b, cfg, database)),
    );
    if let Some(cookie) =
        crate::jav::storage::cookie::load(database, &cfg.site_base, &cfg.cookie, &cfg.user_agent)
            .await?
    {
        store.restore(cookie.value, cookie.user_agent);
    }
    Ok((Arc::new(store), browser))
}

/// Save only successful mints. The refresh gate also serializes settings changes,
/// so cached credentials retain the exact site and settings that produced them.
fn persistent_minter(browser: &Arc<BrowserMinter>, cfg: &Config, database: &Database) -> MintFn {
    let mint = browser.mint_fn();
    let database = database.clone();
    let site = cfg.site_base.clone();
    let configured_cookie = cfg.cookie.clone();
    let configured_user_agent = cfg.user_agent.clone();
    Arc::new(move |ua| {
        let mint = mint.clone();
        let database = database.clone();
        let site = site.clone();
        let configured_cookie = configured_cookie.clone();
        let configured_user_agent = configured_user_agent.clone();
        Box::pin(async move {
            let minted = mint(ua).await?;
            if !minted.cookie.trim().is_empty() {
                let cookie = crate::jav::storage::cookie::Cookie {
                    value: minted.cookie.clone(),
                    user_agent: minted.user_agent.clone(),
                };
                if let Err(error) = crate::jav::storage::cookie::save(
                    &database,
                    &site,
                    &configured_cookie,
                    &configured_user_agent,
                    &cookie,
                )
                .await
                {
                    log::warn!("cannot persist clearance cookie: {error}");
                }
            }
            Ok(minted)
        })
    })
}

fn build_browser(cfg: &Config) -> Option<Arc<BrowserMinter>> {
    if !cfg.browser_enabled {
        return None;
    }

    // The browser must never present Chrome's default headless agent string:
    // Cloudflare rejects `HeadlessChrome` outright even though the rest of the
    // fingerprint is genuine. So an unset user agent does *not* mean "let
    // Chromium decide" — it means "use the coherent browser default", and only
    // an explicit value overrides it.
    let browser_user_agent = if cfg.user_agent.trim().is_empty() {
        crate::jav::source::cf::DEFAULT_USER_AGENT.to_string()
    } else {
        cfg.user_agent.clone()
    };
    let mut opts = BrowserOptions::new(
        cfg.browser_profile_dir.clone(),
        cfg.for_link(&cfg.listing_links()[0]).popular_url(1),
        browser_user_agent,
    );
    // `CHROME_PATH` lets the container image point at its own Chromium without
    // the user having to edit anything.
    opts.executable = if cfg.browser_path.as_os_str().is_empty() {
        std::env::var_os("CHROME_PATH")
            .map(PathBuf::from)
            .unwrap_or_default()
    } else {
        cfg.browser_path.clone()
    };
    Some(Arc::new(BrowserMinter::new(opts)))
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
        assert!(ctx.register_task(info).is_some());
        assert!(ctx.register_task(TaskInfo::new("1", "u")).is_none());
    }

    #[test]
    fn allows_retry_after_terminal_state() {
        let ctx = ctx();
        let mut info = TaskInfo::new("1", "u");
        info.state = TaskState::Failed;
        assert!(ctx.register_task(info).is_some());
        assert!(ctx.register_task(TaskInfo::new("1", "u")).is_some());
    }

    #[test]
    fn slots_observe_live_capacity_and_queued_cancellation() {
        let ctx = Arc::new(ctx());
        ctx.cfg.write().unwrap().concurrent_videos = 1;
        let requests: Vec<_> = ["1", "2", "3"]
            .into_iter()
            .map(|id| ctx.register_task(TaskInfo::new(id, "u")).unwrap())
            .collect();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let first = ctx.download_slot("1", &requests[0]).await.unwrap();
            let second = ctx.download_slot("2", &requests[1]);
            tokio::pin!(second);
            assert!(futures_util::poll!(&mut second).is_pending());
            // Apply only the live in-memory setting; never persist real config.
            ctx.cfg.write().unwrap().concurrent_videos = 2;
            ctx.downloads.notify();
            let second = second.await.unwrap();
            let third = ctx.download_slot("3", &requests[2]);
            tokio::pin!(third);
            assert!(futures_util::poll!(&mut third).is_pending());
            ctx.update_task("3", |task| task.state = TaskState::Cancelled);
            assert!(third.await.is_none());
            drop(first);
            drop(second);
        });
    }

    #[test]
    fn queued_resume_retires_old_waiter_and_cancellation_retires_restart() {
        let ctx = Arc::new(ctx());
        ctx.cfg.write().unwrap().concurrent_videos = 1;
        let blocker = ctx.register_task(TaskInfo::new("blocker", "u")).unwrap();
        let old = ctx.register_task(TaskInfo::new("1", "u")).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let slot = ctx.download_slot("blocker", &blocker).await.unwrap();
            let waiting = ctx.download_slot("1", &old);
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            assert!(ctx.stop_task("1", false));
            let (_, restart) = ctx.request_resume("1").unwrap();
            assert!(ctx.request_resume("1").is_none());
            let resumed = ctx.download_slot("1", &restart);
            tokio::pin!(resumed);
            assert!(futures_util::poll!(&mut resumed).is_pending());
            assert!(waiting.await.is_none());
            assert!(ctx.stop_task("1", true));
            assert!(resumed.await.is_none());
            drop(slot);
            assert_eq!(ctx.task_state("1"), Some(TaskState::Cancelled));
            let (_, retry) = ctx.request_resume("1").unwrap();
            assert!(ctx.download_slot("1", &retry).await.is_some());
        });
    }

    #[tokio::test]
    async fn resume_all_uses_deferred_owner_and_cancel_all_withdraws_restart() {
        let root = tempfile::tempdir().unwrap();
        let mut server = crate::test_support::http::Server::new().await;
        let common = crate::configuration::app::Config {
            jav_download_path: root.path().join("output"),
            temp_path: root.path().join("partial"),
            ..Default::default()
        };
        let common = watch::channel(common).1;
        let mut ctx = AppCtx::new(
            Config {
                browser_enabled: false,
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
        let ctx = Arc::new(ctx);
        let card = crate::jav::source::scraper::VideoCard {
            id: "fixture".into(),
            url: format!("{}/fixture", server.url),
            title: "fixture".into(),
            image_url: String::new(),
            duration_secs: None,
            rank: None,
        };
        let cache = ctx.config().temp_path.join("temp_fixture");
        tokio::fs::create_dir_all(&cache).await.unwrap();
        tokio::fs::write(cache.join("0.ts"), b"preserved")
            .await
            .unwrap();
        let request = ctx
            .register_task(TaskInfo::new(&card.id, &card.url))
            .unwrap();
        // Poll the actual outer downloader only when requested by this test.
        // Holding the page request freezes it before interruption/finalization.
        let old = crate::jav::downloader::download_video(ctx.clone(), card.clone(), request);
        tokio::pin!(old);
        let held = tokio::select! {
            request = server.next() => request,
            _ = &mut old => panic!("old owner exited before resolving"),
        };
        assert!(ctx.stop_task("fixture", false));
        assert_eq!(crate::jav::scheduler::resume_all(ctx.clone()).await, 1);
        assert_eq!(crate::jav::scheduler::resume_all(ctx.clone()).await, 0);
        assert!(!crate::jav::scheduler::resume_task(ctx.clone(), "fixture"));
        tokio::task::yield_now().await;
        assert_eq!(ctx.task_state("fixture"), Some(TaskState::Paused));
        assert!(
            ctx.register_task(TaskInfo::new("fixture", &card.url))
                .is_none()
        );
        old.await;
        drop(held);
        let next = server.next().await;
        assert!(next.head.starts_with("GET /fixture "));
        assert_eq!(ctx.task_state("fixture"), Some(TaskState::Running));
        assert_eq!(
            tokio::fs::read(cache.join("0.ts")).await.unwrap(),
            b"preserved"
        );
        // Pause/resume/cancel-all while this second outer runner still owns the key.
        crate::jav::scheduler::pause_active(&ctx);
        assert!(crate::jav::scheduler::resume_task(ctx.clone(), "fixture"));
        crate::jav::scheduler::cancel_active(&ctx);
        let mut status = ctx.subscribe_status();
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while ctx.downloads.contains("fixture") {
                status.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        drop(next);
        assert_eq!(ctx.task_state("fixture"), Some(TaskState::Cancelled));
        assert!(!cache.exists());
        assert!(ctx.history().is_empty());
    }

    #[test]
    fn pending_failed_retry_survives_clear_and_stale_scheduler_publication() {
        let ctx = Arc::new(ctx());
        let old = ctx.register_task(TaskInfo::new("1", "u")).unwrap();
        ctx.update_task("1", |task| task.state = TaskState::Failed);
        let (_, restart) = ctx.request_resume("1").unwrap();
        assert_eq!(ctx.clear_failed_tasks(), 0);
        assert!(!ctx.drop_task("1"));
        ctx.update_request("1", &old, |task| task.state = TaskState::Cancelled);
        assert_eq!(ctx.task_state("1"), Some(TaskState::Failed));
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert!(ctx.download_slot("1", &restart).await.is_some());
        });
        assert_eq!(ctx.task_state("1"), Some(TaskState::Queued));
        assert!(ctx.stop_task("1", false));
        let replacement = ctx.register_task(TaskInfo::new("1", "other")).unwrap();
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert!(ctx.download_slot("1", &restart).await.is_none());
            assert!(ctx.download_slot("1", &replacement).await.is_some());
        });
    }

    #[test]
    fn copy_state_and_running_count_match_registry() {
        let ctx = ctx();
        assert!(ctx.register_task(TaskInfo::new("1", "u")).is_some());
        assert!(ctx.register_task(TaskInfo::new("2", "u")).is_some());
        ctx.update_task("1", |task| task.state = TaskState::Running);
        assert_eq!(ctx.task_state("1"), ctx.task("1").map(|task| task.state));
        assert_eq!(ctx.task_state("missing"), None);
        assert_eq!(ctx.running_task_count(), 1);
        ctx.update_task("1", |task| task.state = TaskState::Paused);
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
            jav_download_path: root.join("output"),
            temp_path: root.join("partial"),
            ..Default::default()
        })
        .1;
        let mut ctx = AppCtx::new(
            Config {
                site_base: base.into(),
                concurrent_videos: 1,
                browser_enabled: false,
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
    async fn shutdown_drains_held_daily_listing_and_idle_scheduler_without_abort() {
        let root = tempfile::tempdir().unwrap();
        let mut server = crate::test_support::http::Server::new().await;
        let ctx = shutdown_fixture(root.path(), &server.url).await;
        let scheduled = crate::jav::scheduler::run(ctx.clone(), ctx.common.clone());
        let scheduler = crate::runtime::BackgroundTask::spawn("fixture scheduler", scheduled);
        let owner_ctx = ctx.clone();
        let daily = ctx
            .jobs
            .spawn(async move { crate::jav::scheduler::run_daily(owner_ctx, "fixture").await })
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
            crate::jav::scheduler::run_daily(ctx.clone(), "late")
                .await
                .unwrap()
                .attempted,
            0
        );
        assert!(!crate::jav::scheduler::resume_task(ctx.clone(), "missing"));
        drop(held);
    }
    #[tokio::test]
    async fn shutdown_drains_daily_joinset_child_in_actual_resolver_and_preserves_cache() {
        let root = tempfile::tempdir().unwrap();
        let mut server = crate::test_support::http::Server::new().await;
        let mut ctx = shutdown_fixture(root.path(), &server.url).await;
        let address: std::net::SocketAddr =
            server.url.trim_start_matches("http://").parse().unwrap();
        Arc::get_mut(&mut ctx).unwrap().client = wreq::Client::builder()
            .no_proxy()
            .resolve("fixture.missav.ai", address)
            .build()
            .unwrap();
        ctx.cfg.write().unwrap().top_n = 1;
        let cache = ctx.config().temp_path.join("temp_fixture-1");
        tokio::fs::create_dir_all(&cache).await.unwrap();
        tokio::fs::write(cache.join("0.ts"), b"cached")
            .await
            .unwrap();
        let owner_ctx = ctx.clone();
        let daily = ctx
            .jobs
            .spawn(async move { crate::jav::scheduler::run_daily(owner_ctx, "fixture").await })
            .unwrap();
        let listing = server.next().await;
        // The parser requires a MissAV hostname. Pin this fixture hostname to
        // the synthetic listener above; no DNS or real provider is consulted.
        let html = format!(
            "<div class='thumbnail'><a href='http://fixture.missav.ai:{}/cn/fixture-1'><img alt='fixture'></a></div>",
            address.port()
        );
        drop(listing.respond(crate::test_support::http::response(
            "200 OK",
            "Content-Type: text/html\r\n",
            html.as_bytes(),
        )));
        let held = server.next().await;
        assert!(held.head.starts_with("GET /cn/fixture-1 "));
        tokio::time::timeout(std::time::Duration::from_secs(5), ctx.shutdown())
            .await
            .unwrap();
        let report = daily.await.unwrap().unwrap();
        assert_eq!(report.attempted, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(ctx.task_state("fixture-1"), Some(TaskState::Paused));
        assert_eq!(ctx.task("fixture-1").unwrap().phase, "paused");
        assert_eq!(
            tokio::fs::read(cache.join("0.ts")).await.unwrap(),
            b"cached"
        );
        assert!(ctx.history().is_empty());
        drop(held);
    }
}
