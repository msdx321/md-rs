//! Shared application context: configuration, HTTP client, dedup ledger,
//! live task registry and the SSE fan-out channel.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};

use crate::jav::config::{Config, FILE};
use crate::jav::source::browser::{BrowserMinter, BrowserOptions};
use crate::jav::source::cf::{CookieSnapshot, CookieStore, MintFn};
use crate::jav::source::http::{Fetcher, build_client};
use crate::jav::storage::{Record, Repository, State};
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

pub struct AppCtx {
    cfg: RwLock<Config>,
    common: watch::Receiver<crate::configuration::app::Config>,
    client: wreq::Client,
    /// Live `cf_clearance` cookie, shared with every `Fetcher`.
    cookies: Arc<CookieStore>,
    /// Headless browser that mints new cookies; `None` only when the user
    /// explicitly disabled automatic minting.
    browser: RwLock<Option<Arc<BrowserMinter>>>,
    pub(crate) config_update: tokio::sync::Mutex<()>,
    ledger: Repository,
    database: Database,
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
    ) -> anyhow::Result<Self> {
        let client = build_client()?;
        let ledger = Repository::load(database.clone()).await?;
        let (events, _) = broadcast::channel(256);
        let scheduler = SchedulerStatus::default();
        let (cookies, browser) = build_cookie_plumbing(&cfg, &database).await?;
        Ok(Self {
            cfg: RwLock::new(cfg),
            common,
            client,
            cookies,
            browser: RwLock::new(browser),
            config_update: tokio::sync::Mutex::new(()),
            ledger,
            database,
            tasks: Mutex::new(HashMap::new()),
            downloads: Mutex::new(HashSet::new()),
            events,
            status_events: watch::channel(()).0,
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
        self.status_events.send_replace(());
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

    /// Shut the minting browser down, if one was launched.
    pub async fn shutdown(&self) {
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

    pub fn state_snapshot(&self) -> State {
        self.ledger.snapshot()
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

    pub fn subscribe_cookie(&self) -> watch::Receiver<()> {
        self.cookies.subscribe()
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
