//! Shared application context: configuration, HTTP client, dedup ledger,
//! live task registry and the SSE fan-out channel.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::ph::config::{Config, FILE};
use crate::ph::source::http::{DEFAULT_USER_AGENT, Fetcher, Session, build_client};
use crate::ph::storage::{HistorySummary, Provider, Record, Repository};
use crate::runtime::download_slots::DownloadSlot;
use crate::runtime::video_tasks::VideoTasks;
pub use crate::runtime::video_tasks::{TaskInfo, TaskState, now_rfc3339};
use crate::storage::Database;

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
    pub(crate) transfers: VideoTasks,
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
        let ledger = Repository::load(database, Provider::Ph).await?;
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
            transfers: VideoTasks::new(),
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
            !self.transfers.jobs.is_closed(),
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
        self.transfers.notify();
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
        self.transfers.prune(days);
        Ok(())
    }

    pub fn history(&self) -> Vec<Record> {
        self.ledger.history(self.history_retention_days())
    }

    pub fn history_limited(&self, limit: usize) -> (Vec<Record>, usize) {
        self.ledger
            .history_limited(self.history_retention_days(), limit)
    }

    pub fn history_summary(&self) -> HistorySummary {
        self.ledger.history_summary(self.history_retention_days())
    }

    pub fn is_completed(&self, id: &str) -> bool {
        self.ledger.is_completed(id)
    }

    pub async fn upsert_record(&self, record: Record) -> anyhow::Result<()> {
        self.ledger.upsert_record(record).await?;
        self.transfers.library_changed();
        Ok(())
    }

    pub async fn forget_record(&self, id: &str) -> anyhow::Result<()> {
        self.ledger.forget_record(id).await?;
        self.transfers.library_changed();
        Ok(())
    }

    pub async fn clear_history(&self) -> anyhow::Result<usize> {
        let removed = self
            .ledger
            .clear_history(self.history_retention_days())
            .await?;
        self.transfers.library_changed();
        Ok(removed)
    }

    pub async fn subscription(&self, source: &str) -> anyhow::Result<Option<String>> {
        crate::storage::ph::subscription(&self.ledger, source).await
    }

    pub async fn save_subscription(&self, source: &str, state: &str) -> anyhow::Result<()> {
        crate::storage::ph::save_subscription(&self.ledger, source, state).await
    }

    pub async fn mark_daily_run(&self, date: &str) -> anyhow::Result<()> {
        self.ledger.mark_daily_run(date).await?;
        self.transfers.notify();
        Ok(())
    }

    // ── task registry + events ───────────────────────────────────────────

    /// All entry points share the same limit, including manual and resumed jobs.
    pub async fn download_slot(&self, id: &str) -> Option<DownloadSlot> {
        self.transfers
            .download_slot(id, || {
                self.cfg
                    .read()
                    .expect("config lock poisoned")
                    .concurrent_videos
                    .clamp(1, 8)
            })
            .await
    }

    pub fn begin_shutdown(&self) {
        self.transfers.jobs.close();
        self.request_daily_cancel();
        self.transfers.pause_for_shutdown();
    }

    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.transfers.jobs.drain().await;
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
        self.transfers.notify();
    }

    pub fn request_daily_cancel(&self) {
        self.daily_cancel.store(true, Ordering::SeqCst);
    }

    pub fn take_daily_cancel(&self) -> bool {
        self.daily_cancel.swap(false, Ordering::SeqCst)
    }
}
