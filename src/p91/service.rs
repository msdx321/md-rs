//! 91Porn initialization and shutdown.
use crate::p91::{api, app, config, scheduler};
use crate::runtime::{BackgroundTask, RunningEngine};
use std::sync::Arc;

pub async fn start(
    database: crate::storage::Database,
    schedule: tokio::sync::watch::Receiver<crate::configuration::app::Config>,
    limiter: Arc<crate::runtime::download_limiter::DownloadLimiter>,
) -> anyhow::Result<RunningEngine> {
    let loaded = config::FILE.load_optional()?;
    let missing = loaded.is_none();
    let cfg = loaded.unwrap_or_default();
    cfg.title_matcher()?;
    cfg.validate_links()?;
    if missing {
        config::FILE.save(&cfg)?;
    }
    let ctx = Arc::new(app::AppCtx::new(cfg, database, schedule.clone(), limiter).await?);
    ctx.prune_history().await?;
    let cleanup_ctx = ctx.clone();
    let mut settings = schedule.clone();
    let cleanup = BackgroundTask::spawn("p91 history cleanup", async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = interval.tick() => {},
                changed = settings.changed() => { if changed.is_err() { break; } },
            }
            if let Err(error) = cleanup_ctx.prune_history().await {
                log::warn!("cannot prune 91Porn history: {error:#}");
            }
        }
    });
    let router = api::router(ctx.clone());
    let scheduler = BackgroundTask::spawn("p91 scheduler", scheduler::run(ctx.clone(), schedule));
    Ok(RunningEngine::new("/p91/", router, async move {
        cleanup.abort().await;
        scheduler.abort().await;
        ctx.request_daily_cancel();
    }))
}
