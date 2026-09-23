//! JAV initialization and protocol-specific shutdown.
use crate::jav::{api, app, config, scheduler};
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
    let cleanup = BackgroundTask::spawn("jav history cleanup", async move {
        let mut days = settings.borrow_and_update().history_retention_days;
        while settings.changed().await.is_ok() {
            let next = settings.borrow_and_update().history_retention_days;
            if next == days {
                continue;
            }
            days = next;
            if let Err(error) = cleanup_ctx.prune_history().await {
                log::warn!("cannot prune JAV history: {error:#}");
            }
        }
    });
    let router = api::router(ctx.clone());
    let scheduler = BackgroundTask::spawn("jav scheduler", scheduler::run(ctx.clone(), schedule));
    Ok(RunningEngine::new("/jav/", router, async move {
        ctx.begin_shutdown();
        cleanup.abort().await;
        scheduler.finish().await;
        ctx.shutdown().await;
    }))
}
