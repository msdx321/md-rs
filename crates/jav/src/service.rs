//! JAV initialization and protocol-specific shutdown.
use crate::{api, app, config, scheduler};
use media_runtime::{BackgroundTask, RunningEngine};
use std::sync::Arc;

pub async fn start(
    database: media_storage::Database,
    schedule: tokio::sync::watch::Receiver<media_config::app::Config>,
) -> anyhow::Result<RunningEngine> {
    let loaded = config::FILE.load_optional()?;
    let missing = loaded.is_none();
    let cfg = loaded.unwrap_or_default();
    cfg.title_matcher()?;
    cfg.validate_links()?;
    if missing {
        config::FILE.save(&cfg)?;
    }
    let ctx = Arc::new(app::AppCtx::new(cfg, database, schedule.clone()).await?);
    ctx.prune_history().await?;
    let cleanup_ctx = ctx.clone();
    let mut settings = schedule.clone();
    let cleanup = BackgroundTask::spawn("jav history cleanup", async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            tokio::select! {
                _ = interval.tick() => {},
                changed = settings.changed() => { if changed.is_err() { break; } },
            }
            if let Err(error) = cleanup_ctx.prune_history().await {
                log::warn!("cannot prune JAV history: {error:#}");
            }
        }
    });
    let router = api::router(ctx.clone());
    let scheduler = BackgroundTask::spawn("jav scheduler", scheduler::run(ctx.clone(), schedule));
    Ok(RunningEngine::new("/jav/", router, async move {
        cleanup.abort().await;
        scheduler.abort().await;
        ctx.request_daily_cancel();
        ctx.shutdown().await;
    }))
}
