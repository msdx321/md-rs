//! JAV initialization and protocol-specific shutdown.
use crate::{api, app, config, scheduler};
use media_runtime::{BackgroundTask, RunningEngine};
use std::sync::Arc;

pub async fn start(database: media_storage::Database) -> anyhow::Result<RunningEngine> {
    let loaded = config::FILE.load_optional()?;
    let missing = loaded.is_none();
    let cfg = loaded.unwrap_or_default();
    cfg.title_matcher()?;
    cfg.validate_links()?;
    if missing {
        config::FILE.save(&cfg)?;
    }
    let ctx = Arc::new(app::AppCtx::new(cfg, database).await?);
    let router = api::router(ctx.clone());
    let scheduler = BackgroundTask::spawn("jav scheduler", scheduler::run(ctx.clone()));
    Ok(RunningEngine::new("/jav/", router, async move {
        scheduler.abort().await;
        ctx.request_daily_cancel();
        ctx.shutdown().await;
    }))
}
