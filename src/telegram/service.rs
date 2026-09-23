//! Telegram initialization and protocol-specific shutdown.
use crate::runtime::{BackgroundTask, RunningEngine};
use crate::telegram::{api, app};
use std::sync::Arc;
use tokio::sync::mpsc;

pub async fn start(
    database: crate::storage::Database,
    schedule: tokio::sync::watch::Receiver<crate::configuration::app::Config>,
    limiter: Arc<crate::runtime::download_limiter::DownloadLimiter>,
) -> anyhow::Result<RunningEngine> {
    let shutdown = app::Shutdown::new();
    let (tx, mut rx) = mpsc::channel(16);
    let state = Arc::new(api::ApiState::new(tx, database, schedule.clone(), limiter).await?);
    state.prune_history().await;
    let router = api::router(state.clone());
    let mut retention_settings = schedule.clone();
    let worker_shutdown = shutdown.clone();
    let worker_state = state.clone();
    let worker = BackgroundTask::spawn("telegram worker", async move {
        let mut replace_credentials = false;
        loop {
            let cfg = tokio::select! {
                result = app::setup::configure(&worker_state, replace_credentials) => result,
                _ = worker_shutdown.cancelled() => break,
            };
            let result = match cfg {
                Ok(cfg) => {
                    app::run_downloader(
                        cfg,
                        worker_state.clone(),
                        &mut rx,
                        worker_shutdown.clone(),
                        schedule.clone(),
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            if worker_shutdown.is_cancelled() {
                break;
            }
            let message = match result {
                Ok(()) => "Telegram stopped. Retry to reconnect.".into(),
                Err(error) => format!(
                    "Telegram could not continue: {error}. Retry or update your API credentials."
                ),
            };
            worker_state.set_status("Telegram disconnected").await;
            tokio::select! {
                result = worker_state.login_prompt("retry", &message) => {
                    replace_credentials = result.is_ok_and(|input| input.value == "credentials");
                },
                _ = worker_shutdown.cancelled() => break,
            }
        }
    });
    let cleanup = BackgroundTask::spawn("telegram history cleanup", async move {
        let mut days = retention_settings
            .borrow_and_update()
            .history_retention_days;
        while retention_settings.changed().await.is_ok() {
            let next = retention_settings
                .borrow_and_update()
                .history_retention_days;
            if next == days {
                continue;
            }
            days = next;
            state.prune_history().await;
            state.publish().await;
        }
    });
    Ok(RunningEngine::new("/telegram/", router, async move {
        shutdown.cancel();
        worker.finish().await;
        cleanup.abort().await;
    }))
}
