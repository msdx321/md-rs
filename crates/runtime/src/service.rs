use std::future::Future;

use axum::{Router, response::Redirect, routing::get};
use futures_util::future::{BoxFuture, join_all};

/// An initialized engine with routes and the resources needed to shut it down.
/// The engine's shutdown future owns its background tasks, so dropping this
/// handle also drops (and aborts) those tasks.
#[must_use = "keep the engine handle alive until shutdown"]
pub struct RunningEngine {
    mount_path: &'static str,
    router: Router,
    shutdown: BoxFuture<'static, ()>,
}

impl RunningEngine {
    /// `mount_path` is an engine-owned absolute route prefix ending in `/`.
    /// The shutdown future must own all service-level task handles.
    pub fn new(
        mount_path: &'static str,
        router: Router,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> Self {
        Self {
            mount_path,
            router,
            shutdown: Box::pin(shutdown),
        }
    }

    /// Mount an engine and redirect its slashless URL to the canonical path.
    pub fn mount(&self, app: Router) -> Router {
        let path = self.mount_path;
        app.route(
            path.trim_end_matches('/'),
            get(move || async move { Redirect::permanent(path) }),
        )
        .nest(path, self.router.clone())
    }

    pub async fn shutdown(self) {
        self.shutdown.await;
    }
}

/// Start every engine's shutdown concurrently, so one draining engine does not
/// delay cancellation in another.
pub async fn shutdown_all(engines: Vec<RunningEngine>) {
    join_all(engines.into_iter().map(RunningEngine::shutdown)).await;
}
