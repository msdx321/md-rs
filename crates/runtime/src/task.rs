use std::future::Future;
use tokio::task::JoinHandle;

/// A service-owned task. Unlike a raw JoinHandle, dropping it does not detach it.
#[must_use = "dropping the task aborts it"]
pub struct BackgroundTask {
    name: &'static str,
    handle: JoinHandle<()>,
}

impl BackgroundTask {
    pub fn spawn(name: &'static str, future: impl Future<Output = ()> + Send + 'static) -> Self {
        Self {
            name,
            handle: tokio::spawn(future),
        }
    }

    /// Wait after the engine has requested cooperative shutdown. The handle
    /// stays owned by self even if this wait is cancelled.
    pub async fn finish(mut self) {
        if let Err(error) = (&mut self.handle).await
            && !error.is_cancelled()
        {
            log::error!("background task {} failed: {error}", self.name);
        }
    }

    /// Stop an idle timer or scheduler and wait until it has released resources.
    pub async fn abort(self) {
        self.handle.abort();
        self.finish().await;
    }
}

impl Drop for BackgroundTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}
