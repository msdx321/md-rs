//! HTTP engine work ownership only; transport and stop policies stay in each provider.
use std::future::Future;
use std::sync::Mutex;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tokio_util::task::task_tracker::TaskTrackerToken;

#[derive(Default)]
pub(crate) struct HttpLifecycle {
    closed: Mutex<bool>,
    tasks: TaskTracker,
    stop: CancellationToken,
}

impl HttpLifecycle {
    /// Reserve ownership before spawning, including futures not yet polled.
    pub fn enter(&self) -> Option<TaskTrackerToken> {
        let closed = self.closed.lock().expect("HTTP lifecycle lock poisoned");
        (!*closed).then(|| self.tasks.token())
    }

    pub fn spawn<F>(&self, work: F) -> Option<JoinHandle<F::Output>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let owner = self.enter()?;
        Some(tokio::spawn(async move {
            let _owner = owner;
            work.await
        }))
    }

    pub fn is_closed(&self) -> bool {
        self.stop.is_cancelled()
    }

    pub async fn cancelled(&self) {
        self.stop.cancelled().await;
    }

    pub fn close(&self) {
        let mut closed = self.closed.lock().expect("HTTP lifecycle lock poisoned");
        *closed = true;
        self.stop.cancel();
        self.tasks.close();
    }

    pub async fn drain(&self) {
        self.tasks.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn close_rejects_admission_and_drains_even_an_unpolled_spawn() {
        let jobs = HttpLifecycle::default();
        let (release, gate) = tokio::sync::oneshot::channel();
        let run = jobs
            .spawn(async move {
                gate.await.unwrap();
            })
            .unwrap();
        jobs.close();
        assert!(jobs.enter().is_none());
        assert!(jobs.spawn(async { panic!("closed job ran") }).is_none());
        let drain = jobs.drain();
        tokio::pin!(drain);
        assert!(futures_util::poll!(&mut drain).is_pending());
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), &mut drain)
            .await
            .unwrap();
        run.await.unwrap();
    }
}
