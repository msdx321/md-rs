use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::api::ApiState;

#[derive(Clone)]
pub(crate) struct Shutdown {
    token: CancellationToken,
    work: CancellationToken,
}

impl Shutdown {
    pub(crate) fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            work: CancellationToken::new(),
        }
    }

    pub(crate) fn with_work(&self, work: CancellationToken) -> Self {
        Self {
            token: self.token.clone(),
            work,
        }
    }

    pub(crate) fn cancel(&self) {
        self.token.cancel();
    }

    pub(crate) fn work_cancelled(&self) -> bool {
        self.work.is_cancelled()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.token.is_cancelled() || self.work.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        tokio::select! {
            _ = self.token.cancelled() => {},
            _ = self.work.cancelled() => {},
        }
    }
}

/// Wait while the web UI has paused downloads, but bail out immediately when
/// `shutdown` is tripped. Returns `false` if shutdown was requested.
pub(crate) async fn wait_paused(web_state: &ApiState, shutdown: &Shutdown) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = web_state.wait_if_paused() => !shutdown.is_cancelled(),
    }
}

/// Seconds to wait for a FLOOD_WAIT error, or `None` if `err` is not one.
///
/// Grammers renders flood waits as `... (value: 225)`; the raw Telegram
/// error type is `FLOOD_WAIT_N`. Defaults to 60 s when the number can't be
/// read, so we never retry too eagerly.
pub(crate) fn flood_wait_secs(err: &str) -> Option<u64> {
    if !err.contains("FLOOD_WAIT") {
        return None;
    }
    for needle in ["value:", "FLOOD_WAIT_"] {
        if let Some((_, rest)) = err.split_once(needle) {
            let digits: String = rest
                .trim_start()
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(n) = digits.parse::<u64>()
                && n > 0
            {
                return Some(n);
            }
        }
    }
    Some(60)
}

/// Sleep for `dur`, returning early (`true`) if `shutdown` is tripped.
pub(crate) async fn sleep_cancellable(shutdown: &Shutdown, dur: Duration) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(dur) => false,
    }
}
