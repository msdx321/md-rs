//! One bandwidth budget shared by every download, with optional module ceilings.
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::configuration::app::Config;

#[derive(Clone, Copy)]
pub enum DownloadModule {
    Telegram = 1,
    Jav = 2,
}

pub struct DownloadLimiter {
    settings: watch::Receiver<Config>,
    buckets: Mutex<[Bucket; 3]>,
}

struct Bucket {
    rate: u64,
    tokens: f64,
    updated: Instant,
}

impl Bucket {
    fn refill(&mut self, rate: u64, now: Instant) {
        // A changed limit takes effect immediately, without retaining credit
        // earned under a faster limit (or while the limiter was disabled).
        self.tokens = if rate == self.rate {
            (self.tokens + now.duration_since(self.updated).as_secs_f64() * rate as f64)
                .min(quantum(rate) as f64)
        } else {
            0.0
        };
        self.rate = rate;
        self.updated = now;
    }
}

fn quantum(rate: u64) -> u64 {
    (rate / 10).clamp(1, 64 * 1024)
}

impl DownloadLimiter {
    pub fn new(settings: watch::Receiver<Config>) -> Self {
        Self {
            settings,
            buckets: Mutex::new(std::array::from_fn(|_| Bucket {
                rate: 0,
                tokens: 0.0,
                updated: Instant::now(),
            })),
        }
    }

    pub fn limit(&self, module: DownloadModule) -> Option<u64> {
        let rates = rates(&self.settings.borrow());
        [rates[0], rates[module as usize]]
            .into_iter()
            .filter(|&rate| rate > 0)
            .min()
    }

    /// Charge both applicable budgets atomically. No lock or future bandwidth
    /// reservation is held while waiting, so cancellation cannot block peers.
    pub async fn acquire(&self, module: DownloadModule, mut bytes: usize) {
        let mut settings = self.settings.clone();
        while bytes > 0 {
            let delay = {
                let mut buckets = self.buckets.lock().expect("bandwidth lock poisoned");
                let rates = rates(&settings.borrow_and_update());
                let indices = [0, module as usize];
                let Some(rate) = indices.iter().map(|&i| rates[i]).filter(|&r| r > 0).min() else {
                    return;
                };
                let amount = (bytes as u64).min(quantum(rate)) as usize;
                let now = Instant::now();
                let mut wait = 0.0_f64;
                for &i in &indices {
                    buckets[i].refill(rates[i], now);
                    if rates[i] > 0 {
                        wait = wait.max((amount as f64 - buckets[i].tokens) / rates[i] as f64);
                    }
                }
                if wait <= 0.0 {
                    for &i in &indices {
                        if rates[i] > 0 {
                            buckets[i].tokens -= amount as f64;
                        }
                    }
                    bytes -= amount;
                    Duration::ZERO
                } else {
                    Duration::from_secs_f64(wait)
                }
            };
            if !delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {},
                    changed = settings.changed() => {
                        if changed.is_err() {
                            tokio::time::sleep(delay).await;
                        }
                    },
                }
            }
        }
    }
}

fn rates(config: &Config) -> [u64; 3] {
    [
        config.download_limit_mb_per_sec,
        config.telegram_download_limit_mb_per_sec,
        config.jav_download_limit_mb_per_sec,
    ]
    .map(|rate| (rate * 1_000_000.0).round() as u64)
}
