//! File-level admission and ownership, independent of transport and checkpoints.
//!
//! Each engine owns a pool (not a cross-module file cap). Capacity is supplied at
//! admission: Telegram snapshots it per cycle; HTTP engines read live settings.
use std::collections::HashSet;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::sync::watch;

#[derive(Default)]
struct Active {
    count: usize,
    keys: HashSet<String>,
}

pub struct DownloadSlots {
    active: Mutex<Active>,
    changes: watch::Sender<()>,
}

impl DownloadSlots {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(Active::default()),
            changes: watch::channel(()).0,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }

    /// Wake waiters after eligibility or live capacity changes.
    pub fn notify(&self) {
        self.changes.send_replace(());
    }

    /// Wait without reserving capacity. `None` from `capacity` withdraws this
    /// request; zero waits. Duplicate keys wait for their current owner to drop.
    ///
    /// The policy runs under the ownership lock, so callers must use the same
    /// lock order (pool before task registry) for keyed registration. No FIFO
    /// order is promised. Dropping this future leaves no queued reservation.
    pub async fn acquire(
        self: &Arc<Self>,
        key: Option<&str>,
        mut capacity: impl FnMut() -> Option<usize>,
        stop: impl Future<Output = ()>,
    ) -> Option<DownloadSlot> {
        let mut changes = self.subscribe();
        let admission = async {
            loop {
                {
                    let mut active = self.active.lock().expect("downloads lock poisoned");
                    let limit = capacity()?;
                    if active.count < limit && key.is_none_or(|key| !active.keys.contains(key)) {
                        let key = key.map(str::to_owned);
                        if let Some(key) = &key {
                            active.keys.insert(key.clone());
                        }
                        active.count += 1;
                        return Some(DownloadSlot {
                            slots: Arc::clone(self),
                            key,
                        });
                    }
                }
                // The pool retains the sender throughout this wait.
                changes.changed().await.expect("slot sender is alive");
            }
        };
        tokio::select! {
            biased;
            _ = stop => None,
            slot = admission => slot,
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.active
            .lock()
            .expect("downloads lock poisoned")
            .keys
            .contains(key)
    }

    /// Keep the ownership check and registry edit atomic with admission. This
    /// preserves engines that refuse to replace a task while it is unwinding.
    pub fn while_idle<T>(&self, key: &str, edit: impl FnOnce() -> T) -> Option<T> {
        let active = self.active.lock().expect("downloads lock poisoned");
        if active.keys.contains(key) {
            return None;
        }
        Some(edit())
    }
}

/// Keep this lease through writer drain and module-specific cleanup. Telegram
/// also keeps it while paused; HTTP engines drop it after unwinding to cache.
#[must_use = "dropping the slot releases download ownership"]
pub struct DownloadSlot {
    slots: Arc<DownloadSlots>,
    key: Option<String>,
}

impl Drop for DownloadSlot {
    fn drop(&mut self) {
        {
            let mut active = self.slots.active.lock().expect("downloads lock poisoned");
            active.count -= 1;
            if let Some(key) = &self.key {
                active.keys.remove(key);
            }
        }
        self.slots.notify();
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures_util::poll;
    use tokio_util::sync::CancellationToken;

    use super::*;

    #[tokio::test]
    async fn release_wakes_waiter_without_exceeding_capacity() {
        let slots = Arc::new(DownloadSlots::new());
        let first = slots.acquire(None, || Some(1), pending()).await.unwrap();
        let second = slots.acquire(None, || Some(1), pending());
        tokio::pin!(second);
        assert!(poll!(&mut second).is_pending());
        assert_eq!(slots.active.lock().unwrap().count, 1);
        drop(first);
        let second = second.await.unwrap();
        assert_eq!(slots.active.lock().unwrap().count, 1);
        drop(second);
        assert_eq!(slots.active.lock().unwrap().count, 0);
    }

    #[tokio::test]
    async fn live_capacity_changes_do_not_evict_owners() {
        let slots = Arc::new(DownloadSlots::new());
        let capacity = AtomicUsize::new(1);
        let first = slots.acquire(None, || Some(1), pending()).await.unwrap();
        let second = slots.acquire(None, || Some(capacity.load(Ordering::Relaxed)), pending());
        tokio::pin!(second);
        assert!(poll!(&mut second).is_pending());
        capacity.store(2, Ordering::Relaxed);
        slots.notify();
        let second = second.await.unwrap();
        capacity.store(1, Ordering::Relaxed);
        slots.notify();
        let third = slots.acquire(None, || Some(capacity.load(Ordering::Relaxed)), pending());
        tokio::pin!(third);
        assert!(poll!(&mut third).is_pending());
        drop(first);
        assert!(poll!(&mut third).is_pending());
        drop(second);
        assert!(third.await.is_some());
    }

    #[tokio::test]
    async fn cancelled_and_dropped_waiters_leave_no_reservation() {
        let slots = Arc::new(DownloadSlots::new());
        let first = slots.acquire(None, || Some(1), pending()).await.unwrap();
        let cancel = CancellationToken::new();
        let waiter = slots.acquire(Some("cancelled"), || Some(1), cancel.cancelled());
        tokio::pin!(waiter);
        assert!(poll!(&mut waiter).is_pending());
        cancel.cancel();
        assert!(waiter.await.is_none());
        {
            let waiter = slots.acquire(Some("dropped"), || Some(1), pending());
            tokio::pin!(waiter);
            assert!(poll!(&mut waiter).is_pending());
        }
        drop(first);
        assert_eq!(slots.active.lock().unwrap().count, 0);
        assert!(!slots.contains("cancelled"));
        assert!(!slots.contains("dropped"));
        assert!(slots.acquire(None, || Some(1), pending()).await.is_some());
    }

    #[tokio::test]
    async fn stop_takes_precedence_over_ready_admission() {
        let slots = Arc::new(DownloadSlots::new());
        assert!(slots.acquire(None, || Some(1), async {}).await.is_none());
        assert_eq!(slots.active.lock().unwrap().count, 0);
    }

    #[tokio::test]
    async fn duplicate_key_and_registry_edit_wait_for_owner_cleanup() {
        let slots = Arc::new(DownloadSlots::new());
        let first = slots
            .acquire(Some("video"), || Some(2), pending())
            .await
            .unwrap();
        let duplicate = slots.acquire(Some("video"), || Some(2), pending());
        tokio::pin!(duplicate);
        assert!(poll!(&mut duplicate).is_pending());
        assert!(
            slots
                .while_idle("video", || panic!("active task replaced"))
                .is_none()
        );
        let other = slots
            .acquire(Some("other"), || Some(2), pending())
            .await
            .unwrap();
        drop(first);
        let duplicate = duplicate.await.unwrap();
        drop(other);
        assert!(slots.contains("video"));
        drop(duplicate);
        assert_eq!(slots.while_idle("video", || 42), Some(42));
    }

    #[tokio::test]
    async fn withdrawn_request_and_zero_capacity_are_distinct() {
        let slots = Arc::new(DownloadSlots::new());
        let eligible = AtomicUsize::new(1);
        let waiter = slots.acquire(
            None,
            || (eligible.load(Ordering::Relaxed) == 1).then_some(0),
            pending(),
        );
        tokio::pin!(waiter);
        assert!(poll!(&mut waiter).is_pending());
        eligible.store(0, Ordering::Relaxed);
        slots.notify();
        assert!(waiter.await.is_none());
        assert_eq!(slots.active.lock().unwrap().count, 0);
    }
}
