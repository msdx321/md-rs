//! Live tasks, download ownership and event delivery for progressive video downloads.
use super::download_slots::{DownloadSlot, DownloadSlots};
use super::http_lifecycle::HttpLifecycle;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::pending;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    Running,
    Paused,
    Completed,
    Failed,
    Skipped,
    Cancelled,
}

/// Live state of one video download, mirrored to the UI over SSE.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub url: String,
    pub title: String,
    #[serde(default)]
    pub vid: Option<String>,
    #[serde(default)]
    pub source_url: String,
    pub state: TaskState,
    /// Human-readable stage: `resolving`, `downloading`, …
    pub phase: String,
    pub done_segments: usize,
    pub total_segments: usize,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub speed_kbps: f64,
    pub message: String,
    pub path: String,
    pub updated_at: String,
}

impl TaskInfo {
    pub fn new(id: &str, url: &str) -> Self {
        Self {
            id: id.to_string(),
            url: url.to_string(),
            title: String::new(),
            vid: None,
            source_url: String::new(),
            state: TaskState::Queued,
            phase: "queued".into(),
            done_segments: 0,
            total_segments: 0,
            downloaded_bytes: 0,
            total_bytes: 0,
            speed_kbps: 0.0,
            message: String::new(),
            path: String::new(),
            updated_at: now_rfc3339(),
        }
    }

    /// True when the task is in a state that will not progress on its own.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state,
            TaskState::Completed | TaskState::Failed | TaskState::Skipped | TaskState::Cancelled
        )
    }
}

#[derive(Default)]
struct TaskRegistry {
    tasks: HashMap<String, TaskInfo>,
    committing: HashSet<String>,
    pausing: HashSet<String>,
}

pub struct VideoTasks {
    library_revision: AtomicU64,
    tasks: Mutex<TaskRegistry>,
    downloads: Arc<DownloadSlots>,
    pub(crate) jobs: HttpLifecycle,
    events: broadcast::Sender<Arc<str>>,
}

impl VideoTasks {
    pub fn new() -> Self {
        Self {
            library_revision: AtomicU64::new(0),
            tasks: Mutex::default(),
            downloads: Arc::new(DownloadSlots::new()),
            jobs: HttpLifecycle::default(),
            events: broadcast::channel(256).0,
        }
    }

    pub fn notify(&self) {
        self.downloads.notify();
    }

    pub async fn download_slot(
        &self,
        id: &str,
        capacity: impl Fn() -> usize,
    ) -> Option<DownloadSlot> {
        self.downloads
            .acquire(
                Some(id),
                || {
                    (!self.jobs.is_closed() && self.task_state(id) == Some(TaskState::Queued))
                        .then(&capacity)
                },
                pending(),
            )
            .await
    }

    pub fn prune(&self, days: u32) {
        let cutoff = chrono::Utc::now() - chrono::Duration::days(i64::from(days));
        self.tasks
            .lock()
            .expect("tasks lock poisoned")
            .tasks
            .retain(|_, task| {
                !task.is_terminal()
                    || chrono::DateTime::parse_from_rfc3339(&task.updated_at)
                        .is_ok_and(|time| time > cutoff)
            });
        self.library_changed();
    }

    pub fn library_revision(&self) -> u64 {
        self.library_revision.load(Ordering::Relaxed)
    }

    pub fn library_changed(&self) {
        self.library_revision.fetch_add(1, Ordering::Relaxed);
        self.downloads.notify();
    }

    pub fn tasks(&self) -> Vec<TaskInfo> {
        self.tasks_matching(|_| true)
    }

    pub fn visible_tasks(&self) -> Vec<TaskInfo> {
        self.tasks_matching(|task| task.state != TaskState::Completed)
    }

    fn tasks_matching(&self, include: impl Fn(&TaskInfo) -> bool) -> Vec<TaskInfo> {
        let guard = self.tasks.lock().expect("task lock poisoned");
        let mut out: Vec<TaskInfo> = guard
            .tasks
            .values()
            .filter(|task| include(task))
            .cloned()
            .collect();
        drop(guard);
        out.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
        out
    }

    pub fn task(&self, id: &str) -> Option<TaskInfo> {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .tasks
            .get(id)
            .cloned()
    }

    pub fn task_state(&self, id: &str) -> Option<TaskState> {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .tasks
            .get(id)
            .map(|task| task.state)
    }

    /// Pause is an unwind request before the registry publishes Paused. Read
    /// both fields under one lock without cloning URLs, titles or messages.
    pub fn task_control(&self, id: &str) -> Option<TaskState> {
        let registry = self.tasks.lock().expect("task lock poisoned");
        registry.tasks.get(id).map(|task| {
            if registry.pausing.contains(id)
                || (task.phase == "pausing" && task.state == TaskState::Running)
            {
                TaskState::Paused
            } else {
                task.state
            }
        })
    }

    pub fn running_task_count(&self) -> usize {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .tasks
            .values()
            .filter(|task| task.state == TaskState::Running)
            .count()
    }

    /// Apply `f` to a task and broadcast the result.
    pub fn update_task(&self, id: &str, f: impl FnOnce(&mut TaskInfo)) {
        let mut state_changed = false;
        let updated = {
            let mut guard = self.tasks.lock().expect("task lock poisoned");
            match guard.tasks.get_mut(id) {
                Some(task) => {
                    // A "pausing" phase is a control change too (see task_control).
                    let control = |task: &TaskInfo| (task.state, task.phase == "pausing");
                    let previous = control(task);
                    f(task);
                    state_changed = previous != control(task);
                    task.updated_at = now_rfc3339();
                    Some(task.clone())
                }
                None => None,
            }
        };
        if let Some(task) = updated {
            self.publish(&task);
        }
        if state_changed {
            self.downloads.notify();
        }
    }

    /// Register a new task. Returns false when one is already active for `id`.
    pub fn register_task(&self, info: TaskInfo) -> bool {
        let id = info.id.clone();
        self.downloads
            .while_idle(&id, || {
                let mut guard = self.tasks.lock().expect("task lock poisoned");
                if self.jobs.is_closed() {
                    return false;
                }
                if let Some(existing) = guard.tasks.get(&info.id)
                    && !existing.is_terminal()
                    && existing.state != TaskState::Paused
                {
                    return false;
                }
                self.publish(&info);
                guard.pausing.remove(&info.id);
                guard.tasks.insert(info.id.clone(), info);
                true
            })
            .unwrap_or(false)
    }

    pub fn is_downloading(&self, id: &str) -> bool {
        self.downloads.contains(id)
    }

    /// Remove only failed entries under one lock so a concurrent retry is preserved.
    pub fn clear_failed_tasks(&self) -> usize {
        let mut tasks = self.tasks.lock().expect("task lock poisoned");
        let before = tasks.tasks.len();
        tasks
            .tasks
            .retain(|_, task| task.state != TaskState::Failed);
        let removed = before - tasks.tasks.len();
        drop(tasks);
        if removed > 0 {
            self.library_changed();
        }
        removed
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<str>> {
        self.events.subscribe()
    }

    fn publish(&self, task: &TaskInfo) {
        if self.events.receiver_count() == 0 {
            return;
        }
        match serde_json::to_string(task) {
            Ok(json) => {
                let _ = self.events.send(json.into());
            }
            Err(error) => log::warn!("cannot serialize task {}: {error}", task.id),
        }
    }

    /// Wakes after every task state transition (and other slot changes), but
    /// not after progress-only updates.
    pub fn subscribe_status(&self) -> watch::Receiver<()> {
        self.downloads.subscribe()
    }

    /// Forget a finished task so it disappears from the UI.
    pub fn drop_task(&self, id: &str) -> bool {
        let removed = self
            .downloads
            .while_idle(id, || {
                let mut registry = self.tasks.lock().expect("task lock poisoned");
                if registry
                    .tasks
                    .get(id)
                    .is_none_or(|task| !task.is_terminal())
                {
                    return false;
                }
                registry.pausing.remove(id);
                registry.tasks.remove(id).is_some()
            })
            .unwrap_or(false);
        if removed {
            self.library_changed();
        }
        removed
    }

    /// Preserve p91's Running/pausing unwind contract while serializing stops
    /// against irreversible publication and history commits.
    pub fn stop_task(&self, id: &str, cancel: bool) -> bool {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        if registry.committing.contains(id) {
            return false;
        }
        let Some(task) = registry.tasks.get_mut(id) else {
            return false;
        };
        if if cancel {
            task.is_terminal()
        } else {
            !matches!(task.state, TaskState::Running | TaskState::Queued)
        } {
            return false;
        }
        if cancel {
            task.phase = if task.state == TaskState::Queued {
                "cancelled"
            } else {
                "cancelling"
            }
            .into();
            task.state = TaskState::Cancelled;
            task.message = "cancelled by user".into();
        } else {
            if task.state == TaskState::Queued {
                task.state = TaskState::Paused;
            }
            task.phase = if task.state == TaskState::Paused {
                "paused"
            } else {
                "pausing"
            }
            .into();
            task.message = "paused by user".into();
        }
        task.updated_at = now_rfc3339();
        self.publish(task);
        if cancel {
            registry.pausing.remove(id);
        } else {
            registry.pausing.insert(id.to_string());
        }
        drop(registry);
        self.downloads.notify();
        true
    }

    pub(crate) fn begin_terminal_commit(&self, id: &str) -> bool {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        if registry.committing.contains(id) {
            return true;
        }
        if registry.pausing.contains(id)
            || registry
                .tasks
                .get(id)
                .is_none_or(|task| task.state != TaskState::Running || task.phase == "pausing")
        {
            return false;
        }
        registry.committing.insert(id.to_string());
        true
    }

    pub(crate) fn end_terminal_commit(&self, id: &str) {
        self.tasks
            .lock()
            .expect("task lock poisoned")
            .committing
            .remove(id);
    }

    pub fn pause_for_shutdown(&self) {
        let mut registry = self.tasks.lock().expect("task lock poisoned");
        let TaskRegistry {
            tasks,
            committing,
            pausing,
        } = &mut *registry;
        for (id, task) in tasks {
            if !committing.contains(id)
                && matches!(task.state, TaskState::Running | TaskState::Queued)
            {
                task.phase = if task.state == TaskState::Queued {
                    "paused"
                } else {
                    "pausing"
                }
                .into();
                if task.state == TaskState::Queued {
                    task.state = TaskState::Paused;
                }
                task.message = "service shutting down; partials retained".into();
                pausing.insert(id.clone());
                self.publish(task);
            }
        }
        drop(registry);
        self.downloads.notify();
    }
}

pub fn now_rfc3339() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}
