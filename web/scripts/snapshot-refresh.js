// One GET per resource at a time. Mutations invalidate older snapshots;
// callers awaiting a refresh also wait for the one coalesced follow-up GET.
export function createSnapshotRefresh(read, apply, onError) {
  let revision = 0;
  let pending;
  function invalidate() { revision++; }
  function refresh({ fresh = false } = {}) {
    if (fresh) invalidate();
    if (!pending) {
      pending = (async () => {
        try {
          let started;
          do {
            started = revision;
            const controller = new AbortController();
            let timer;
            const timeout = new Promise((_, reject) => {
              timer = setTimeout(() => {
                const error = new Error('Snapshot request timed out');
                controller.abort(error);
                reject(error);
              }, 15000);
            });
            try {
              const value = await Promise.race([
                Promise.resolve().then(() => read(controller.signal)), timeout,
              ]);
              if (started === revision) apply(value);
            } catch (error) {
              if (controller.signal.aborted || started === revision) onError(error);
              // Release waiting actions even if events kept invalidating this read.
              // A later refresh can retry; a late response cannot be applied.
              if (controller.signal.aborted) break;
            } finally { clearTimeout(timer); }
          } while (started !== revision);
        } finally { pending = undefined; }
      })();
    }
    return pending;
  }
  return { refresh, invalidate };
}

// Task progress may arrive faster than a GET can finish. Overlay those pushes
// instead of retrying on every event (which would starve the initial snapshot).
export function createTaskRefresh(read, apply, onError) {
  let updates;
  const snapshots = createSnapshotRefresh((signal) => {
    updates = new Map();
    return read(signal);
  }, (tasks) => {
    const merged = new Map(tasks.map((task) => [task.id, task]));
    for (const [id, task] of updates) merged.set(id, task);
    updates = undefined;
    apply([...merged.values()]);
  }, (error) => {
    updates = undefined;
    onError(error);
  });
  return {
    ...snapshots,
    receive(task) { updates?.set(task.id, task); },
  };
}
