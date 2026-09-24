import { $, api, bindHistoryPagination, scheduleRender, bytes, reconcileRows, dateTime, label, esc, toast, runAction } from './shared.js';
import { createSnapshotRefresh, createTaskRefresh } from './snapshot-refresh.js';

// Shared video-library mechanics; listing, access, settings and progress models
// remain with each provider. Pending keys disable only the action being awaited.
export function createVideoLibrary({ apiBase, progress, detail, loadStatus }) {
  let tasks = [];
  const pendingTasks = new Map();
  let clearingFailed = false;
  const pendingHistory = new Set();
  const queueTasks = scheduleRender(renderTasks);
  const taskSnapshots = createTaskRefresh((signal) => api(`${apiBase}/tasks`, { signal }), (data) => {
    tasks = data;
    $('tasks-banner').textContent = '';
    queueTasks();
  }, (error) => { $('tasks-banner').textContent = 'Could not load tasks: ' + error.message; });

  function renderTasks() {
    tasks = tasks.filter((t) => !['completed', 'cancelled'].includes(t.state));
    $('btn-clear-failed').disabled = clearingFailed || !tasks.some((t) => t.state === 'failed');
    const active = tasks.filter((t) => !['completed', 'failed', 'skipped', 'cancelled'].includes(t.state)).length;
    $('task-count').textContent = active ? `(${active})` : '';
    $('stat-active').textContent = tasks.filter((t) => t.state === 'running').length;
    $('tasks-empty').style.display = tasks.length ? 'none' : 'block';
    const nextRows = document.createElement('tbody');
    nextRows.innerHTML = tasks.map((t) => {
      const terminal = ['completed', 'failed', 'skipped', 'cancelled'].includes(t.state);
      const pct = progress(t);
      const barClass = t.state === 'completed' ? 'done' : t.state === 'failed' ? 'fail' : '';
      const speed = t.speed_kbps > 0 && t.state === 'running' ? bytes(t.speed_kbps * 1024) + '/s' : '—';
      const description = detail(t);
      const disabled = (action) => pendingTasks.get(t.id)?.has(action) ? 'disabled' : '';
      return `<tr data-row-id="${esc(t.id)}">
        <td><span class="truncate" title="${esc(t.title || t.url)}">${esc(t.title || t.url)}</span></td>
        <td class="muted"><span class="truncate" title="${esc(t.source_url || 'Manual')}">${esc(t.source_url || 'Manual')}</span></td>
        <td><span class="tag ${t.state}">${label(t.state)}</span></td>
        <td><div class="bar ${barClass}" role="progressbar" aria-label="Download progress" aria-valuenow="${Math.round(pct)}" aria-valuemin="0" aria-valuemax="100"><i style="width:${pct.toFixed(1)}%"></i></div><span class="muted">${pct.toFixed(0)}%</span></td>
        <td class="muted">${speed}</td>
        <td class="muted"><span class="truncate" title="${esc(description)}">${esc(description)}</span></td>
        <td style="white-space:nowrap">
          ${t.state === 'running' || t.state === 'queued' ? `<button class="tiny" data-act="pause" data-id="${esc(t.id)}" ${disabled('pause')}>Pause</button>` : ''}
          ${['paused', 'failed', 'skipped'].includes(t.state) ? `<button class="tiny" data-act="resume" data-id="${esc(t.id)}" ${disabled('resume')}>Resume</button>` : ''}
          ${!terminal ? `<button class="tiny danger" data-act="cancel" data-id="${esc(t.id)}" ${disabled('cancel')}>Cancel</button>` : ''}
          ${terminal ? `<button class="tiny" data-act="dismiss" data-id="${esc(t.id)}" aria-label="Dismiss task" ${disabled('dismiss')}>✕</button>` : ''}
        </td>
      </tr>`;
    }).join('');
    reconcileRows($('tasks-body'), [...nextRows.children]);
  }

  // Invalidate on both sides: a GET started during the command is stale too.
  async function mutateTasks(action) {
    taskSnapshots.invalidate();
    try { return await action(); }
    finally { await taskSnapshots.refresh({ fresh: true }); }
  }

  async function taskAction(action, id) {
    let pending = pendingTasks.get(id);
    if (pending?.has(action)) return;
    if (!pending) { pending = new Set(); pendingTasks.set(id, pending); }
    pending.add(action);
    renderTasks();
    const encoded = encodeURIComponent(id);
    try {
      await mutateTasks(async () => {
        await api(action === 'dismiss' ? `${apiBase}/tasks/${encoded}` : `${apiBase}/tasks/${encoded}/${action}`,
          { method: action === 'dismiss' ? 'DELETE' : 'POST' });
        if (action === 'dismiss') {
          tasks = tasks.filter((t) => t.id !== id);
          renderTasks();
        }
      });
    } catch (e) { toast(e.message, 'err'); }
    finally {
      pending.delete(action);
      if (!pending.size) pendingTasks.delete(id);
      renderTasks();
    }
  }
  $('tasks-body').addEventListener('click', (event) => {
    const button = event.target.closest('button[data-act]');
    if (button && !button.disabled) taskAction(button.dataset.act, button.dataset.id);
  });
  $('btn-clear-failed').onclick = async () => {
    if (clearingFailed || $('btn-clear-failed').disabled) return;
    clearingFailed = true;
    renderTasks();
    try {
      await mutateTasks(async () => {
        const result = await api(`${apiBase}/tasks/failed`, { method: 'DELETE' });
        toast(`Cleared ${result.count} failed task(s).`, 'ok');
      });
    } catch (e) { toast(e.message, 'err'); }
    finally { clearingFailed = false; renderTasks(); }
  };

  function receiveTask(task) {
    taskSnapshots.receive(task);
    const index = tasks.findIndex((t) => t.id === task.id);
    if (index >= 0) tasks[index] = task; else tasks.unshift(task);
    queueTasks();
    if (task.state === 'completed') {
      loadStatus();
      // Invalidate even while hidden so an older history GET cannot win.
      historySnapshots.invalidate();
      if ($('history-tab').getAttribute('aria-selected') === 'true') loadHistory();
    }
  }

  const historyBody = $('history-body');
  const historyPager = bindHistoryPagination(renderHistoryPage);
  const historySnapshots = createSnapshotRefresh((signal) => api(`${apiBase}/history`, { signal }), (data) => {
    $('history-empty').style.display = data.records.length ? 'none' : 'block';
    historyPager.update(data.records);
  }, (error) => toast(error.message, 'err'));
  const loadHistory = (options) => historySnapshots.refresh(options);

  function updateHistoryButtons() {
    historyBody.querySelectorAll('button[data-forget]').forEach((button) => {
      button.disabled = pendingHistory.has(button.dataset.forget);
    });
  }
  function renderHistoryPage(records) {
    const rows = records.map((r) => {
      const row = document.createElement('tr');
      row.dataset.rowId = r.id;
      row.innerHTML = `
        <td><span class="truncate" title="${esc(r.path || r.title)}">${esc(r.title || r.url)}</span></td>
        <td class="muted">${r.rank != null ? '#' + r.rank : '—'}</td>
        <td class="muted">${bytes(r.size)}</td>
        <td class="muted">${dateTime(r.finished_at)}</td>
        <td><span class="tag ${r.status === 'completed' ? 'completed' : 'failed'}">${esc(label(r.status))}</span></td>
        <td><button class="tiny" data-forget="${esc(r.id)}">Forget</button></td>`;
      return row;
    });
    reconcileRows(historyBody, rows);
    updateHistoryButtons();
  }
  async function deleteHistory(path, message) {
    historySnapshots.invalidate();
    try {
      await api(path, { method: 'DELETE' });
      if (message) toast(message, 'ok');
    } finally {
      // Status is auxiliary; a stalled status read must not lock history actions.
      void loadStatus();
      await historySnapshots.refresh({ fresh: true });
    }
  }
  historyBody.addEventListener('click', async (event) => {
    const button = event.target.closest('button[data-forget]');
    if (!button || button.disabled || pendingHistory.has(button.dataset.forget)) return;
    const id = button.dataset.forget;
    pendingHistory.add(id);
    updateHistoryButtons();
    try {
      await deleteHistory(`${apiBase}/history/${encodeURIComponent(id)}`, 'Removed from history — it can be downloaded again');
    } catch (e) { toast(e.message, 'err'); }
    finally { pendingHistory.delete(id); updateHistoryButtons(); }
  });
  $('btn-reload-history').onclick = loadHistory;
  $('btn-clear-history').onclick = () => runAction($('btn-clear-history'), async () => {
    if (!confirm('Clear retained history? Downloaded files are kept, but videos in this history become downloadable again.')) return;
    await deleteHistory(`${apiBase}/history`);
  });

  return { loadTasks: taskSnapshots.refresh, loadHistory, receiveTask, mutateTasks };
}
