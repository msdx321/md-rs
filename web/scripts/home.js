import { $, api, connectEvents, pollWhenVisible, scheduleRender, reconcileRows, bytes, historyPeriod, dateTime, label, sizeLabel } from './shared.js';
import { createSnapshotRefresh, createTaskRefresh } from './snapshot-refresh.js';

let telegram = null;
const libraries = Object.entries({ jav: 'JAV', p91: '91Porn', pornhub: 'Pornhub' })
  .map(([id, name]) => ({ id, name, status: null, tasks: null, history: null, run: {} }));
let telegramHistory = null;
let telegramHistoryRevision = null;
// Only the newest entries are shown, so each module sends just that many.
const RECENT_LIMIT = 10;
const terminal = new Set(['completed', 'failed', 'skipped', 'cancelled']);
const queueSummary = scheduleRender(renderSummary);
const queueActivity = scheduleRender(renderActivity);
const queueHistory = scheduleRender(renderHistory);
const renderedLists = new Map();
const activityRows = new Map();
let lastTelegramRun;

function telegramChatName(snapshot, chatId) {
  const names = snapshot.channel_names || {};
  const id = String(chatId);
  const key = id.replace(/^@/, '').toLowerCase();
  return (Object.hasOwn(names, id) && names[id])
    || (Object.hasOwn(names, key) && names[key])
    || id;
}

function renderTelegramRun(snapshot) {
  const scan = snapshot.scan;
  const run = scan?.last_run;
  const status = $('telegram-status');
  status.hidden = Boolean(run) && snapshot.login.step === 'ready' && !scan.running
    && !snapshot.paused && !snapshot.cancelling
    && ['Ready for downloads', 'running', 'Schedule disabled'].includes(snapshot.status);
  status.textContent = snapshot.login.step !== 'ready' ? snapshot.login.message
    : snapshot.cancelling ? 'Cancelling' : snapshot.paused ? 'Paused'
    : scan?.running ? 'Scan running'
    : label(snapshot.status);
  const signature = JSON.stringify([run, snapshot.channel_names]);
  if (signature === lastTelegramRun) return;
  lastTelegramRun = signature;
  $('telegram-last-run').hidden = !run;
  if (!run) return;
  const totals = run.chats.reduce((sum, chat) => {
    for (const key of ['downloaded', 'scanned', 'failed', 'skipped']) sum[key] += chat[key];
    return sum;
  }, { downloaded: 0, scanned: 0, failed: 0, skipped: 0 });
  const incomplete = run.chats.filter(chat => chat.completed).length < run.total_chats;
  const outcome = run.stopped ? ' · Stopped' : run.error || run.chats.some(chat => chat.error) || totals.failed ? ' · With errors' : incomplete ? ' · Incomplete' : '';
  $('telegram-run-label').textContent = `Last ${run.trigger} run${outcome}`;
  $('telegram-run-counts').replaceChildren(...[
    [`${totals.downloaded} downloaded`, 'completed'],
    [`${totals.scanned} scanned`, ''],
    [`${totals.failed} failed`, totals.failed ? 'failed' : ''],
    [`${totals.skipped} skipped`, ''],
  ].map(([text, kind]) => {
    const count = document.createElement('span');
    count.className = `tag ${kind}`;
    count.textContent = text;
    return count;
  }));
  const details = [
    `Started ${dateTime(run.started_at)} · Finished ${dateTime(run.finished_at)}`,
    `${run.chats.filter(chat => chat.completed).length} / ${run.total_chats} chats scanned`,
    ...run.chats.map(chat => `${telegramChatName(snapshot, chat.chat_id)}: ${chat.downloaded} downloaded, ${chat.scanned} scanned, ${chat.failed} failed, ${chat.skipped} skipped${chat.error ? ` · ${chat.error}` : !chat.completed ? ' · Interrupted' : ''}`),
  ];
  if (!run.total_chats) details.push('No subscribed chats to scan');
  if (run.error) details.push(run.error);
  if (run.stopped) details.push('Run stopped before completion');
  $('telegram-run-breakdown').replaceChildren(...details.map(text => {
    const item = document.createElement('li');
    item.textContent = text;
    return item;
  }));
}

function renderRun(prefix, scheduler, signature) {
  const current = JSON.stringify([scheduler.running, scheduler.last_result]);
  if (current === signature.value) return;
  signature.value = current;
  const result = scheduler.last_result || '';
  // The scheduler also sends free-form progress and error messages.
  const match = result.match(/^(manual|scheduled): (\d+)\/(\d+) completed, (\d+) attempted, (\d+) (failed(?: candidates)?), (\d+) skipped; ([\s\S]*)$/);
  const structured = !scheduler.running && match !== null;
  $(`${prefix}-run-counts`).hidden = !structured;
  $(`${prefix}-run-details`).hidden = !structured;
  if (!structured) {
    $(`${prefix}-status`).textContent = scheduler.running ? 'Scheduled job running' : result || 'Ready for downloads';
    return;
  }
  const [, trigger, completed, target, attempted, failed, failureLabel, skipped, details] = match;
  const stopped = details.endsWith('stopped by user');
  $(`${prefix}-status`).textContent = `Last ${trigger} run${stopped ? ' · Stopped' : Number(completed) < Number(target) ? ' · Incomplete' : ''}`;
  const counts = [
    [`${completed} / ${target} completed`, 'completed'],
    [`${attempted} attempted`, ''],
    [`${failed} ${failureLabel}`, Number(failed) ? 'failed' : ''],
    [`${skipped} skipped`, ''],
  ];
  $(`${prefix}-run-counts`).replaceChildren(...counts.map(([text, kind]) => {
    const count = document.createElement('span');
    count.className = `tag ${kind}`;
    count.textContent = text;
    return count;
  }));
  $(`${prefix}-run-breakdown`).replaceChildren(...details.split('; ').map((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    return item;
  }));
}

function renderSummary() {
  $('overview-active').textContent = telegram && libraries.every(lib => lib.status)
    ? libraries.reduce((sum, lib) => sum + lib.status.active_tasks, telegram.active_count) : '—';
  if (telegram) {
    historyPeriod("telegram", telegram.history_retention_days);
    $('overview-telegram-size').textContent = sizeLabel(telegram.downloaded_bytes);
    $('telegram-saved').textContent = sizeLabel(telegram.downloaded_bytes);
    renderTelegramRun(telegram);
    $('telegram-next-run').textContent = telegram.paused ? 'Paused' : telegram.next_run_at ? dateTime(telegram.next_run_at) : '—';
    $('telegram-transfers').textContent = telegram.paused ? 'Paused' : `${telegram.active_count} active`;
  }
  for (const { id, status, run } of libraries) {
    if (!status) continue;
    historyPeriod(id, status.history_retention_days);
    const size = bytes(status.downloaded_bytes);
    $(`overview-${id}-size`).textContent = size;
    $(`${id}-saved`).textContent = size;
    $(`${id}-transfers`).textContent = `${status.active_tasks} active`;
    renderRun(id, status.scheduler, run);
    $(`${id}-next-run`).textContent = !status.scheduler.enabled ? 'Disabled'
      : status.scheduler.next_run_at ? dateTime(status.scheduler.next_run_at) : 'Waiting for next run';
  }
}

// Queued tasks have neither bytes nor speed yet; show only what is known.
function libraryTransfer(task) {
  if (task.state === 'paused') return 'Paused';
  if (task.state === 'running') return `${bytes(task.downloaded_bytes)} · ${bytes(task.speed_kbps * 1024)}/s`;
  return task.downloaded_bytes ? bytes(task.downloaded_bytes) : '';
}

// Adapt provider snapshots into one presentation model at the UI boundary.
function activeDownloads() {
  return [
    ...(telegram?.active || []).map((item) => ({
      id: `telegram:${item.msg_id}:${item.path}`,
      source: 'Telegram', name: item.file_name,
      detail: `${item.source_name ? `${item.source_name} · ` : ''}${telegram.paused ? 'Paused' : `${sizeLabel(item.downloaded)} / ${sizeLabel(item.total)}`}`,
      progress: item.percent, status: telegram.paused ? 'Paused' : sizeLabel(item.speed),
    })),
    ...libraries.flatMap(({ id, name, tasks }) => (tasks || [])
      .filter(task => !terminal.has(task.state))
      .map(task => ({
        id: `${id}:${task.id}`, source: name, name: task.title || task.id,
        detail: `${label(task.state)} · ${label(task.phase)}`,
        progress: id === 'jav' && task.total_segments ? task.done_segments / task.total_segments * 100
          : task.total_bytes ? task.downloaded_bytes / task.total_bytes * 100 : null,
        status: libraryTransfer(task),
      }))),
  ];
}

function recentDownloads() {
  return [
    ...(telegramHistory || []).map((item) => ({
      id: `telegram:${item.id}`,
      source: 'Telegram', name: item.file_name, detail: `${sizeLabel(item.size)} · Completed`, date: new Date(item.completed_at).getTime(),
    })),
    ...libraries.flatMap(({ id, name, history }) => (history || []).map(item => ({
      id: `${id}:${item.id}`, source: name, name: item.title || item.id,
      detail: `${bytes(item.size)} · ${label(item.status)}`, date: Date.parse(item.finished_at),
    }))),
  ].sort((a, b) => b.date - a.date).slice(0, RECENT_LIMIT);
}

function renderRows(target, rows, empty) {
  const signature = JSON.stringify([rows, empty]);
  if (renderedLists.get(target) === signature) return;
  renderedLists.set(target, signature);
  if (!rows.length) {
    const text = document.createElement('p');
    text.className = 'empty';
    text.textContent = empty;
    target.replaceChildren(text);
    return;
  }
  const nextRows = rows.map(item => {
    const row = document.createElement('article');
    row.className = 'overview-row';
    row.dataset.rowId = item.id;
    const main = document.createElement('div');
    const title = document.createElement('h3');
    title.className = 'overview-name';
    title.textContent = item.name;
    const detail = document.createElement('div');
    detail.className = 'overview-detail';
    detail.textContent = `${item.source} · ${item.detail}`;
    main.append(title, detail);
    const status = document.createElement('div');
    status.className = 'overview-progress';
    if ('progress' in item) {
      const percent = Number.isFinite(item.progress) ? Math.min(100, Math.max(0, item.progress)) : null;
      const meta = document.createElement('div');
      meta.className = 'progress-meta';
      const value = document.createElement('span');
      value.className = 'progress-value';
      value.textContent = percent === null ? 'Preparing' : `${Math.round(percent)}%`;
      const speed = document.createElement('span');
      speed.textContent = item.status;
      meta.append(value, speed);
      const progress = document.createElement('div');
      progress.className = percent === null ? 'bar indeterminate' : 'bar';
      progress.setAttribute('role', 'progressbar');
      progress.setAttribute('aria-label', `${item.source}: ${item.name}`);
      progress.setAttribute('aria-valuemin', '0');
      progress.setAttribute('aria-valuemax', '100');
      const fill = document.createElement('i');
      if (percent !== null) {
        progress.setAttribute('aria-valuenow', String(Math.round(percent)));
        fill.style.width = `${percent}%`;
      }
      progress.append(fill);
      status.append(meta, progress);
    } else {
      const time = document.createElement('time');
      if (Number.isFinite(item.date)) {
        time.dateTime = new Date(item.date).toISOString();
        time.textContent = dateTime(item.date);
      }
      status.append(time);
    }
    row.append(main, status);
    return row;
  });
  reconcileRows(target, nextRows);
}

function renderActivity() {
  const downloads = activeDownloads();
  const activeIds = new Set(downloads.map(item => item.id));
  for (const id of activityRows.keys()) {
    if (!activeIds.has(id)) activityRows.delete(id);
  }
  // Preserve first-seen order across progress events and reordered snapshots.
  // Updating a Map entry keeps its position; only new tasks go at the end.
  for (const item of downloads) activityRows.set(item.id, item);
  const rows = [...activityRows.values()];
  $('activity-count').textContent = `${rows.length} active`;
  renderRows($('overview-downloads'), rows, telegram && libraries.every(lib => lib.tasks) ? 'All caught up. New downloads will appear here.' : 'Waiting for all modules…');
}
function renderHistory() {
  renderRows($('overview-history'), recentDownloads(), telegramHistory && libraries.every(lib => lib.history) ? 'No downloads recorded yet' : 'Waiting for all modules…');
}
const taskErrors = new Set();
const historyErrors = new Set();
function refreshError(errors, provider, failed, target, message) {
  if (failed) errors.add(provider); else errors.delete(provider);
  $(target).hidden = !errors.size;
  if (errors.size) $(target).textContent = message;
}
const telegramHistoryResource = createSnapshotRefresh((signal) => api(`/telegram/api/history?limit=${RECENT_LIMIT}`, { signal }), (data) => {
  telegramHistory = data.records;
  refreshError(historyErrors, 'telegram', false, 'history-error', 'Download history is unavailable. Retrying automatically.');
  queueHistory();
}, () => {
  refreshError(historyErrors, 'telegram', true, 'history-error', 'Download history is unavailable. Retrying automatically.');
  queueHistory();
});
connectEvents('/telegram/events', {
  message(event) {
    telegram = JSON.parse(event.data);
    if (telegram.history_revision !== telegramHistoryRevision) {
      telegramHistoryRevision = telegram.history_revision;
      telegramHistoryResource.refresh({ fresh: true });
    }
    queueSummary(); queueActivity();
  },
}, $('telegram-connection'));
for (const lib of libraries) {
  const { id } = lib;
  const taskError = failed => refreshError(taskErrors, id, failed, 'activity-error',
    'Download task status is unavailable. Retrying automatically.');
  const historyError = failed => refreshError(historyErrors, id, failed, 'history-error',
    'Download history is unavailable. Retrying automatically.');
  const tasks = createTaskRefresh(signal => api(`/${id}/api/tasks`, { signal }), data => {
    lib.tasks = data;
    taskError(false);
    queueActivity();
  }, () => { taskError(true); queueActivity(); });
  const history = createSnapshotRefresh(signal => api(`/${id}/api/history?limit=${RECENT_LIMIT}`, { signal }), data => {
    lib.history = data.records;
    historyError(false);
    queueHistory();
  }, () => { historyError(true); queueHistory(); });

  function resync() {
    tasks.refresh({ fresh: true });
    history.refresh({ fresh: true });
  }
  let revision = null;
  connectEvents(`/${id}/api/events`, {
    status(event) {
      lib.status = JSON.parse(event.data);
      if (revision !== lib.status.library_revision) {
        revision = lib.status.library_revision;
        resync();
      }
      queueSummary();
    },
    task(event) {
      const task = JSON.parse(event.data);
      tasks.receive(task);
      if (lib.tasks) {
        lib.tasks = lib.tasks.filter(item => item.id !== task.id);
        lib.tasks.push(task);
        queueActivity();
      }
    },
    open() { revision = null; },
    resync,
  }, $(`${id}-connection`));
  lib.taskResource = tasks;
  lib.historyResource = history;
}

// Revisions reconcile cross-tab deletions; polling retries only failed reads.
pollWhenVisible(() => {
  const retries = [];
  for (const lib of libraries) {
    if (taskErrors.has(lib.id)) retries.push(lib.taskResource.refresh());
    if (historyErrors.has(lib.id)) retries.push(lib.historyResource.refresh());
  }
  if (historyErrors.has('telegram')) retries.push(telegramHistoryResource.refresh());
  return Promise.all(retries);
}, 30000);
