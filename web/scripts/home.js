import { $, api, connectEvents, pollWhenVisible, scheduleRender, reconcileRows, bytes, historyPeriod, dateTime, label, sizeLabel } from './shared.js';
import { createSnapshotRefresh, createTaskRefresh } from './snapshot-refresh.js';

let telegram = null;
let jav = null;
let p91 = null;
let tasks = null;
let p91Tasks = null;
let history = null;
let p91History = null;
const terminal = new Set(['completed', 'failed', 'cancelled']);
const queueSummary = scheduleRender(renderSummary);
const queueActivity = scheduleRender(renderActivity);
const queueHistory = scheduleRender(renderHistory);
const renderedLists = new Map();
const activityRows = new Map();
const javRun = { value: undefined };
const p91Run = { value: undefined };
let lastTelegramRun;

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
  const signature = JSON.stringify(run);
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
    ...run.chats.map(chat => `${chat.chat_id}: ${chat.downloaded} downloaded, ${chat.scanned} scanned, ${chat.failed} failed, ${chat.skipped} skipped${chat.error ? ` · ${chat.error}` : !chat.completed ? ' · Interrupted' : ''}`),
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
  const match = result.match(/^(manual|scheduled): (\d+)\/(\d+) completed, (\d+) attempted, (\d+) failed, (\d+) skipped; ([\s\S]*)$/);
  const structured = !scheduler.running && match !== null;
  $(`${prefix}-run-counts`).hidden = !structured;
  $(`${prefix}-run-details`).hidden = !structured;
  if (!structured) {
    $(`${prefix}-status`).textContent = scheduler.running ? 'Scheduled job running' : result || 'Ready for downloads';
    return;
  }
  const [, trigger, completed, target, attempted, failed, skipped, details] = match;
  const stopped = details.endsWith('stopped by user');
  $(`${prefix}-status`).textContent = `Last ${trigger} run${stopped ? ' · Stopped' : Number(completed) < Number(target) ? ' · Incomplete' : ''}`;
  const counts = [
    [`${completed} / ${target} completed`, 'completed'],
    [`${attempted} attempted`, ''],
    [`${failed} failed`, Number(failed) ? 'failed' : ''],
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
  $('overview-active').textContent = telegram && jav && p91 ? telegram.active_count + jav.active_tasks + p91.active_tasks : '—';
  if (telegram) {
    historyPeriod("telegram", telegram.history_retention_days);
    $('overview-telegram-size').textContent = sizeLabel(telegram.downloaded_bytes);
    $('telegram-saved').textContent = sizeLabel(telegram.downloaded_bytes);
    renderTelegramRun(telegram);
    $('telegram-next-run').textContent = telegram.paused ? 'Paused' : telegram.next_run_at ? dateTime(telegram.next_run_at) : '—';
    $('telegram-transfers').textContent = telegram.paused ? 'Paused' : `${telegram.active_count} active`;
  }
  if (jav) {
    historyPeriod("jav", jav.history_retention_days);
    $('overview-jav-size').textContent = bytes(jav.downloaded_bytes);
    $('jav-transfers').textContent = `${jav.active_tasks} active`;
    $('jav-saved').textContent = bytes(jav.downloaded_bytes);
    renderRun('jav', jav.scheduler, javRun);
    $('jav-next-run').textContent = !jav.scheduler.enabled ? 'Disabled' : jav.scheduler.next_run_at ? dateTime(jav.scheduler.next_run_at) : 'Waiting for next run';
  }
  if (p91) {
    historyPeriod("p91", p91.history_retention_days);
    $('overview-p91-size').textContent = bytes(p91.downloaded_bytes);
    $('p91-transfers').textContent = `${p91.active_tasks} active`;
    $('p91-saved').textContent = bytes(p91.downloaded_bytes);
    renderRun('p91', p91.scheduler, p91Run);
    $('p91-next-run').textContent = !p91.scheduler.enabled ? 'Disabled' : p91.scheduler.next_run_at ? dateTime(p91.scheduler.next_run_at) : 'Waiting for next run';
  }
}

// Adapt provider snapshots into one presentation model at the UI boundary.
function activeDownloads() {
  return [
    ...(telegram?.active || []).map((item) => ({
      id: `telegram:${item.msg_id}:${item.path}`,
      source: 'Telegram', name: item.file_name,
      detail: telegram.paused ? 'Paused' : `${sizeLabel(item.downloaded)} / ${sizeLabel(item.total)}`,
      progress: item.percent, status: telegram.paused ? 'Paused' : sizeLabel(item.speed),
    })),
    ...(tasks || []).filter((task) => !terminal.has(task.state)).map((task) => ({
      id: `jav:${task.id}`,
      source: 'JAV', name: task.title || task.id,
      detail: `${label(task.state)} · ${label(task.phase)}`,
      progress: task.total_segments ? task.done_segments / task.total_segments * 100
        : task.total_bytes ? task.downloaded_bytes / task.total_bytes * 100 : null,
      status: task.state === 'paused' ? 'Paused' : `${bytes(task.downloaded_bytes)} · ${bytes(task.speed_kbps * 1024)}/s`,
    })),
    ...(p91Tasks || []).filter((task) => !terminal.has(task.state)).map((task) => ({
      id: `p91:${task.id}`,
      source: '91Porn', name: task.title || task.id,
      detail: `${label(task.state)} · ${label(task.phase)}`,
      progress: task.total_bytes ? task.downloaded_bytes / task.total_bytes * 100 : null,
      status: task.state === 'paused' ? 'Paused' : `${bytes(task.downloaded_bytes)} · ${bytes(task.speed_kbps * 1024)}/s`,
    })),
  ];
}

function recentDownloads() {
  return [
    ...(telegram?.completed || []).map((item) => ({
      id: `telegram:${item.id}`,
      source: 'Telegram', name: item.file_name, detail: `${sizeLabel(item.size)} · Completed`, date: new Date(item.completed_at).getTime(),
    })),
    ...(history || []).map((item) => ({
      id: `jav:${item.id}`,
      source: 'JAV', name: item.title || item.id, detail: `${bytes(item.size)} · ${label(item.status)}`, date: Date.parse(item.finished_at),
    })),
    ...(p91History || []).map((item) => ({
      id: `p91:${item.id}`,
      source: '91Porn', name: item.title || item.id, detail: `${bytes(item.size)} · ${label(item.status)}`, date: Date.parse(item.finished_at),
    })),
  ].sort((a, b) => b.date - a.date).slice(0, 10);
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
  renderRows($('overview-downloads'), rows, telegram && tasks && p91Tasks ? 'All caught up. New downloads will appear here.' : 'Waiting for all modules…');
}
function renderHistory() {
  renderRows($('overview-history'), recentDownloads(), telegram && history && p91History ? 'No downloads recorded yet' : 'Waiting for all modules…');
}
const providers = ['jav', 'p91'];
const taskErrors = new Set();
const historyErrors = new Set();
function refreshError(errors, provider, failed, target, message) {
  if (failed) errors.add(provider); else errors.delete(provider);
  $(target).hidden = !errors.size;
  if (errors.size) $(target).textContent = message;
}
const taskResources = Object.fromEntries(providers.map((provider) => {
  const showError = (failed) => refreshError(taskErrors, provider, failed, 'activity-error',
    'Download task status is unavailable. Retrying automatically.');
  return [provider, createTaskRefresh((signal) => api(`/${provider}/api/tasks`, { signal }), (data) => {
    if (provider === 'jav') tasks = data; else p91Tasks = data;
    showError(false);
    queueActivity();
  }, () => { showError(true); queueActivity(); })];
}));
const historyResources = Object.fromEntries(providers.map((provider) => {
  const showError = (failed) => refreshError(historyErrors, provider, failed, 'history-error',
    'Download history is unavailable. Retrying automatically.');
  return [provider, createSnapshotRefresh((signal) => api(`/${provider}/api/history`, { signal }), (data) => {
    if (provider === 'jav') history = data.records; else p91History = data.records;
    showError(false);
    queueHistory();
  }, () => { showError(true); queueHistory(); })];
}));
function refreshTasks(provider, fresh = false) {
  return Promise.all((provider ? [provider] : providers).map((name) => taskResources[name].refresh({ fresh })));
}
function refreshHistory(provider, fresh = false) {
  return Promise.all((provider ? [provider] : providers).map((name) => historyResources[name].refresh({ fresh })));
}
connectEvents('/telegram/events', {
  message(event) {
    telegram = JSON.parse(event.data);
    queueSummary(); queueActivity(); queueHistory();
  },
}, $('telegram-connection'));
connectEvents('/jav/api/events', {
  status(event) { jav = JSON.parse(event.data); queueSummary(); },
  task(event) {
    const task = JSON.parse(event.data);
    taskResources.jav.receive(task);
    if (tasks) {
      tasks = tasks.filter((item) => item.id !== task.id);
      tasks.push(task);
      queueActivity();
    }
    if (terminal.has(task.state)) refreshHistory('jav', true);
  },
  open() { refreshTasks('jav', true); refreshHistory('jav', true); },
}, $('jav-connection'));
connectEvents('/p91/api/events', {
  status(event) { p91 = JSON.parse(event.data); queueSummary(); },
  task(event) {
    const task = JSON.parse(event.data);
    taskResources.p91.receive(task);
    if (p91Tasks) {
      p91Tasks = p91Tasks.filter((item) => item.id !== task.id);
      p91Tasks.push(task);
      queueActivity();
    }
    if (terminal.has(task.state)) refreshHistory('p91', true);
  },
  open() { refreshTasks('p91', true); refreshHistory('p91', true); },
}, $('p91-connection'));
// Refresh removals and history changes made in another tab, which have no task event.
pollWhenVisible(() => Promise.all([refreshTasks(), refreshHistory()]), 30000);
