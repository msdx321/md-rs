import { $, api, connectEvents, bytes, historyPeriod } from './shared.js';

let telegram = null;
let jav = null;
let tasks = null;
let history = null;
const terminal = new Set(['completed', 'failed', 'cancelled']);

function renderSummary() {
  $('overview-active').textContent = telegram && jav ? telegram.active_count + jav.active_tasks : '—';
  if (telegram) {
    historyPeriod("telegram", telegram.history_retention_days);
    $('overview-telegram').textContent = telegram.downloaded_files;
    $('telegram-saved').textContent = telegram.downloaded_bytes;
    $('telegram-status').textContent = telegram.login.step === 'ready' ? telegram.status : telegram.login.message;
    $('telegram-transfers').textContent = telegram.paused ? 'Paused' : `${telegram.active_count} active`;
  }
  if (jav) {
    historyPeriod("jav", jav.history_retention_days);
    $('overview-jav').textContent = jav.downloaded;
    $('jav-saved').textContent = bytes(jav.downloaded_bytes);
    $('jav-status').textContent = jav.scheduler.running ? 'Daily job running' : jav.scheduler.last_result || 'Ready for downloads';
    $('jav-next-run').textContent = !jav.scheduler.enabled ? 'Disabled' : jav.scheduler.next_run_at ? new Date(jav.scheduler.next_run_at).toLocaleString() : 'Waiting for next run';
  }
}

// Adapt provider snapshots into one presentation model at the UI boundary.
function activeDownloads() {
  return [
    ...(telegram?.active || []).map((item) => ({
      source: 'Telegram', name: item.file_name,
      detail: telegram.paused ? 'Paused' : `${item.downloaded} / ${item.total}`,
      progress: item.percent, status: item.speed,
    })),
    ...(tasks || []).filter((task) => !terminal.has(task.state)).map((task) => ({
      source: 'JAV', name: task.title || task.id,
      detail: `${task.state} · ${task.phase}`,
      progress: task.total_segments ? task.done_segments / task.total_segments * 100 : null,
      status: `${bytes(task.downloaded_bytes)} · ${bytes(task.speed_kbps * 1024)}/s`,
    })),
  ];
}

function recentDownloads() {
  return [
    ...(telegram?.completed || []).map((item) => ({
      source: 'Telegram', name: item.file_name, detail: `${item.size} · Completed`, date: item.completed_at,
    })),
    ...(history || []).map((item) => ({
      source: 'JAV', name: item.title || item.id, detail: `${bytes(item.size)} · ${item.status}`, date: Date.parse(item.finished_at),
    })),
  ].sort((a, b) => b.date - a.date).slice(0, 10);
}

function renderRows(target, rows, empty) {
  target.replaceChildren();
  if (!rows.length) {
    const text = document.createElement('p');
    text.className = 'empty';
    text.textContent = empty;
    target.append(text);
    return;
  }
  for (const item of rows) {
    const row = document.createElement('article');
    row.className = 'overview-row';
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
      status.textContent = item.status;
      const progress = document.createElement('progress');
      progress.max = 100;
      progress.setAttribute('aria-label', `${item.source}: ${item.name}`);
      if (item.progress !== null) progress.value = Math.min(100, Math.max(0, item.progress));
      status.append(progress);
    } else {
      const time = document.createElement('time');
      if (Number.isFinite(item.date)) {
        time.dateTime = new Date(item.date).toISOString();
        time.textContent = new Date(item.date).toLocaleString();
      }
      status.append(time);
    }
    row.append(main, status);
    target.append(row);
  }
}

function renderActivity() {
  const rows = activeDownloads();
  $('activity-count').textContent = `${rows.length} listed`;
  renderRows($('overview-downloads'), rows, telegram && tasks ? 'No current downloads' : 'Waiting for all modules…');
}
function renderHistory() {
  renderRows($('overview-history'), recentDownloads(), telegram && history ? 'No downloads recorded yet' : 'Waiting for all modules…');
}
async function refreshTasks() {
  try {
    tasks = await api('/jav/api/tasks');
    $('activity-error').hidden = true;
    renderActivity();
  } catch {
    $('activity-error').textContent = 'JAV task status is unavailable. Retrying automatically.';
    $('activity-error').hidden = false;
  }
}
async function refreshHistory() {
  try {
    history = (await api('/jav/api/history')).records;
    $('history-error').hidden = true;
    renderHistory();
  } catch {
    $('history-error').textContent = 'JAV history is unavailable. Retrying automatically.';
    $('history-error').hidden = false;
  }
}
connectEvents('/telegram/events', {
  message(event) {
    telegram = JSON.parse(event.data);
    renderSummary(); renderActivity(); renderHistory();
  },
}, $('telegram-connection'));
connectEvents('/jav/api/events', {
  status(event) { jav = JSON.parse(event.data); renderSummary(); },
  task(event) {
    const task = JSON.parse(event.data);
    if (tasks) {
      tasks = tasks.filter((item) => item.id !== task.id);
      tasks.push(task);
      renderActivity();
    }
    if (terminal.has(task.state)) refreshHistory();
  },
  open() { refreshTasks(); refreshHistory(); },
}, $('jav-connection'));
// Refresh removals and history changes made in another tab, which have no task event.
setInterval(() => { refreshTasks(); refreshHistory(); }, 30000);
