import { $, pollWhenVisible } from './shared.js';

const panel = $('logs-panel');
const output = $('logs-output');
const search = $('logs-search');
const level = $('logs-level');
const source = $('logs-module');
const status = $('logs-status');
const pause = $('logs-pause');
let entries = [];
let loaded = false;
let paused = false;
let loading = false;
let error = false;

source.value = ['telegram', 'jav'].find(module => document.body.classList.contains(module)) || '';

function showScope() {
  $('logs-scope').textContent = source.selectedOptions[0].textContent;
}
showScope();

function showStatus() {
  status.textContent = paused ? 'Paused' : error ? 'Unable to refresh logs · Retrying…'
    : loaded ? `Live · ${entries.length} entries · Refreshes every 2s` : 'Loading…';
  status.classList.toggle('logs-status-error', error && !paused);
}

function render() {
  const follow = output.scrollTop < 40;
  const scrollTop = output.scrollTop;
  const query = search.value.trim().toLowerCase();
  const visible = entries.filter(entry => (!level.value || entry.level === level.value)
    && (!source.value || entry.target.split('::').includes(source.value))
    && `${entry.target} ${entry.message}`.toLowerCase().includes(query));
  const fragment = document.createDocumentFragment();
  for (const entry of visible.reverse()) {
    const row = document.createElement('div');
    row.className = 'log-entry';
    row.dataset.level = entry.level;
    const time = document.createElement('time');
    time.className = 'log-time';
    time.dateTime = entry.timestamp;
    time.title = entry.timestamp;
    time.textContent = new Date(entry.timestamp).toLocaleString();
    const severity = document.createElement('span');
    severity.className = 'log-level';
    severity.textContent = entry.level;
    const content = document.createElement('div');
    content.className = 'log-content';
    const target = document.createElement('div');
    target.className = 'log-target';
    target.textContent = entry.target;
    const message = document.createElement('p');
    message.className = 'log-message';
    message.textContent = entry.message;
    content.append(target, message);
    row.append(time, severity, content);
    fragment.append(row);
  }
  if (!visible.length) {
    const empty = document.createElement('p');
    empty.className = 'empty';
    empty.textContent = entries.length ? 'No logs match these filters.' : 'No log entries yet.';
    fragment.append(empty);
  }
  output.replaceChildren(fragment);
  output.scrollTop = follow ? 0 : scrollTop;
}

async function refresh() {
  if (!panel.open || !panel.getClientRects().length || document.hidden || paused || loading) return;
  loading = true;
  try {
    const response = await fetch('/api/logs', { cache: 'no-store', signal: AbortSignal.timeout(10000) });
    if (!response.ok) throw new Error('Could not load logs');
    const next = await response.json();
    if (!panel.open || !panel.getClientRects().length || document.hidden || paused) return;
    const changed = !loaded || next.at(-1)?.id !== entries.at(-1)?.id
      || next.at(-1)?.timestamp !== entries.at(-1)?.timestamp;
    entries = next;
    loaded = true;
    error = false;
    if (changed) render();
  } catch {
    error = true;
    if (!loaded) output.querySelector('.empty').textContent = 'Logs are temporarily unavailable.';
  } finally {
    loading = false;
    showStatus();
  }
}

panel.addEventListener('toggle', refresh);
search.addEventListener('input', render);
level.addEventListener('change', render);
source.addEventListener('change', () => { showScope(); render(); });
pause.addEventListener('click', () => {
  paused = !paused;
  pause.textContent = paused ? 'Resume' : 'Pause';
  pause.setAttribute('aria-pressed', String(paused));
  showStatus();
  if (!paused) refresh();
});
document.addEventListener('visibilitychange', refresh);
window.addEventListener('pageshow', refresh);
pollWhenVisible(refresh, 2000);
