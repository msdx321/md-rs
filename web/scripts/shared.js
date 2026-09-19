import { initTableColumns } from './table-columns.js';

initTableColumns();

export const $ = (id) => document.getElementById(id);

export const esc = (s) => String(s ?? '').replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

export function toast(message, kind = '') {
  const el = document.createElement('div');
  el.className = kind;
  el.textContent = message;
  $('toast').appendChild(el);
  setTimeout(() => el.remove(), 6000);
}

// For static controls only; reconciled rows own pending state in their controller.
export async function runAction(button, action) {
  if (button.disabled) return;
  button.disabled = true;
  try { await action(); }
  catch (e) { toast(e.message, 'err'); }
  finally { button.disabled = false; }
}

// Coalesce event bursts, rendering the latest state once per animation frame.
export function scheduleRender(render) {
  let frame;
  let args;
  return (...latest) => {
    args = latest;
    if (frame !== undefined) return;
    frame = requestAnimationFrame(() => {
      frame = undefined;
      render(...args);
    });
  };
}

document.addEventListener('invalid', (event) => {
  for (let parent = event.target.parentElement; parent; parent = parent.parentElement) {
    if (parent.tagName === 'DETAILS') parent.open = true;
  }
}, true);

const dateFormatter = new Intl.DateTimeFormat('en-GB', {
  day: '2-digit', month: 'short', year: 'numeric',
  hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZoneName: 'short',
});
// All timestamps use the browser's timezone, including server-scheduled runs.
export function dateTime(value) {
  if (value == null || value === '') return '—';
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '—' : dateFormatter.format(date);
}

export const label = (value) => value ? value[0].toUpperCase() + value.slice(1).replaceAll('_', ' ') : '—';

// Telegram supplies formatted sizes; normalize their spacing at the UI boundary.
export const sizeLabel = (value) => value === '0' ? '0 B' : String(value ?? '—').replace(/(\d)\s*([KMGTPE]?B)/g, '$1 $2');

// Both APIs use JSON for data; login errors and empty command replies are text.
export async function api(path, options = {}) {
  const headers = new Headers(options.headers);
  if (!headers.has('content-type')) headers.set('content-type', 'application/json');
  const response = await fetch(path, { ...options, headers });
  const text = await response.text();
  let data = {};
  if (text) {
    try { data = JSON.parse(text); }
    catch {
      throw new Error(response.ok ? 'Invalid response from the server' : text);
    }
  }
  if (!response.ok) throw new Error(data.error || data.message || response.statusText);
  return data;
}

export function bindTabs(elements, onSelect = () => {}) {
  const tabs = [...elements];
  function select(selected) {
    tabs.forEach((tab) => {
      const active = tab === selected;
      tab.classList.toggle('active', active);
      tab.setAttribute('aria-selected', String(active));
      tab.tabIndex = active ? 0 : -1;
      const panel = $(tab.getAttribute('aria-controls'));
      panel.hidden = !active;
      panel.classList.toggle('active', active);
    });
    onSelect(selected);
  }
  tabs.forEach((tab, index) => {
    tab.addEventListener('click', () => select(tab));
    tab.addEventListener('keydown', (event) => {
      const next = {
        ArrowRight: (index + 1) % tabs.length,
        ArrowLeft: (index + tabs.length - 1) % tabs.length,
        Home: 0,
        End: tabs.length - 1,
      }[event.key];
      if (next === undefined) return;
      event.preventDefault();
      select(tabs[next]);
      tabs[next].focus();
    });
  });
}

export function connectEvents(path, handlers = {}, connection = $('connection')) {
  const listeners = new EventTarget();
  const names = new Set(['open', 'error', ...Object.keys(handlers)]);
  let stream;
  function showConnection() {
    const live = navigator.onLine && stream?.readyState === EventSource.OPEN;
    connection.textContent = document.hidden ? 'Paused' : live ? 'Live' : navigator.onLine ? 'Reconnecting' : 'Offline';
    connection.className = 'pill ' + (document.hidden ? '' : live ? 'ok' : 'err');
  }
  function forward(event) {
    showConnection();
    listeners.dispatchEvent(event instanceof MessageEvent
      ? new MessageEvent(event.type, { data: event.data, lastEventId: event.lastEventId })
      : new Event(event.type));
  }
  function close() { stream?.close(); stream = null; }
  function connect() {
    if (document.hidden) close();
    else if (!stream) {
      stream = new EventSource(path);
      for (const name of names) stream.addEventListener(name, forward);
    }
    showConnection();
  }
  for (const [name, handler] of Object.entries(handlers)) listeners.addEventListener(name, handler);
  document.addEventListener('visibilitychange', connect);
  window.addEventListener('pagehide', close);
  window.addEventListener('pageshow', connect);
  window.addEventListener('online', showConnection);
  window.addEventListener('offline', showConnection);
  connect();
  return {
    get readyState() { return stream?.readyState ?? EventSource.CLOSED; },
    addEventListener(name, handler, options) {
      if (!names.has(name)) {
        names.add(name);
        stream?.addEventListener(name, forward);
      }
      listeners.addEventListener(name, handler, options);
    },
  };
}

// Hidden pages need neither polling timers nor DOM updates. Streams reconnect
// with fresh snapshots when the page becomes visible again.
export function pollWhenVisible(callback, interval) {
  let timer;
  let running = false;
  async function tick() {
    if (running) return;
    running = true;
    try { await callback(); }
    finally { running = false; }
  }
  function update() {
    clearInterval(timer);
    if (!document.hidden) timer = setInterval(tick, interval);
  }
  document.addEventListener('visibilitychange', update);
  window.addEventListener('pagehide', () => clearInterval(timer));
  window.addEventListener('pageshow', update);
  update();
}

export const bytes = (n) => {
  if (n == null) return '—';
  if (n === 0) return '0 B';
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return n.toFixed(n < 10 && i > 0 ? 1 : 0) + ' ' + u[i];
};

// Keep keyed rows and their controls mounted while live snapshots change.
export function reconcileRows(container, rows) {
  const existing = new Map([...container.children].map(row => [row.dataset.rowId, row]));
  let position = container.firstElementChild;
  for (const next of rows) {
    const key = next.dataset.rowId;
    const current = existing.get(key);
    const row = current || next;
    if (current) patchNode(current, next);
    if (row !== position) container.insertBefore(row, position);
    position = row.nextElementSibling;
    existing.delete(key);
  }
  for (const row of existing.values()) row.remove();
}

function patchNode(current, next) {
  if (current.nodeType !== next.nodeType || current.nodeName !== next.nodeName) {
    current.replaceWith(next.cloneNode(true));
    return;
  }
  if (current.nodeType === Node.TEXT_NODE) {
    if (current.nodeValue !== next.nodeValue) current.nodeValue = next.nodeValue;
    return;
  }
  for (const attr of [...current.attributes]) {
    if (!next.hasAttribute(attr.name)) current.removeAttribute(attr.name);
  }
  for (const attr of next.attributes) {
    if (current.getAttribute(attr.name) !== attr.value) current.setAttribute(attr.name, attr.value);
  }
  const oldChildren = [...current.childNodes];
  const newChildren = [...next.childNodes];
  for (let i = 0; i < Math.max(oldChildren.length, newChildren.length); i++) {
    if (!newChildren[i]) oldChildren[i].remove();
    else if (!oldChildren[i]) current.append(newChildren[i].cloneNode(true));
    else patchNode(oldChildren[i], newChildren[i]);
  }
}

export function historyPeriod(module, days) {
  document.querySelectorAll(`[data-history-days="${module}"]`).forEach(el => {
    const text = `· ${days} days`;
    if (el.textContent !== text) el.textContent = text;
  });
}

// Keep the selected history page when live snapshots replace the records.
export function bindHistoryPagination(render) {
  const previous = $('history-prev');
  const next = $('history-next');
  const size = $('history-page-size');
  const status = $('history-page-status');
  let items = [];
  let page = 1;
  let disabled = false;
  let renderedPage = '';
  const pageSize = () => Number(size.value);
  const pages = () => Math.max(1, Math.ceil(items.length / pageSize()));
  function controls() {
    previous.disabled = disabled || page <= 1;
    next.disabled = disabled || page >= pages();
    size.disabled = disabled;
  }
  function show() {
    page = Math.min(page, pages());
    const start = (page - 1) * pageSize();
    status.textContent = items.length
      ? `${start + 1}–${Math.min(start + pageSize(), items.length)} of ${items.length} · Page ${page} of ${pages()}`
      : 'No entries';
    controls();
    const visible = items.slice(start, start + pageSize());
    // Progress snapshots often repeat the same history. Compare only this page.
    const signature = JSON.stringify(visible);
    if (signature !== renderedPage) {
      render(visible);
      renderedPage = signature;
    }
  }
  previous.addEventListener('click', () => { if (page > 1) { page--; show(); } });
  next.addEventListener('click', () => { if (page < pages()) { page++; show(); } });
  size.addEventListener('change', () => { page = 1; show(); });
  return {
    update(records) { items = records; show(); },
    setDisabled(value) { disabled = value; controls(); },
  };
}
