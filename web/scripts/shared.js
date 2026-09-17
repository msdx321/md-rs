export const $ = (id) => document.getElementById(id);

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
  function update() {
    clearInterval(timer);
    if (!document.hidden) timer = setInterval(callback, interval);
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

// Keep task rows and their controls mounted while progress snapshots change.
export function reconcileTaskRows(container, rows) {
  const existing = new Map([...container.children].map(row => [row.dataset.taskId, row]));
  let position = container.firstElementChild;
  for (const next of rows) {
    const key = next.dataset.taskId;
    const current = existing.get(key);
    const row = current || next;
    if (current) patchTaskNode(current, next);
    if (row !== position) container.insertBefore(row, position);
    position = row.nextElementSibling;
    existing.delete(key);
  }
  for (const row of existing.values()) row.remove();
}

function patchTaskNode(current, next) {
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
    else patchTaskNode(oldChildren[i], newChildren[i]);
  }
}

export function historyPeriod(module, days) {
  document.querySelectorAll(`[data-history-days="${module}"]`).forEach(el => {
    const text = `· ${days} days`;
    if (el.textContent !== text) el.textContent = text;
  });
}

// Column preferences belong to the table, so live row updates never reset them.
for (const table of document.querySelectorAll('table[data-resizable]')) {
  const headers = [...table.querySelectorAll('th')];
  const columns = [...table.querySelectorAll('col')];
  const minimums = headers.map(header => Number(header.dataset.minWidth || 90));
  const key = `media-downloader-columns-${table.dataset.resizable}`;
  const tools = document.createElement('div');
  tools.className = 'table-tools';
  const hint = document.createElement('span');
  hint.textContent = 'Drag column edges to resize';
  const reset = document.createElement('button');
  reset.type = 'button';
  reset.className = 'tiny';
  reset.textContent = 'Reset columns';
  reset.disabled = true;
  tools.append(hint, reset);
  table.parentElement.before(tools);

  function apply(widths) {
    const total = widths.reduce((sum, width) => sum + width, 0);
    table.style.width = '100%';
    table.style.minWidth = `${total}px`;
    // The last column fills spare space while the resizable columns keep their widths.
    columns.forEach((column, index) => { column.style.width = index === columns.length - 1 ? 'auto' : `${widths[index]}px`; });
    headers.forEach((header, index) => header.querySelector('.column-resizer')?.setAttribute('aria-valuenow', String(Math.round(widths[index]))));
    reset.disabled = false;
  }
  function remember(widths) {
    try { localStorage.setItem(key, JSON.stringify(widths)); } catch {}
  }
  function measure() { return headers.map(header => header.getBoundingClientRect().width); }
  reset.onclick = () => {
    table.style.removeProperty('width');
    table.style.removeProperty('min-width');
    columns.forEach(column => column.style.removeProperty('width'));
    try { localStorage.removeItem(key); } catch {}
    reset.disabled = true;
    headers.forEach((header, index) => header.querySelector('.column-resizer')?.setAttribute('aria-valuenow', String(Math.round(measure()[index]))));
  };
  headers.slice(0, -1).forEach((header, index) => {
    const handle = document.createElement('span');
    handle.className = 'column-resizer';
    handle.tabIndex = 0;
    handle.setAttribute('role', 'separator');
    handle.setAttribute('aria-orientation', 'vertical');
    handle.setAttribute('aria-label', `Resize ${header.textContent.trim()} column`);
    handle.setAttribute('aria-valuemin', String(minimums[index]));
    handle.setAttribute('aria-valuemax', '1000');
    handle.setAttribute('aria-valuenow', String(Math.round(header.getBoundingClientRect().width) || minimums[index]));
    handle.title = 'Drag or use arrow keys to resize. Double-click or press Home to reset columns.';
    header.append(handle);
    let startX;
    let widths;
    let initialWidth;
    handle.addEventListener('focus', () => handle.setAttribute('aria-valuenow', String(Math.round(measure()[index]))));
    handle.addEventListener('pointerdown', event => {
      if (event.button !== 0) return;
      event.preventDefault();
      handle.focus();
      widths = measure();
      initialWidth = widths[index];
      startX = event.clientX;
      handle.setPointerCapture(event.pointerId);
      handle.classList.add('resizing');
    });
    handle.addEventListener('pointermove', event => {
      if (startX === undefined) return;
      widths[index] = Math.min(1000, Math.max(minimums[index], initialWidth + event.clientX - startX));
      apply(widths);
    });
    handle.addEventListener('lostpointercapture', () => {
      if (startX === undefined) return;
      remember(widths);
      startX = undefined;
      handle.classList.remove('resizing');
    });
    handle.addEventListener('dblclick', () => reset.click());
    handle.addEventListener('keydown', event => {
      if (event.key === 'Home') { event.preventDefault(); reset.click(); return; }
      if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
      event.preventDefault();
      const next = measure();
      const delta = (event.key === 'ArrowRight' ? 1 : -1) * (event.shiftKey ? 40 : 10);
      next[index] = Math.min(1000, Math.max(minimums[index], next[index] + delta));
      apply(next);
      remember(next);
    });
  });
  try {
    const widths = JSON.parse(localStorage.getItem(key));
    if (Array.isArray(widths) && widths.length === columns.length && widths.every((width, index) => Number.isFinite(width) && width >= minimums[index] && width <= 1000)) apply(widths);
  } catch {}
  new ResizeObserver(() => {
    headers.forEach(header => {
      const width = Math.round(header.getBoundingClientRect().width);
      if (width) header.querySelector('.column-resizer')?.setAttribute('aria-valuenow', String(width));
    });
  }).observe(table);
}
