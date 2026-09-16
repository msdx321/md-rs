export const $ = (id) => document.getElementById(id);

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
  const events = new EventSource(path);
  function showConnection() {
    const live = navigator.onLine && events.readyState === EventSource.OPEN;
    connection.textContent = live ? 'Live' : navigator.onLine ? 'Reconnecting' : 'Offline';
    connection.className = 'pill ' + (live ? 'ok' : 'err');
  }
  for (const [name, handler] of Object.entries(handlers)) events.addEventListener(name, handler);
  events.addEventListener('open', showConnection);
  events.addEventListener('error', showConnection);
  window.addEventListener('online', showConnection);
  window.addEventListener('offline', showConnection);
  return events;
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
