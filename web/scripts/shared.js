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
