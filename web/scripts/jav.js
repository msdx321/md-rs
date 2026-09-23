import { $, api, bindTabs, connectEvents, pollWhenVisible, scheduleRender, bytes, historyPeriod, dateTime, esc, toast, runAction } from "./shared.js";
import { createVideoLibrary } from './video-library.js';

// ── tabs ──────────────────────────────────────────────────────────────────
bindTabs(document.querySelectorAll('[role="tab"]'), (tab) => {
  if (tab.dataset.tab === 'history') loadHistory();
  if (tab.dataset.tab === 'tasks') loadTasks();
});

// ── status ────────────────────────────────────────────────────────────────
let cookieRefreshPending = false;
let latestStatus;
let statusRevision = 0;
function renderStatus(s) {
  latestStatus = s;
  historyPeriod("jav", s.history_retention_days);
  $("history-empty").textContent = `No downloads in the last ${s.history_retention_days} days.`;
  statusRevision += 1;
  const src = s.cookie_source === 'browser' ? 'automatic' : 'manual';
  $('pill-cookie').textContent = s.cookie_refreshing ? 'Refreshing access…'
    : s.cookie_error ? 'Access refresh failed'
    : s.cookie_configured ? 'Site access ready' : 'Site access pending';
  $('pill-cookie').className = 'pill ' + (s.cookie_refreshing ? '' : s.cookie_error || !s.cookie_configured ? 'err' : 'ok');
  const sched = s.scheduler;
  $('pill-sched').textContent = sched.running ? 'Job running'
    : sched.enabled ? 'Next run: ' + dateTime(sched.next_run_at) : 'Schedule disabled';
  $('pill-sched').className = 'pill ' + (sched.running ? 'ok' : '');
  $('stat-files').textContent = s.downloaded.toLocaleString();
  $('stat-bytes').textContent = bytes(s.downloaded_bytes);
  $('stat-active').textContent = s.active_tasks;

  // The clearance section: automatic minting means "no cookie" is only a
  // problem when nothing can produce one.
  $('btn-refresh-cookie').disabled = !s.cookie_minting || s.cookie_refreshing || cookieRefreshPending;
  const detail = $('cookie-detail');
  if (detail) {
    if (s.cookie_refreshing) {
      detail.textContent = 'Refreshing site access. Waiting for the browser to finish.';
    } else if (s.cookie_error) {
      detail.textContent = 'Clearance refresh failed: ' + s.cookie_error;
    } else if (!s.cookie_minting) {
      detail.textContent = 'Automatic site access is disabled. Paste a cookie under Advanced settings.';
    } else if (s.cookie_configured) {
      const age = s.cookie_age_secs == null ? '' : `, ${duration(s.cookie_age_secs)} old`;
      detail.textContent = `Access cookie: ${src}${age}. Reused until the site rejects it.` +
        (s.browser_running ? ' Browser ready.' : '');
    } else {
      detail.textContent = 'Site access will refresh automatically when needed.';
    }
  }

  $('settings-banner').innerHTML = (s.cookie_configured || s.cookie_minting) ? '' :
    '<div class="banner warn">Site access is not configured. Enable automatic site access ' +
    'or paste a cookie under Advanced settings.</div>';
}
async function loadStatus() {
  const revision = statusRevision;
  try {
    const status = await api('/jav/api/status');
    // A slow poll must not overwrite a newer push update.
    if (revision === statusRevision) renderStatus(status);
  } catch (e) { /* the event stream reports connection failures */ }
}
const duration = (secs) => {
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  return `${Math.floor(secs / 3600)}h`;
};

// ── popular ───────────────────────────────────────────────────────────────
let page = 1;
let popularRequest;
async function loadPopular(requestedPage = page) {
  popularRequest?.abort();
  const request = new AbortController();
  popularRequest = request;
  $('btn-prev').disabled = true;
  $('btn-next').disabled = true;
  $('popular-note').textContent = '';
  $('popular-banner').innerHTML = '<div class="banner warn">Loading listing…</div>';
  $('popular-grid').innerHTML = '';
  try {
    const query = new URLSearchParams({ page: requestedPage, link: $('popular-link').value || 0 });
    if ($('popular-sort').value !== 'configured') query.set('sort', $('popular-sort').value);
    const data = await api('/jav/api/videos?' + query, { signal: request.signal });
    if (popularRequest !== request) return;
    page = data.page;
    $('popular-banner').innerHTML = '';
    $('page-num').textContent = data.page;
    $('popular-note').textContent = `${data.videos.length} videos on this page${data.title_filter_active ? ' · name filter active' : ''}`;
    $('popular-grid').innerHTML = data.videos.map((v, i) => `
      <div class="card">
        ${v.image_url
          ? `<img alt="" class="thumb" src="${esc(v.image_url)}" referrerpolicy="no-referrer" loading="lazy" onerror="this.style.visibility='hidden'">`
          : '<div class="thumb"></div>'}
        <div class="body">
          <div class="title" title="${esc(v.title)}">
            <span class="rank">#${(data.page - 1) * 24 + i + 1}</span> ${esc(v.title)}
          </div>
          <div class="row">
            <span class="muted">${v.rank != null ? 'rank #' + v.rank : ''}</span>
            <button class="tiny" data-id="${esc(v.id)}" data-url="${esc(v.url)}"
                    data-title="${esc(v.title)}" data-rank="${v.rank ?? ''}"
                    ${v.downloaded || v.in_progress ? 'disabled' : ''}>
              ${v.downloaded ? 'Downloaded' : v.in_progress ? 'Downloading' : 'Download'}
            </button>
          </div>
        </div>
      </div>`).join('') || '<div class="empty">No videos found on this page.</div>';
  } catch (e) {
    if (popularRequest === request && e.name !== 'AbortError') $('popular-banner').innerHTML = `<div class="banner err">${esc(e.message)}</div>`;
  } finally {
    if (popularRequest === request) {
      $('btn-prev').disabled = page <= 1;
      $('btn-next').disabled = false;
    }
  }
}

$('popular-grid').addEventListener('click', (event) => {
  const button = event.target.closest('button[data-id]');
  if (button) runAction(button, () => download(button.dataset));
});

async function download(ds) {
  try {
    const r = await mutateTasks(() => api('/jav/api/download', {
      method: 'POST',
      body: JSON.stringify({
        id: ds.id,
        url: ds.url,
        title: ds.title,
        rank: ds.rank ? Number(ds.rank) : null,
      }),
    }));
    toast(r.status === 'started' ? 'Download queued' : 'Already ' + r.status.replace('_', ' '), 'ok');
    document.querySelector('nav button[data-tab="tasks"]').click();
  } catch (e) { toast(e.message, 'err'); }
}

// ── tasks and history ─────────────────────────────────────────────────────
const { loadTasks, loadHistory, receiveTask, mutateTasks } = createVideoLibrary({
  apiBase: '/jav/api',
  loadStatus,
  progress: (t) => t.state === 'completed' ? 100
    : t.total_segments > 0 ? Math.min(100, (t.done_segments / t.total_segments) * 100)
    : t.total_bytes > 0 ? Math.min(100, (t.downloaded_bytes / t.total_bytes) * 100) : 0,
  detail: (t) => t.state === 'running' || t.state === 'paused'
    ? (t.total_segments ? `${t.done_segments}/${t.total_segments} seg · ` : '') + bytes(t.downloaded_bytes)
    : (t.message || ''),
});

// ── settings ──────────────────────────────────────────────────────────────
const FIELDS = ['cookie', 'user_agent', 'site_base',
  'title_filter', 'title_filter_regex', 'max_pages', 'min_duration_secs', 'resolution', 'concurrent_videos',
  'segment_concurrency',
  'browser_enabled', 'browser_path', 'browser_profile_dir'];

let savedSettings = null;
let settingsSaving = false;
const queueSettings = scheduleRender(updateSettingsControls);
const queueLinkSearch = scheduleRender(updateLinkButtons);
function readSettings() {
  const values = Object.fromEntries(FIELDS.map((key) => {
    const el = $(key);
    const value = el.type === 'number' ? Number(el.value)
      : el.tagName === 'SELECT' ? el.value === 'true' : el.value;
    return [key, value];
  }));
  values.links = [...document.querySelectorAll('.ranking-link')].map((row) => ({
    url: sortedLink(row.querySelector('[data-link-url]').value.trim(), row.querySelector('[data-link-sort]').value),
    daily_quota: Number(row.querySelector('[data-link-quota]').value),
  }));
  return values;
}
function settingsChanged() {
  if (savedSettings === null) return false;
  const current = readSettings();
  return FIELDS.some((key) => current[key] !== savedSettings[key])
    || JSON.stringify(current.links) !== JSON.stringify(configuredLinks(savedSettings));
}
function updateSettingsControls() {
  const dirty = settingsChanged();
  $('btn-save').disabled = !dirty || settingsSaving;
  $('btn-discard').disabled = !dirty || settingsSaving;
  $('btn-save').textContent = settingsSaving ? 'Saving…' : 'Save settings';
  $('save-note').classList.toggle('is-dirty', dirty);
  $('save-note').textContent = settingsSaving ? 'Saving changes…' : dirty ? 'Unsaved changes' : 'All changes saved';
  $('regex-hint').hidden = $('title_filter_regex').value !== 'true';
  $('title_filter').placeholder = $('title_filter_regex').value === 'true' ? 'e.g. (?i)ABC-\\d+' : 'All titles';
}
function configuredLinks(cfg) {
  const links = cfg.links?.length ? cfg.links : [{ url: cfg.popular_path, daily_quota: cfg.top_n }];
  return links.map(link => ({ ...link, url: sortedLink(link.url, linkSort(link.url) || '') }));
}
function linkSort(value) {
  const query = value.split('#', 1)[0].split(/\?(.*)/s)[1];
  return new URLSearchParams(query).get('sort');
}
function sortedLink(value, sort) {
  const [address, fragment] = value.split(/#(.*)/s);
  const [path, query] = address.split(/\?(.*)/s);
  const params = new URLSearchParams(query);
  params.delete('sort');
  if (sort) params.set('sort', sort);
  const suffix = params.toString();
  return `${path}${suffix ? `?${suffix}` : ''}${fragment === undefined ? '' : `#${fragment}`}`;
}
function addLinkRow(link = { url: '', daily_quota: 10 }) {
  const row = document.createElement('details');
  row.className = 'ranking-link settings-item';
  row.innerHTML = `<summary><span class="settings-item-icon" aria-hidden="true">↗</span><span class="settings-item-summary"><strong data-link-name></strong><span data-link-preview></span></span><span class="settings-item-edit">Edit</span></summary>
    <div class="settings-item-fields">
      <label class="field full">Ranking URL or path
        <input data-link-url required placeholder="https://missav.ai/cn/fc2"><span class="hint">Use a full ranking URL or a path on your configured site.</span></label>
      <label class="field">Sort by
        <select data-link-sort>
          <option value="">Site default</option>
          <option value="today_views">Daily views</option>
          <option value="weekly_views">Weekly views</option>
          <option value="monthly_views">Monthly views</option>
        </select></label>
      <label class="field">Downloads per run
        <input data-link-quota type="number" required min="1" max="100"><span class="hint">Number of new completed downloads to save from this link.</span></label>
      <div class="settings-item-actions full"><button type="button" class="tiny danger" data-remove-link>Remove link</button></div>
    </div>`;
  const urlInput = row.querySelector('[data-link-url]');
  const sortInput = row.querySelector('[data-link-sort]');
  urlInput.value = link.url;
  row.querySelector('[data-link-quota]').value = link.daily_quota;
  const syncSort = () => {
    sortInput.disabled = !urlInput.value.trim();
    const sort = linkSort(urlInput.value) ?? sortInput.value;
    sortInput.querySelector('[data-custom-sort]')?.remove();
    if (![...sortInput.options].some(option => option.value === sort)) {
      const option = new Option(`Custom (${sort})`, sort);
      option.dataset.customSort = '';
      sortInput.append(option);
    }
    sortInput.value = sort;
    urlInput.value = sortedLink(urlInput.value, '');
  };
  syncSort();
  const updateSummary = () => {
    row.querySelector('[data-link-name]').textContent = urlInput.value.trim() || 'New ranking link';
    const quota = row.querySelector('[data-link-quota]').value;
    const quotaText = quota ? `${quota} ${quota === '1' ? 'download' : 'downloads'} per run` : 'Set a download quota';
    row.querySelector('[data-link-preview]').textContent = `${sortInput.selectedOptions[0].textContent} · ${quotaText}`;
  };
  urlInput.addEventListener('input', syncSort);
  sortInput.addEventListener('change', updateSummary);
  updateSummary();
  row.addEventListener('input', updateSummary);
  row.querySelector('[data-remove-link]').onclick = () => {
    const next = row.nextElementSibling || row.previousElementSibling;
    row.remove();
    updateLinkButtons();
    updateSettingsControls();
    (next && !next.hidden ? next.querySelector('summary') : $('btn-add-link')).focus();
  };
  $('ranking-links').appendChild(row);
  return row;
}
function updateLinkButtons() {
  const buttons = document.querySelectorAll('[data-remove-link]');
  buttons.forEach((button) => { button.disabled = buttons.length === 1; });
  const rows = [...$('ranking-links').children];
  const query = $('ranking-search').value.trim().toLowerCase();
  let visible = 0;
  for (const row of rows) {
    row.hidden = !row.querySelector('[data-link-url]').value.toLowerCase().includes(query);
    if (!row.hidden) visible++;
  }
  $('ranking-count').textContent = rows.length;
  $('ranking-search-count').textContent = query ? `${visible} of ${rows.length} links` : `${rows.length} ranking ${rows.length === 1 ? 'link' : 'links'}`;
  $('no-ranking-matches').hidden = visible > 0;
}
$('ranking-search').addEventListener('input', queueLinkSearch);
$('ranking-search').addEventListener('keydown', (event) => {
  if (event.key === 'Enter') event.preventDefault();
});
$('settings-form').addEventListener('invalid', (event) => {
  const row = event.target.closest('.ranking-link');
  if (row) {
    $('ranking-search').value = '';
    updateLinkButtons();
    row.open = true;
  }
}, true);
function populatePopularLinks(cfg) {
  const selected = $('popular-link').value;
  $('popular-link').innerHTML = configuredLinks(cfg).map((link, index) =>
    `<option value="${index}">${esc(sortedLink(link.url, ''))} · ${link.daily_quota}/run</option>`).join('');
  if ([...$('popular-link').options].some((option) => option.value === selected)) $('popular-link').value = selected;
  else $('popular-sort').value = 'configured';
  updatePopularSort(cfg);
}
function updatePopularSort(cfg = savedSettings) {
  const link = configuredLinks(cfg)[Number($('popular-link').value)];
  const sort = linkSort(link.url) || '';
  const option = [...$('popular-sort').options].find(option => option.value === sort);
  $('popular-sort').querySelector('[value="configured"]').textContent = `Saved: ${option?.textContent || `Custom (${sort})`}`;
}
$('btn-add-link').onclick = () => {
  $('ranking-search').value = '';
  const row = addLinkRow();
  updateLinkButtons();
  row.open = true;
  row.querySelector('input').focus();
  updateSettingsControls();
};
$('popular-link').onchange = () => {
  $('popular-sort').value = 'configured';
  updatePopularSort();
  loadPopular(1);
};
$('popular-sort').onchange = () => loadPopular(1);
function populateSettings(cfg) {
  $('ranking-links').innerHTML = '';
  configuredLinks(cfg).forEach(addLinkRow);
  updateLinkButtons();
  for (const key of FIELDS) $(key).value = String(cfg[key] ?? '');
  updateSettingsControls();
}
async function loadSettings() {
  savedSettings = await api('/jav/api/config');
  populateSettings(savedSettings);
  populatePopularLinks(savedSettings);
  loadPopular(1);
  $('settings-fields').disabled = false;
}
async function saveSettings() {
  settingsSaving = true;
  $('settings-fields').disabled = true;
  $('settings-error').hidden = true;
  updateSettingsControls();
  try {
    const previous = savedSettings;
    savedSettings = await api('/jav/api/config', { method: 'PUT', body: JSON.stringify(readSettings()) });
    populateSettings(savedSettings);
    toast('Settings saved', 'ok');
    loadStatus();
    populatePopularLinks(savedSettings);
    if (['title_filter', 'title_filter_regex', 'site_base'].some((key) => previous[key] !== savedSettings[key])
      || JSON.stringify(configuredLinks(previous)) !== JSON.stringify(configuredLinks(savedSettings))) loadPopular(1);
  } catch (e) {
    $('settings-error').textContent = e.message;
    $('settings-error').hidden = false;
    $('settings-error').scrollIntoView({ block: 'center' });
  } finally {
    settingsSaving = false;
    $('settings-fields').disabled = false;
    updateSettingsControls();
  }
}
$('settings-form').addEventListener('input', queueSettings);
$('settings-form').addEventListener('change', queueSettings);
$('settings-form').addEventListener('invalid', (event) => {
  const details = event.target.closest('details');
  if (details) details.open = true;
}, true);
$('btn-discard').onclick = () => {
  populateSettings(savedSettings);
  $('settings-error').hidden = true;
};
window.addEventListener('beforeunload', (event) => {
  if (settingsChanged()) { event.preventDefault(); event.returnValue = ''; }
});

// ── wiring ────────────────────────────────────────────────────────────────
$('btn-run').onclick = async () => {
  try { await api('/jav/api/daily/run', { method: 'POST' }); toast('Download job started', 'ok'); }
  catch (e) { toast(e.message, 'err'); }
};
$('btn-pause').onclick = async () => { await api('/jav/api/daily/pause', { method: 'POST' }); toast('Pausing all downloads'); };
$('btn-cancel').onclick = async () => { await api('/jav/api/daily/cancel', { method: 'POST' }); toast('Cancelling all downloads'); };
$('btn-resume').onclick = async () => {
  const r = await api('/jav/api/tasks/resume-all', { method: 'POST' });
  toast(`Resumed ${r.count} task(s)`, 'ok');
};
$('btn-refresh').onclick = () => loadPopular();
$('btn-prev').onclick = () => { if (page > 1) loadPopular(page - 1); };
$('btn-next').onclick = () => { loadPopular(page + 1); };
$('settings-form').onsubmit = (event) => { event.preventDefault(); if (!settingsSaving && settingsChanged()) saveSettings(); };
$('btn-refresh-cookie').onclick = async () => {
  if ($('btn-refresh-cookie').disabled) return;
  cookieRefreshPending = true;
  if (latestStatus) renderStatus({ ...latestStatus, cookie_refreshing: true, cookie_error: null });
  $('btn-refresh-cookie').disabled = true;
  try {
    await api('/jav/api/cookie/refresh', { method: 'POST' });
    toast('Fresh clearance cookie adopted', 'ok');
  } catch (e) {
    toast(e.message, 'err');
  } finally {
    cookieRefreshPending = false;
    await loadStatus();
    if (latestStatus) renderStatus(latestStatus);
  }
};

for (const id of ['btn-run', 'btn-pause', 'btn-cancel', 'btn-resume']) {
  const button = $(id);
  const action = button.onclick;
  button.onclick = () => runAction(button, () => mutateTasks(action));
}

// Live snapshots cover startup and reconnects; poll only if the stream is down.
const es = connectEvents('/jav/api/events');
es.addEventListener('status', (ev) => renderStatus(JSON.parse(ev.data)));
es.addEventListener('task', (ev) => receiveTask(JSON.parse(ev.data)));
function reconcileLibrary() {
  loadTasks({ fresh: true });
  if ($('history-tab').getAttribute('aria-selected') === 'true') loadHistory({ fresh: true });
}
es.addEventListener('open', reconcileLibrary);
es.addEventListener('resync', reconcileLibrary);

loadSettings().catch((e) => {
  toast('Could not load settings: ' + e.message, 'err');
  $('btn-save').disabled = true;
  $('save-note').textContent = 'Could not load settings. Reload to try again.';
});
pollWhenVisible(() => {
  if (es.readyState !== EventSource.OPEN) return Promise.all([loadStatus(), loadTasks()]);
}, 15000);
