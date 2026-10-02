import "./telegram-settings.js";
import { createSnapshotRefresh } from "./snapshot-refresh.js";
import { api, bindTabs, bindHistoryPagination, connectEvents, scheduleRender, reconcileRows, historyPeriod, dateTime, label, sizeLabel, bytes, esc, toast } from "./shared.js";

const statusEl = document.querySelector("#status");
const requestEl = document.querySelector("#request-status");
const filesEl = document.querySelector("#downloaded-files");
const bytesEl = document.querySelector("#downloaded-bytes");
const activeEl = document.querySelector("#active-count");
const downloadsEl = document.querySelector("#tasks-body");
const completedEl = document.querySelector("#history-body");
const tabEls = [...document.querySelectorAll('[role="tab"]')];
const controls = Object.fromEntries(["pause", "resume", "cancel"].map(action => [action, document.querySelector(`#btn-${action}`)]));
const taskBanner = document.querySelector("#tasks-banner");
let cancelling = false;
let controlBusy = false;
function updateControls() {
  const unavailable = controlBusy || cancelling || loginStep?.step !== "ready";
  controls.pause.disabled = unavailable || paused;
  controls.resume.disabled = unavailable || !paused;
  controls.cancel.disabled = unavailable;
  document.querySelector("#btn-run").disabled = unavailable || paused;
}
const formEl = document.querySelector("#download-form");
const linkEl = document.querySelector("#chat-link");
const actionEls = [...document.querySelectorAll("button[name=action]")];
let paused = false;
let historyBusy = false;
let channelNamesSignature = null;
let historyRevision = null;
let requestStatus = null;
const historyPager = bindHistoryPagination(renderHistoryPage);

bindTabs(tabEls, (tab) => {
  if (tab.id === 'completed-tab') loadHistory();
  if (tab.id === 'browse-tab') loadBrowseChats();
});

// Browsing has its own cursors; it never changes the downloader's scan position.
const browseChat = document.querySelector('#browse-chat');
const browseCustom = document.querySelector('#browse-custom');
const browseGrid = document.querySelector('#browse-grid');
const browseBanner = document.querySelector('#browse-banner');
let browseNames = {};
let browseRequest;
let browseLoading = false;
let browseCurrentChat = '';
let browseCursors = [0];
let browsePage = 0;
let browseNext = null;
const browseQueued = new Set();
function updateBrowseControls() {
  const ready = loginStep?.step === 'ready';
  document.querySelector('#btn-browse').disabled = !ready || browseLoading;
  document.querySelector('#browse-login-note').hidden = ready;
  document.querySelector('#browse-prev').disabled = !ready || browseLoading || browsePage === 0;
  document.querySelector('#browse-next').disabled = !ready || browseLoading || browseNext === null;
  browseGrid.querySelectorAll('button[data-message]').forEach(button => {
    button.disabled = !ready || browseLoading || paused || cancelling || button.dataset.unavailable === 'true';
  });
}
function updateBrowseChat() {
  document.querySelector('#browse-custom-field').hidden = Boolean(browseChat.value);
  browseCustom.required = !browseChat.value;
}
browseChat.addEventListener('change', updateBrowseChat);
async function loadBrowseChats() {
  try {
    const cfg = await api('/telegram/api/config');
    const selected = browseChat.value;
    browseChat.replaceChildren(...(cfg.chat || []).map(chat => new Option(
      browseNames[chat.chat_id.replace(/^@/, '').toLowerCase()] || chat.chat_id,
      chat.chat_id,
    )), new Option('Other chat…', ''));
    if ([...browseChat.options].some(option => option.value === selected) && selected) browseChat.value = selected;
    updateBrowseChat();
    if (!browseCurrentChat && !browseLoading && loginStep?.step === 'ready' && browseChat.value) loadBrowse(0, true);
  } catch (error) {
    browseBanner.textContent = error.message;
    browseBanner.className = 'banner err';
    browseBanner.hidden = false;
  }
}
async function loadBrowse(requestedPage = 0, reset = false) {
  const chat = reset ? (browseChat.value || browseCustom.value.trim()) : browseCurrentChat;
  if (!chat) return;
  browseRequest?.abort();
  const request = new AbortController();
  browseRequest = request;
  browseLoading = true;
  updateBrowseControls();
  browseBanner.textContent = 'Loading chat media…';
  browseBanner.className = 'banner';
  browseBanner.hidden = false;
  try {
    const before = reset ? 0 : requestedPage > browsePage ? browseNext : browseCursors[requestedPage];
    const query = new URLSearchParams({ chat, before });
    const data = await api('/telegram/api/media?' + query, { signal: request.signal });
    if (browseRequest !== request) return;
    if (reset) {
      browseCursors = [0];
      browseQueued.clear();
    }
    browseCursors[requestedPage] = before;
    browseCurrentChat = data.chat_id;
    browsePage = requestedPage;
    browseNext = data.next_before;
    document.querySelector('#browse-page').textContent = `Page ${browsePage + 1}`;
    document.querySelector('#browse-note').textContent = `${data.chat_name} · ${data.media.length} items on this page`;
    browseGrid.innerHTML = data.media.map(item => {
      const queued = browseQueued.has(`${data.chat_id}:${item.message_id}`);
      const title = item.caption || `${label(item.media_type)} · Message ${item.message_id}`;
      const duration = item.duration_secs > 0 ? ` · ${Math.floor(item.duration_secs / 60)}:${String(item.duration_secs % 60).padStart(2, '0')}` : '';
      const resolution = item.media_type === 'video'
        ? ` · ${item.width > 0 && item.height > 0 ? `${Math.min(item.width, item.height)}p` : 'Resolution unknown'}`
        : '';
      return `<div class="card">
        <div class="thumb browse-thumb"><span class="muted">${label(item.media_type)} preview unavailable</span>${item.image_url ? `<img alt="" src="${esc(item.image_url)}" loading="lazy">` : ''}</div>
        <div class="body"><div class="title" title="${esc(title)}">${esc(title)}</div>
          <div class="muted">${label(item.media_type)} · ${bytes(item.size)}${duration}${resolution}</div>
          <div class="row"><span class="muted">#${item.message_id}</span><button class="tiny" type="button" data-message="${item.message_id}" data-unavailable="${item.downloaded || queued}">${item.downloaded ? 'Downloaded' : queued ? 'Queued' : 'Download'}</button></div>
        </div></div>`;
    }).join('') || '<div class="empty">No photos or videos found in this chat.</div>';
    browseGrid.querySelectorAll('img').forEach(image => {
      image.addEventListener('error', () => image.remove(), { once: true });
      image.addEventListener('load', () => { image.previousElementSibling.hidden = true; }, { once: true });
    });
    browseBanner.hidden = true;
  } catch (error) {
    if (browseRequest !== request || error.name === 'AbortError') return;
    browseBanner.textContent = error.message;
    browseBanner.className = 'banner err';
    browseBanner.hidden = false;
  } finally {
    if (browseRequest === request) {
      browseLoading = false;
      updateBrowseControls();
    }
  }
}
document.querySelector('#browse-form').addEventListener('submit', event => {
  event.preventDefault();
  loadBrowse(0, true);
});
document.querySelector('#browse-prev').onclick = () => loadBrowse(browsePage - 1);
document.querySelector('#browse-next').onclick = () => loadBrowse(browsePage + 1);
browseGrid.addEventListener('click', async event => {
  const button = event.target.closest('button[data-message]');
  if (!button || button.disabled) return;
  const chat = browseCurrentChat;
  const message = Number(button.dataset.message);
  button.dataset.unavailable = 'true';
  button.disabled = true;
  try {
    const result = await api('/telegram/api/media/download', {
      method: 'POST', body: JSON.stringify({ chat_id: chat, message_id: message }),
    });
    browseQueued.add(`${chat}:${message}`);
    button.textContent = 'Queued';
    toast(result.message, 'ok');
  } catch (error) {
    button.dataset.unavailable = 'false';
    toast(error.message, 'err');
  } finally { updateBrowseControls(); }
});

formEl.addEventListener("submit", async (event) => {
  event.preventDefault();
  const submitEl = event.submitter || actionEls[0];
  const action = submitEl?.value || "once";
  const label = submitEl.textContent;
  actionEls.forEach((button) => { button.disabled = true; });
  submitEl.textContent = action === "subscribe" ? "Subscribing..." : "Queueing...";
  requestEl.className = "request-status";
  try {
    const result = await api(action === "subscribe" ? "/telegram/subscriptions" : "/telegram/downloads", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ chat_link: linkEl.value }),
    });
    requestEl.textContent = result.message;
    formEl.reset();
  } catch (error) {
    requestEl.textContent = error.message || "Could not reach the downloader";
    requestEl.classList.add("is-error");
  } finally {
    actionEls.forEach((button) => { button.disabled = false; });
    submitEl.textContent = label;
  }
});

for (const [action, button] of Object.entries(controls)) {
  button.addEventListener("click", async () => {
    controlBusy = true;
    updateControls();
    taskBanner.textContent = "";
    taskBanner.className = "";
    try {
      await api(`/telegram/${action}`, { method: "POST" });
    } catch (error) {
      taskBanner.textContent = error.message || "Could not reach the downloader";
      taskBanner.className = "banner err";
    } finally {
      controlBusy = false;
      updateControls();
    }
  });
}

document.querySelector('#btn-run').addEventListener('click', async () => {
  controlBusy = true;
  updateControls();
  try {
    await api('/telegram/scan', { method: 'POST' });
    taskBanner.textContent = '';
    taskBanner.className = '';
  } catch (error) {
    taskBanner.textContent = error.message;
    taskBanner.className = 'banner err';
  } finally {
    controlBusy = false;
    updateControls();
  }
});

function setText(selector, value, root = document) {
  root.querySelector(selector).textContent = value;
}

function updateHistoryControls() {
  historyPager.setDisabled(historyBusy);
  document.querySelector('#btn-reload-history').disabled = historyBusy;
  document.querySelector('#btn-clear-history').disabled = historyBusy || !completedEl.children.length;
  completedEl.querySelectorAll('button').forEach(button => { button.disabled = historyBusy; });
}

function renderHistory(items, days) {
  const historyEmpty = document.querySelector('#history-empty');
  historyEmpty.hidden = items.length !== 0;
  historyEmpty.textContent = `No downloads in the last ${days} days.`;
  historyPager.update(items);
}

function renderHistoryPage(items) {
  const rows = [];
  for (const item of items) {
    const row = document.createElement("tr");
    row.dataset.rowId = item.id;
    row.innerHTML = `
      <td><span class="truncate file"></span></td>
      <td class="muted msg"></td>
      <td class="muted size"></td>
      <td class="muted"><time></time></td>
      <td><span class="tag completed">Completed</span></td>
      <td class="muted"><span class="truncate path"></span></td>
      <td><button class="tiny" type="button" data-forget>Forget</button></td>`;
    row.querySelector('button').dataset.forget = item.id;
    row.querySelector('button').title = 'Forget this record and allow downloading again; keep the saved file';
    setText(".file", item.file_name, row);
    row.querySelector('.file').title = item.file_name;
    setText(".size", sizeLabel(item.size), row);
    setText(".msg", `#${item.msg_id}`, row);
    setText(".path", item.path, row);
    row.querySelector('.path').title = item.path;
    const date = new Date(item.completed_at);
    const time = row.querySelector("time");
    time.dateTime = date.toISOString();
    time.textContent = dateTime(item.completed_at);
    rows.push(row);
  }
  reconcileRows(completedEl, rows);
  updateHistoryControls();
}

const historySnapshots = createSnapshotRefresh((signal) => api('/telegram/api/history', { signal }), (snapshot) => {
  renderHistory(snapshot.records, snapshot.history_retention_days);
  historyPeriod('telegram', snapshot.history_retention_days);
  filesEl.textContent = snapshot.downloaded_files.toLocaleString();
  bytesEl.textContent = sizeLabel(snapshot.downloaded_bytes);
}, (error) => { throw error; });
function refreshHistory() { return historySnapshots.refresh({ fresh: true }); }

async function historyAction(path, message) {
  if (historyBusy) return;
  historyBusy = true;
  updateHistoryControls();
  const banner = document.querySelector('#history-banner');
  banner.hidden = true;
  banner.className = 'banner';
  try {
    if (path) await api(path, { method: 'DELETE' });
    await refreshHistory();
    if (message) {
      banner.textContent = message;
      banner.hidden = false;
    }
  } catch (error) {
    banner.textContent = error.message;
    banner.className = 'banner err';
    banner.hidden = false;
  } finally {
    historyBusy = false;
    updateHistoryControls();
  }
}

function loadHistory() { return historyAction(); }
document.querySelector('#btn-reload-history').onclick = loadHistory;
document.querySelector('#btn-clear-history').onclick = () => {
  if (!confirm('Clear retained history? Saved files are kept and these messages can be requested again. Chat scan positions are unchanged.')) return;
  historyAction('/telegram/api/history', 'History cleared. Saved files were kept.');
};
completedEl.addEventListener('click', (event) => {
  const button = event.target.closest('button[data-forget]');
  if (button) historyAction('/telegram/api/history/' + encodeURIComponent(button.dataset.forget), 'Removed from history. You can request the message again; the saved file was kept.');
});

function render(snapshot) {
  const channelNames = snapshot.channel_names || {};
  browseNames = channelNames;
  for (const option of browseChat.options) {
    if (option.value) option.textContent = channelNames[option.value.replace(/^@/, '').toLowerCase()] || option.value;
  }
  const namesSignature = JSON.stringify(channelNames);
  if (namesSignature !== channelNamesSignature) {
    channelNamesSignature = namesSignature;
    window.dispatchEvent(new CustomEvent('telegram:channel-names', { detail: channelNames }));
  }
  const wasReady = loginStep?.step === 'ready';
  renderLogin(snapshot.login);
  if (!wasReady && snapshot.login?.step === 'ready'
    && document.querySelector('#browse-tab').getAttribute('aria-selected') === 'true') loadBrowseChats();
  historyPeriod("telegram", snapshot.history_retention_days);
  paused = snapshot.paused;
  cancelling = snapshot.cancelling;
  updateControls();
  updateBrowseControls();
  statusEl.textContent = cancelling ? 'Cancelling' : paused ? 'Paused' : snapshot.next_run_at ? `Next run: ${dateTime(snapshot.next_run_at)}` : label(snapshot.status);
  statusEl.className = 'pill ' + (snapshot.status === 'running' && !paused && !cancelling ? 'ok' : '');
  const loginBadge = document.querySelector('#pill-login');
  const ready = snapshot.login?.step === 'ready';
  loginBadge.textContent = ready ? 'Connected' : 'Login required';
  loginBadge.className = 'pill ' + (ready ? 'ok' : 'err');
  document.querySelector('#task-count').textContent = snapshot.active.length ? `(${snapshot.active.length})` : '';
  if (snapshot.request_status !== requestStatus) {
    requestStatus = snapshot.request_status;
    requestEl.textContent = requestStatus;
    requestEl.classList.toggle("is-error", requestStatus.startsWith("Could not"));
  }
  filesEl.textContent = snapshot.downloaded_files.toLocaleString();
  bytesEl.textContent = sizeLabel(snapshot.downloaded_bytes);
  activeEl.textContent = snapshot.active_count;
  // Live snapshots carry only a history revision; fetch records when it moves.
  if (snapshot.history_revision !== historyRevision) {
    historyRevision = snapshot.history_revision;
    historySnapshots.invalidate();
    if (document.querySelector('#completed-tab').getAttribute('aria-selected') === 'true') {
      historySnapshots.refresh().catch(() => {});
    }
  }

  const rows = [];
  document.querySelector('#tasks-empty').style.display = snapshot.active.length ? 'none' : 'block';
  for (const item of snapshot.active) {
    const row = document.createElement('tr');
    row.dataset.rowId = `${item.msg_id}:${item.path}`;
    const state = cancelling ? 'cancelling' : paused ? 'paused' : 'running';
    const percent = Math.min(100, Math.max(0, item.percent));
    row.innerHTML = `
      <td><span class="truncate file"></span></td>
      <td class="muted"><span class="truncate source"></span></td>
      <td><span class="tag ${state}">${label(state)}</span></td>
      <td><div class="bar" role="progressbar" aria-label="Download progress" aria-valuenow="${Math.round(percent)}" aria-valuemin="0" aria-valuemax="100"><i style="width:${percent.toFixed(1)}%"></i></div><span class="muted">${percent.toFixed(0)}%</span></td>
      <td class="muted task-speed"></td>
      <td class="muted"><span class="truncate detail"></span></td>`;
    setText('.file', item.file_name, row);
    row.querySelector('.file').title = item.file_name;
    setText('.source', [item.source_name, `Message ${item.msg_id}`].filter(Boolean).join(' · '), row);
    setText('.task-speed', paused || cancelling ? '—' : sizeLabel(item.speed), row);
    setText('.detail', `${sizeLabel(item.downloaded)} / ${sizeLabel(item.total)}`, row);
    row.querySelector('.detail').title = item.path;
    rows.push(row);
  }
  reconcileRows(downloadsEl, rows);
}

let loginStep = null;
const loginForm = document.querySelector("#login-form");
const loginValue = document.querySelector("#login-value");
const loginHash = document.querySelector("#login-hash");
const loginButton = document.querySelector("#login-submit");
const loginError = document.querySelector("#login-error");
function renderLogin(login) {
  if (!login) return;
  const changed = !loginStep || loginStep.id !== login.id || loginStep.step !== login.step;
  loginStep = login;
  const ready = login.step === "ready";
  const hasInput = ["credentials", "phone", "code", "password"].includes(login.step);
  document.querySelector("#login-panel").hidden = !hasInput && login.step !== "retry";
  formEl.hidden = !ready;
  document.querySelector('#download-login-note').hidden = ready;
  document.querySelector("#login-message").textContent = login.message;
  document.querySelector("#login-value-field").hidden = !hasInput;
  document.querySelector("#login-hash-field").hidden = login.step !== "credentials";
  document.querySelector("#login-help").hidden = login.step !== "credentials";
  document.querySelector("#login-reset").hidden = login.step !== "retry";
  loginValue.required = hasInput;
  loginHash.required = login.step === "credentials";
  loginButton.disabled = login.step === "working" || ready;
  loginButton.textContent = login.step === "retry" ? "Retry login" : login.step === "phone" ? "Send code" : "Continue";
  const labels = { credentials: "API ID", phone: "Phone number", code: "Verification code", password: "Two-factor password" };
  document.querySelector("#login-value-label").textContent = labels[login.step] || "Value";
  loginValue.type = login.step === "password" ? "password" : login.step === "phone" ? "tel" : "text";
  loginValue.inputMode = ["credentials", "code"].includes(login.step) ? "numeric" : "text";
  loginValue.autocomplete = login.step === "password" ? "current-password" : login.step === "code" ? "one-time-code" : "off";
  loginValue.placeholder = login.step === "phone" ? "+49…" : "";
  if (changed) {
    loginValue.value = "";
    loginHash.value = "";
    loginError.textContent = "";
    if (hasInput) loginValue.focus();
  }
}
async function submitLogin(value) {
  loginButton.disabled = true;
  document.querySelector("#login-reset").disabled = true;
  loginError.textContent = "";
  try {
    await api("/telegram/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: loginStep.id, value, api_hash: loginHash.value }),
    });
    loginValue.value = "";
    loginHash.value = "";
  } catch (error) {
    loginError.textContent = error.message;
  } finally {
    loginButton.disabled = loginStep.step === "working" || loginStep.step === "ready";
    document.querySelector("#login-reset").disabled = false;
  }
}
loginForm.addEventListener("submit", (event) => {
  event.preventDefault();
  submitLogin(loginStep.step === "retry" ? "retry" : loginValue.value);
});
document.querySelector("#login-reset").addEventListener("click", () => submitLogin("credentials"));

const queueSnapshot = scheduleRender(render);
connectEvents("/telegram/events", {
  message: (event) => queueSnapshot(JSON.parse(event.data)),
});

if (location.hash === '#browse') document.querySelector('#browse-tab').click();
