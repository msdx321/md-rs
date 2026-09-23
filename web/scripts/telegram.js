import "./telegram-settings.js";
import { createSnapshotRefresh } from "./snapshot-refresh.js";
import { api, bindTabs, bindHistoryPagination, connectEvents, scheduleRender, reconcileRows, historyPeriod, dateTime, label, sizeLabel } from "./shared.js";

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
const historyPager = bindHistoryPagination(renderHistoryPage);

bindTabs(tabEls, (tab) => {
  if (tab.id === 'completed-tab') loadHistory();
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
  const namesSignature = JSON.stringify(channelNames);
  if (namesSignature !== channelNamesSignature) {
    channelNamesSignature = namesSignature;
    window.dispatchEvent(new CustomEvent('telegram:channel-names', { detail: channelNames }));
  }
  renderLogin(snapshot.login);
  historyPeriod("telegram", snapshot.history_retention_days);
  paused = snapshot.paused;
  cancelling = snapshot.cancelling;
  updateControls();
  statusEl.textContent = cancelling ? 'Cancelling' : paused ? 'Paused' : snapshot.next_run_at ? `Next run: ${dateTime(snapshot.next_run_at)}` : label(snapshot.status);
  statusEl.className = 'pill ' + (snapshot.status === 'running' && !paused && !cancelling ? 'ok' : '');
  const loginBadge = document.querySelector('#pill-login');
  const ready = snapshot.login?.step === 'ready';
  loginBadge.textContent = ready ? 'Connected' : 'Login required';
  loginBadge.className = 'pill ' + (ready ? 'ok' : 'err');
  document.querySelector('#task-count').textContent = snapshot.active.length ? `(${snapshot.active.length})` : '';
  requestEl.textContent = snapshot.request_status;
  requestEl.classList.toggle("is-error", snapshot.request_status.startsWith("Could not"));
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
      <td class="muted"><span class="truncate detail"></span></td>
      <td></td>`;
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
