import "./telegram-settings.js";
import { api, bindTabs, connectEvents } from "./shared.js";

const statusEl = document.querySelector("#status");
const requestEl = document.querySelector("#request-status");
const filesEl = document.querySelector("#downloaded-files");
const bytesEl = document.querySelector("#downloaded-bytes");
const activeEl = document.querySelector("#active-count");
const downloadsEl = document.querySelector("#downloads");
const completedEl = document.querySelector("#completed-downloads");
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
}
const formEl = document.querySelector("#download-form");
const linkEl = document.querySelector("#chat-link");
const actionEls = [...document.querySelectorAll("button[name=action]")];
let paused = false;

bindTabs(tabEls);

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

function setText(selector, value, root = document) {
  root.querySelector(selector).textContent = value;
}

function render(snapshot) {
  renderLogin(snapshot.login);
  paused = snapshot.paused;
  cancelling = snapshot.cancelling;
  updateControls();
  statusEl.textContent = snapshot.status;
  requestEl.textContent = snapshot.request_status;
  requestEl.classList.toggle("is-error", snapshot.request_status.startsWith("Could not"));
  filesEl.textContent = snapshot.downloaded_files;
  bytesEl.textContent = snapshot.downloaded_bytes;
  activeEl.textContent = snapshot.active_count;

  completedEl.replaceChildren();
  if (snapshot.completed.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No files downloaded in the last 30 days";
    completedEl.append(empty);
  }
  for (const item of snapshot.completed) {
    const card = document.createElement("div");
    card.className = "download-card";
    card.innerHTML = `
      <div class="download-head">
        <strong class="file"></strong>
        <span class="size"></span>
      </div>
      <div class="download-meta">
        <time class="completed-at"></time>
        <span class="msg"></span>
      </div>
      <div class="path"></div>
    `;
    setText(".file", item.file_name, card);
    setText(".size", item.size, card);
    setText(".msg", `msg ${item.msg_id}`, card);
    setText(".path", item.path, card);
    const date = new Date(item.completed_at);
    const time = card.querySelector("time");
    time.dateTime = date.toISOString();
    time.textContent = `Saved ${date.toLocaleString()}`;
    completedEl.append(card);
  }

  downloadsEl.replaceChildren();
  if (snapshot.active.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty";
    empty.textContent = "No tasks. Completed downloads appear in History.";
    downloadsEl.append(empty);
    return;
  }

  for (const item of snapshot.active) {
    const card = document.createElement("div");
    card.className = "download-card";
    card.innerHTML = `
      <div class="download-head">
        <strong class="file"></strong>
        <span class="speed"></span>
      </div>
      <progress class="download-progress" aria-label="Download progress" max="100"></progress>
      <div class="download-meta">
        <span class="progress-text"></span>
        <span class="msg"></span>
      </div>
      <div class="path"></div>
    `;
    setText(".file", item.file_name, card);
    setText(".speed", item.speed, card);
    setText(".progress-text", `${item.percent.toFixed(1)}% (${item.downloaded}/${item.total})`, card);
    setText(".msg", `msg ${item.msg_id}`, card);
    setText(".path", item.path, card);
    card.querySelector("progress").value = Math.min(100, Math.max(0, item.percent));
    downloadsEl.append(card);
  }
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
  document.querySelector("#login-panel").hidden = ready;
  formEl.hidden = !ready;
  document.querySelector("#login-message").textContent = login.message;
  const hasInput = ["credentials", "phone", "code", "password"].includes(login.step);
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

connectEvents("/telegram/events", {
  message: (event) => render(JSON.parse(event.data)),
});
