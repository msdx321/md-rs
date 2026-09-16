import { $, api } from './shared.js';

const form = $('settings-form');
const fields = $('settings-fields');
const save = $('btn-save');
const discard = $('btn-discard');
const note = $('save-note');
const error = $('settings-error');
const strings = ['save_path', 'file_name_prefix_split', 'date_format'];
const numbers = ['max_download_task', 'download_connections', 'check_interval_secs'];
const lists = ['file_path_prefix', 'file_name_prefix'];
const formats = ['audio', 'video', 'document'];
let saved = null;
let saving = false;
let chatSequence = 0;
let originalChats = [];
function updateChatEmpty() {
  $('no-subscriptions').hidden = $('subscribed-chats').children.length > 0;
}
function addChat(chat = { chat_id: '', download_filter: null }) {
  const row = document.createElement('div');
  row.className = 'subscription-row';
  const id = ++chatSequence;
  row.innerHTML = `<div class="field"><label for="chat-id-${id}">Chat username or ID</label><input id="chat-id-${id}" data-chat-id required placeholder="@channel or -1001234567890"></div>
    <div class="field"><label for="chat-filter-${id}">Download filter (optional)</label><input id="chat-filter-${id}" data-chat-filter placeholder="media_type == 'video'"></div>
    <div class="field"><label for="chat-cursor-${id}">Last read message ID (optional)</label><input id="chat-cursor-${id}" data-chat-cursor type="number" min="0" max="2147483647" step="1" placeholder="Keep saved cursor"><span class="hint">Blank keeps the saved cursor. Set 0 to scan from the beginning.</span></div>
    <button type="button" class="danger" data-remove-chat>Remove</button>`;
  row.querySelector('[data-chat-id]').value = chat.chat_id;
  row.querySelector('[data-chat-filter]').value = chat.download_filter || '';
  row.querySelector('[data-chat-cursor]').value = chat.last_read_message_id ?? '';
  row.querySelector('[data-remove-chat]').addEventListener('click', () => {
    row.remove();
    updateChatEmpty();
    update();
  });
  $('subscribed-chats').append(row);
  updateChatEmpty();
  return row;
}
$('btn-add-chat').addEventListener('click', () => {
  addChat().querySelector('input').focus();
  update();
});
const split = (value) => value.split(',').map((item) => item.trim()).filter(Boolean);

function read() {
  const config = { chat: [...$('subscribed-chats').children].map(row => ({
    chat_id: row.querySelector('[data-chat-id]').value.trim(),
    download_filter: row.querySelector('[data-chat-filter]').value.trim() || null,
    last_read_message_id: row.querySelector('[data-chat-cursor]').value === '' ? null : Number(row.querySelector('[data-chat-cursor]').value),
  })) };
  for (const id of strings) config[id] = $(id).value;
  for (const id of numbers) config[id] = Number($(id).value);
  for (const id of lists) config[id] = split($(id).value);
  config.file_formats = Object.fromEntries(formats.map((kind) => [kind, split($('formats-' + kind).value)]));
  config.media_types = [...form.querySelectorAll('[name="media_type"]:checked')].map((input) => input.value);
  return config;
}

function show(config, resetBaseline = true) {
  if (resetBaseline) originalChats = structuredClone(config.chat || []);
  $('subscribed-chats').replaceChildren();
  for (const chat of config.chat || []) addChat(chat);
  updateChatEmpty();
  for (const id of [...strings, ...numbers]) $(id).value = config[id];
  for (const id of lists) $(id).value = config[id].join(', ');
  for (const kind of formats) $('formats-' + kind).value = config.file_formats[kind].join(', ');
  form.querySelectorAll('[name="media_type"]').forEach((input) => { input.checked = config.media_types.includes(input.value); });
  saved = JSON.stringify(read());
  update();
}

function update() {
  const dirty = saved !== null && JSON.stringify(read()) !== saved;
  save.disabled = saving || !dirty;
  discard.disabled = saving || !dirty;
  note.classList.toggle('is-dirty', dirty);
  note.textContent = saving ? 'Saving…' : dirty ? 'Unsaved changes' : saved ? 'All changes saved' : 'Loading settings…';
}

form.addEventListener('input', update);
form.addEventListener('change', update);
discard.addEventListener('click', () => {
  show(JSON.parse(saved), false);
  error.hidden = true;
});
form.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (saving || saved === null) return;
  const config = read();
  const previous = JSON.parse(saved).chat;
  if (JSON.stringify(config.chat) === JSON.stringify(previous)) delete config.chat;
  else config.original_chat = originalChats;
  saving = true;
  fields.disabled = true;
  error.hidden = true;
  update();
  try {
    show(await api('/telegram/api/config', { method: 'PUT', body: JSON.stringify(config) }));
  } catch (reason) {
    error.textContent = reason.message;
    error.hidden = false;
  } finally {
    saving = false;
    fields.disabled = false;
    update();
  }
});
window.addEventListener('beforeunload', (event) => {
  if (saved !== null && JSON.stringify(read()) !== saved) event.preventDefault();
});
api('/telegram/api/config').then((config) => {
  show(config);
  fields.disabled = false;
}).catch((reason) => {
  error.textContent = reason.message;
  error.hidden = false;
  note.textContent = 'Could not load settings. Reload to try again.';
});
