import { $, api, scheduleRender } from './shared.js';
import { bindSettingPresets, presets } from './settings-presets.js';

const syncPresets = bindSettingPresets({
  max_download_task: presets.concurrency,
  download_connections: presets.concurrency,
  'formats-video': [['all', 'All video formats'], ['mp4', 'MP4 only'], ['mp4, mkv', 'MP4 and MKV']],
  'formats-audio': [['all', 'All audio formats'], ['mp3', 'MP3 only'], ['mp3, flac', 'MP3 and FLAC']],
  'formats-document': [['all', 'All document formats'], ['pdf', 'PDF only'], ['pdf, zip', 'PDF and ZIP']],
  file_path_prefix: [['', 'No subfolders'], ['chat_title', 'By chat'], ['chat_title, media_datetime', 'By chat and date'], ['chat_title, media_type', 'By chat and media type']],
  file_name_prefix: [['', 'Message ID (default)'], ['message_id, file_name', 'Message ID + original filename'], ['file_name', 'Original filename'], ['message_id, caption', 'Message ID + caption']],
  file_name_prefix_split: [['_', 'Underscore ( _ )'], ['-', 'Hyphen ( - )'], [' ', 'Space'], ['', 'No separator']],
  date_format: [['%Y_%m', 'Year and month · 2026_09'], ['%Y-%m-%d', 'Full date · 2026-09-15'], ['%Y', 'Year · 2026']],
});

const form = $('settings-form');
const fields = $('settings-fields');
const save = $('btn-save');
const discard = $('btn-discard');
const note = $('save-note');
const error = $('settings-error');
const strings = ['file_name_prefix_split', 'date_format'];
const numbers = ['max_download_task', 'download_connections'];
const lists = ['file_path_prefix', 'file_name_prefix'];
const formats = ['audio', 'video', 'document'];
const queueUpdate = scheduleRender(update);
const queueChatSearch = scheduleRender(updateChatEmpty);
let saved = null;
let saving = false;
let chatSequence = 0;
let originalChats = [];
let channelNames = {};
function channelName(chatId) {
  const id = chatId.trim();
  const key = id.replace(/^@/, '').toLowerCase();
  return (Object.hasOwn(channelNames, id) && channelNames[id])
    || (Object.hasOwn(channelNames, key) && channelNames[key])
    || id;
}
function updateChatSummary(row) {
  const chatId = row.querySelector('[data-chat-id]').value.trim();
  row.querySelector('[data-chat-name]').textContent = channelName(chatId) || 'New subscription';
  row.querySelector('[data-chat-preview]').textContent = row.querySelector('[data-chat-filter]').value.trim() || 'All matching media';
}
function updateChatEmpty() {
  const rows = [...$('subscribed-chats').children];
  const query = $('chat-search').value.trim().toLowerCase();
  let visible = 0;
  for (const row of rows) {
    const chatId = row.querySelector('[data-chat-id]').value;
    const text = `${channelName(chatId)} ${chatId} ${row.querySelector('[data-chat-filter]').value}`.toLowerCase();
    row.hidden = query !== '' && !text.includes(query);
    if (!row.hidden) visible++;
  }
  $('subscription-count').textContent = rows.length;
  $('chat-search-count').textContent = query ? `${visible} of ${rows.length} chats` : `${rows.length} subscribed ${rows.length === 1 ? 'chat' : 'chats'}`;
  $('no-subscriptions').hidden = rows.length > 0;
  $('no-chat-matches').hidden = rows.length === 0 || visible > 0;
}
window.addEventListener('telegram:channel-names', (event) => {
  channelNames = event.detail || {};
  for (const row of $('subscribed-chats').children) updateChatSummary(row);
  updateChatEmpty();
});
$('chat-search').addEventListener('input', queueChatSearch);
$('chat-search').addEventListener('keydown', (event) => {
  if (event.key === 'Enter') event.preventDefault();
});
function addChat(chat = { chat_id: '', download_filter: null }) {
  const row = document.createElement('details');
  row.className = 'subscription-row settings-item';
  const id = ++chatSequence;
  row.innerHTML = `<summary><span class="settings-item-icon" aria-hidden="true">#</span><span class="settings-item-summary"><strong data-chat-name></strong><span data-chat-preview></span></span><span class="settings-item-edit">Edit</span></summary>
    <div class="settings-item-fields">
    <div class="field"><label for="chat-id-${id}">Chat username or ID</label><input id="chat-id-${id}" data-chat-id required placeholder="@channel or -1001234567890"><span class="hint">Your Telegram account must have access to this chat.</span></div>
    <div class="field"><label for="chat-cursor-${id}">Last read message ID</label><input id="chat-cursor-${id}" data-chat-cursor type="number" min="0" max="2147483647" step="1" placeholder="Keep saved position"><span class="hint">Optional. Blank keeps the saved position; 0 scans from the beginning.</span></div>
    <div class="field full"><label for="chat-filter-${id}">Download filter</label><input id="chat-filter-${id}" data-chat-filter placeholder="media_type == 'video'"><span class="hint">Optional. For example: media_type == 'video' and file_size &gt; 10MB. Blank includes all matching media.</span></div>
    <div class="settings-item-actions full"><button type="button" class="danger tiny" data-remove-chat>Remove subscription</button></div></div>`;
  row.querySelector('[data-chat-id]').value = chat.chat_id;
  row.querySelector('[data-chat-filter]').value = chat.download_filter || '';
  row.querySelector('[data-chat-cursor]').value = chat.last_read_message_id ?? '';
  const updateSummary = () => updateChatSummary(row);
  updateSummary();
  row.addEventListener('input', updateSummary);
  row.querySelector('[data-remove-chat]').addEventListener('click', () => {
    const next = row.nextElementSibling || row.previousElementSibling;
    row.remove();
    updateChatEmpty();
    update();
    (next && !next.hidden ? next.querySelector('summary') : $('btn-add-chat')).focus();
  });
  $('subscribed-chats').append(row);
  return row;
}
$('btn-add-chat').addEventListener('click', () => {
  $('chat-search').value = '';
  const row = addChat();
  updateChatEmpty();
  row.open = true;
  row.querySelector('input').focus();
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
  syncPresets();
  update();
}

// A required field may be inside a collapsed or filtered subscription.
form.addEventListener('invalid', (event) => {
  const row = event.target.closest('.subscription-row');
  if (row) {
    $('chat-search').value = '';
    updateChatEmpty();
    row.open = true;
  }
}, true);

function update() {
  const dirty = saved !== null && JSON.stringify(read()) !== saved;
  save.disabled = saving || !dirty;
  discard.disabled = saving || !dirty;
  note.classList.toggle('is-dirty', dirty);
  note.textContent = saving ? 'Saving…' : dirty ? 'Unsaved changes' : saved ? 'All changes saved' : 'Loading settings…';
}

form.addEventListener('input', queueUpdate);
form.addEventListener('change', queueUpdate);
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
