import { $, api, scheduleRender } from './shared.js';
const paths = ['host', 'telegram_download_path', 'jav_download_path', 'temp_path'];
const modules = ['telegram', 'jav'];
let current;
let saving = false;
let saved = '';
const queueUpdate = scheduleRender(update);
function visibility() {
  for (const name of modules) {
    const daily = $(`${name}-mode`).value === 'daily';
    const enabled = $(`${name}-enabled`).value === 'true';
    for (const key of ['mode', 'daily_time', 'interval_secs', 'run_on_start']) {
      const input = $(`${name}-${key}`);
      const inactive = key === 'daily_time' ? !daily : key === 'interval_secs' ? daily : false;
      input.disabled = !enabled || inactive;
      input.closest('.field').hidden = inactive;
    }
  }
}
function read() {
  const config = structuredClone(current);
  for (const key of paths) config[key] = $(key).value.trim();
  config.port = Number($('port').value);
  config.history_retention_days = Number($('history_retention_days').value);
  for (const name of modules) for (const [key, value] of Object.entries(config.schedules[name])) {
    const raw = $(`${name}-${key}`).value;
    config.schedules[name][key] = typeof value === 'boolean' ? raw === 'true' : typeof value === 'number' ? Number(raw) : raw;
  }
  return config;
}
function changed() { return current && JSON.stringify(read()) !== saved; }
function update() {
  const dirty = changed();
  $('save-settings').disabled = saving || !dirty;
  $('reload-settings').disabled = saving || !dirty;
  $('save-note').classList.toggle('is-dirty', Boolean(dirty));
  $('save-note').textContent = saving ? 'Saving changes…' : dirty ? 'Unsaved changes' : current ? 'All changes saved' : 'Loading settings…';
}
function render(config) {
  current = config;
  saved = JSON.stringify(config);
  for (const key of [...paths, 'port', 'history_retention_days']) $(key).value = config[key];
  for (const name of modules) for (const [key, value] of Object.entries(config.schedules[name])) $(`${name}-${key}`).value = String(value);
  visibility();
  update();
}
$('common-settings').addEventListener('input', () => { $('settings-result').hidden = true; queueUpdate(); });
$('common-settings').addEventListener('change', () => { visibility(); queueUpdate(); });
$('reload-settings').addEventListener('click', () => { render(current); $('settings-result').hidden = true; });
$('common-settings').addEventListener('submit', async event => {
  event.preventDefault();
  if (saving || !changed()) return;
  const config = read();
  const restart = config.host !== current.host || config.port !== current.port;
  saving = true;
  $('common-fields').disabled = true;
  $('settings-result').hidden = true;
  update();
  try {
    render(await api('/api/config', {method:'PUT', body:JSON.stringify(config)}));
    $('settings-result').textContent = restart ? 'Settings saved. Restart the application to apply host or port changes.' : 'Settings saved.';
    $('settings-result').className = 'banner';
  } catch (error) {
    $('settings-result').textContent = error.message;
    $('settings-result').className = 'banner err';
  } finally {
    saving = false;
    $('common-fields').disabled = false;
    $('settings-result').hidden = false;
    visibility();
    update();
  }
});
window.addEventListener('beforeunload', event => { if (changed()) event.preventDefault(); });
api('/api/config').then(config => {
  render(config);
  $('common-fields').disabled = false;
}).catch(error => {
  $('settings-result').textContent = error.message;
  $('settings-result').className = 'banner err';
  $('settings-result').hidden = false;
  $('save-note').textContent = 'Could not load settings. Reload to try again.';
});
