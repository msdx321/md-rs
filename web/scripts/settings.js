import { $, api } from './shared.js';
const paths = ['host', 'telegram_download_path', 'jav_download_path', 'temp_path'];
const modules = ['telegram', 'jav'];
let current;
function visibility() {
  for (const name of modules) {
    const daily = $(`${name}-mode`).value === 'daily';
    for (const key of ['daily_time', 'interval_secs']) {
      const input = $(`${name}-${key}`);
      input.disabled = key === 'daily_time' ? !daily : daily;
      input.closest('.field').hidden = input.disabled;
    }
  }
}
function render(config) {
  current = config;
  for (const key of [...paths, 'port', 'history_retention_days']) $(key).value = config[key];
  for (const name of modules) for (const [key, value] of Object.entries(config.schedules[name])) $(`${name}-${key}`).value = String(value);
  $('common-fields').disabled = false;
  visibility();
}
$('common-settings').addEventListener('change', visibility);
$('reload-settings').addEventListener('click', () => { render(current); $('settings-result').textContent = ''; });
$('common-settings').addEventListener('submit', async event => {
  event.preventDefault();
  const config = structuredClone(current);
  for (const key of paths) config[key] = $(key).value.trim();
  config.port = Number($('port').value);
  config.history_retention_days = Number($('history_retention_days').value);
  for (const name of modules) for (const [key, value] of Object.entries(config.schedules[name])) {
    const raw = $(`${name}-${key}`).value;
    config.schedules[name][key] = typeof value === 'boolean' ? raw === 'true' : typeof value === 'number' ? Number(raw) : raw;
  }
  $('common-fields').disabled = true;
  try { render(await api('/api/config', {method:'PUT', body:JSON.stringify(config)})); $('settings-result').textContent = 'Settings saved. Restart to apply web host or port changes.'; }
  catch (error) { $('settings-result').textContent = error.message; }
  finally { $('common-fields').disabled = false; visibility(); }
});
api('/api/config').then(render).catch(error => { $('settings-result').textContent = error.message; });
