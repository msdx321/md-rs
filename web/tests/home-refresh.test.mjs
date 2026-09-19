import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

const source = await readFile(new URL('../scripts/home.js', import.meta.url), 'utf8');
const refreshSource = await readFile(new URL('../scripts/snapshot-refresh.js', import.meta.url), 'utf8');
const refresh = await import(`data:text/javascript;base64,${Buffer.from(refreshSource).toString('base64')}`);
const tick = () => new Promise((resolve) => setImmediate(resolve));
function dashboard(api) {
  const handlers = new Map();
  const elements = new Map();
  let poll;
  const context = vm.createContext({
    ...refresh,
    api,
    $: (id) => {
      if (!elements.has(id)) elements.set(id, {});
      return elements.get(id);
    },
    // This harness counts requests; DOM rendering is verified in the mock browser.
    scheduleRender: () => () => {},
    connectEvents: (path, events) => handlers.set(path, events),
    pollWhenVisible: (callback, interval) => { assert.equal(interval, 30000); poll = callback; },
  });
  vm.runInContext(source.replace(/^import .*;\n/gm, ''), context);
  return { handlers, elements, poll: () => poll() };
}

test('home refresh: two provider opens issue four GETs, terminal refresh is provider-local, polling still reconciles both', async () => {
  const requests = [];
  const home = dashboard(async (path) => {
    requests.push(path);
    return path.endsWith('/history') ? { records: [] } : [];
  });
  home.handlers.get('/jav/api/events').open();
  home.handlers.get('/p91/api/events').open();
  await tick();
  assert.deepEqual(requests.sort(), ['/jav/api/history', '/jav/api/tasks', '/p91/api/history', '/p91/api/tasks']);
  requests.length = 0;
  home.handlers.get('/jav/api/events').task({ data: JSON.stringify({ id: 'done', state: 'completed' }) });
  await tick();
  assert.deepEqual(requests, ['/jav/api/history']);
  requests.length = 0;
  await home.poll();
  assert.deepEqual(requests.sort(), ['/jav/api/history', '/jav/api/tasks', '/p91/api/history', '/p91/api/tasks']);
});

test('home refresh: terminal bursts during history GET coalesce into one follow-up, not cross-provider GETs', async () => {
  const requests = [];
  const reads = [];
  const home = dashboard((path) => {
    requests.push(path);
    if (path === '/jav/api/history') return new Promise((resolve) => reads.push(resolve));
    return Promise.resolve([]);
  });
  const jav = home.handlers.get('/jav/api/events');
  jav.open();
  await tick();
  for (let i = 0; i < 20; i++) jav.task({ data: JSON.stringify({ id: `done-${i}`, state: 'completed' }) });
  reads[0]({ records: [] });
  await tick();
  assert.deepEqual(requests, ['/jav/api/tasks', '/jav/api/history', '/jav/api/history']);
  reads[1]({ records: [] });
  await tick();
});

test('home refresh: successful provider cannot hide another provider failure', async () => {
  let failing = true;
  const home = dashboard(async (path) => {
    if (path === '/jav/api/history' && failing) throw new Error('mock failure');
    return path.endsWith('/history') ? { records: [] } : [];
  });
  home.handlers.get('/jav/api/events').open();
  await tick();
  assert.equal(home.elements.get('history-error').hidden, false);
  home.handlers.get('/p91/api/events').open();
  await tick();
  assert.equal(home.elements.get('history-error').hidden, false);
  failing = false;
  await home.poll();
  assert.equal(home.elements.get('history-error').hidden, true);
});
