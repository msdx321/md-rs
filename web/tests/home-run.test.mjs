import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

const source = await readFile(new URL('../scripts/home.js', import.meta.url), 'utf8');
const javResult = 'scheduled: 6/7 completed, 10 attempted, 2 failed candidates, 161 skipped; /dm597/en/fc2?sort=weekly_views: 3/3 completed, 0 failed candidates; /dm2208642/en/heyzo?sort=weekly_views: 1/2 completed, 1 failed candidates; not enough eligible videos within 5 listing page(s); /dm5199603/en/1pondo?sort=weekly_views: 2/2 completed, 1 failed candidates; quotas not reached';

function dashboard() {
  const elements = new Map();
  const element = () => ({
    hidden: false,
    textContent: '',
    children: [],
    replaceChildren(...children) { this.children = children; },
  });
  const context = vm.createContext({
    $: (id) => {
      if (!elements.has(id)) elements.set(id, element());
      return elements.get(id);
    },
    document: { createElement: element },
    scheduleRender: () => () => {},
    createSnapshotRefresh: () => ({}),
    createTaskRefresh: () => ({}),
    connectEvents: () => {},
    pollWhenVisible: () => {},
  });
  vm.runInContext(source.replace(/^import .*;\n/gm, ''), context);
  const signature = { value: undefined };
  return {
    get: (id) => elements.get(id),
    render: (prefix, last_result, running = false) => context.renderRun(prefix, { last_result, running }, signature),
  };
}

test('JAV candidate failures render as badges with the full breakdown in run details', () => {
  const home = dashboard();
  home.render('jav', javResult);
  assert.equal(home.get('jav-status').textContent, 'Last scheduled run · Incomplete');
  const counts = home.get('jav-run-counts');
  assert.equal(counts.hidden, false);
  assert.deepEqual(counts.children.map(item => item.textContent), ['6 / 7 completed', '10 attempted', '2 failed candidates', '161 skipped']);
  assert.equal(counts.children[2].className, 'tag failed');
  assert.equal(home.get('jav-run-details').hidden, false);
  assert.deepEqual(home.get('jav-run-breakdown').children.map(item => item.textContent), javResult.split('; ').slice(1));
});

for (const prefix of ['jav', 'p91']) {
  test(`${prefix} retains support for the original result format`, () => {
    const home = dashboard();
    home.render(prefix, 'manual: 2/2 completed, 2 attempted, 0 failed, 4 skipped; /popular: 2/2 completed');
    assert.equal(home.get(`${prefix}-status`).textContent, 'Last manual run');
    assert.equal(home.get(`${prefix}-run-counts`).hidden, false);
    assert.deepEqual(home.get(`${prefix}-run-counts`).children.map(item => item.textContent), ['2 / 2 completed', '2 attempted', '0 failed', '4 skipped']);
    assert.equal(home.get(`${prefix}-run-counts`).children[2].className, 'tag ');
    assert.equal(home.get(`${prefix}-run-details`).hidden, false);
  });
}

test('a JAV run that reaches its quota still shows candidate failures without an incomplete label', () => {
  const home = dashboard();
  home.render('jav', 'scheduled: 7/7 completed, 9 attempted, 2 failed candidates, 1 skipped; /popular: 7/7 completed, 2 failed candidates; all quotas reached');
  assert.equal(home.get('jav-status').textContent, 'Last scheduled run');
  assert.equal(home.get('jav-run-counts').children[2].textContent, '2 failed candidates');
});

test('stopped JAV runs retain their outcome and unchanged snapshots preserve expanded details', () => {
  const home = dashboard();
  const result = javResult.replace('quotas not reached', 'stopped by user');
  home.render('jav', result);
  assert.equal(home.get('jav-status').textContent, 'Last scheduled run · Stopped');
  const details = home.get('jav-run-details');
  details.open = true;
  const children = home.get('jav-run-breakdown').children;
  home.render('jav', result);
  assert.equal(details.open, true);
  assert.equal(home.get('jav-run-breakdown').children, children);
});

test('running jobs and free-form status messages hide the previous run badges and details', () => {
  const home = dashboard();
  home.render('jav', javResult);
  home.render('jav', javResult, true);
  assert.equal(home.get('jav-status').textContent, 'Scheduled job running');
  assert.equal(home.get('jav-run-counts').hidden, true);
  assert.equal(home.get('jav-run-details').hidden, true);
  home.render('jav', javResult);
  assert.equal(home.get('jav-run-counts').hidden, false);
  home.render('jav', 'listing failed: connection timed out');
  assert.equal(home.get('jav-status').textContent, 'listing failed: connection timed out');
  assert.equal(home.get('jav-run-counts').hidden, true);
  assert.equal(home.get('jav-run-details').hidden, true);
  home.render('jav', '');
  assert.equal(home.get('jav-status').textContent, 'Ready for downloads');
});
