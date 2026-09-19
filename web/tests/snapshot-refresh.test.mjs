import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const source = await readFile(new URL('../scripts/snapshot-refresh.js', import.meta.url), 'utf8');
const { createSnapshotRefresh, createTaskRefresh } = await import(`data:text/javascript;base64,${Buffer.from(source).toString('base64')}`);
const tick = () => new Promise((resolve) => setImmediate(resolve));
function fixture(create = createSnapshotRefresh) {
  const reads = [];
  const applied = [];
  const errors = [];
  const resource = create((signal) => new Promise((resolve, reject) => reads.push({ resolve, reject, signal })),
    (value) => applied.push(value), (error) => errors.push(error.message));
  return { resource, reads, applied, errors };
}

test('snapshot-refresh coalesces concurrent reads and allows later refreshes', async () => {
  const { resource, reads, applied } = fixture();
  const first = resource.refresh();
  assert.equal(resource.refresh(), first);
  await tick();
  assert.equal(reads.length, 1);
  reads[0].resolve('first');
  await first;
  const second = resource.refresh();
  await tick();
  assert.equal(reads.length, 2);
  reads[1].resolve('second');
  await second;
  assert.deepEqual(applied, ['first', 'second']);
});

test('snapshot-refresh queues one fresh GET after mutation during an in-flight GET', async () => {
  const { resource, reads, applied } = fixture();
  const old = resource.refresh();
  await tick();
  resource.invalidate(); // Command started.
  const mutation = resource.refresh({ fresh: true }); // Command completed.
  resource.refresh({ fresh: true }); // Another completion coalesces.
  assert.equal(mutation, old);
  reads[0].resolve('stale before delete');
  await tick();
  assert.deepEqual(applied, []);
  assert.equal(reads.length, 2);
  reads[1].resolve('after delete');
  await mutation;
  assert.deepEqual(applied, ['after delete']);
});

test('snapshot-refresh invalidates GETs started during a mutation too', async () => {
  const { resource, reads, applied } = fixture();
  resource.invalidate();
  const poll = resource.refresh();
  await tick();
  resource.refresh({ fresh: true });
  reads[0].resolve('pre-completion');
  await tick();
  reads[1].resolve('post-completion');
  await poll;
  assert.deepEqual(applied, ['post-completion']);
});

test('snapshot-refresh ignores stale failures, reports current failures and recovers', async () => {
  const { resource, reads, applied, errors } = fixture();
  const first = resource.refresh();
  await tick();
  resource.invalidate();
  reads[0].reject(new Error('obsolete'));
  await tick();
  reads[1].reject(new Error('offline'));
  await first;
  assert.deepEqual(errors, ['offline']);
  const retry = resource.refresh();
  await tick();
  reads[2].resolve('recovered');
  await retry;
  assert.deepEqual(applied, ['recovered']);
});

test('snapshot-refresh handles synchronous read failures without retaining a settled flight', async () => {
  let calls = 0;
  const errors = [];
  const resource = createSnapshotRefresh(() => { calls++; throw new Error('sync'); },
    () => assert.fail('unexpected snapshot'), (error) => errors.push(error.message));
  await resource.refresh();
  await resource.refresh();
  assert.equal(calls, 2);
  assert.deepEqual(errors, ['sync', 'sync']);
});

for (const create of [createSnapshotRefresh, createTaskRefresh]) {
  test(`${create.name} releases a stalled read and ignores its late response`, async (t) => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const { resource, reads, applied, errors } = fixture(create);
    const stalled = resource.refresh();
    await tick();
    const action = resource.refresh({ fresh: true });
    t.mock.timers.tick(15000);
    await action;
    await stalled;
    assert.equal(reads[0].signal.aborted, true);
    assert.deepEqual(errors, ['Snapshot request timed out']);
    const recovery = resource.refresh();
    await tick();
    assert.equal(reads.length, 2);
    reads[1].resolve([{ id: 'current' }]);
    await recovery;
    reads[0].resolve([{ id: 'obsolete' }]);
    await tick();
    assert.deepEqual(applied, [[{ id: 'current' }]]);
    assert.equal(reads[1].signal.aborted, false);
  });
}

test('task refresh overlays latest SSE progress and terminals without retrying or losing other rows', async () => {
  const { resource, reads, applied } = fixture(createTaskRefresh);
  const load = resource.refresh();
  await tick();
  for (let progress = 1; progress <= 100; progress++) resource.receive({ id: 'a', progress });
  resource.receive({ id: 'c', state: 'completed' });
  reads[0].resolve([{ id: 'a', progress: 0 }, { id: 'b', state: 'paused' }, { id: 'c', state: 'running' }]);
  await load;
  assert.equal(reads.length, 1);
  assert.deepEqual(applied, [[{ id: 'a', progress: 100 }, { id: 'b', state: 'paused' }, { id: 'c', state: 'completed' }]]);
});

test('task refresh forgets pre-mutation overlays and lets a fresh snapshot remove dismissed rows', async () => {
  const { resource, reads, applied } = fixture(createTaskRefresh);
  const load = resource.refresh();
  await tick();
  resource.receive({ id: 'dismissed', state: 'failed' });
  resource.refresh({ fresh: true });
  reads[0].resolve([{ id: 'dismissed', state: 'failed' }]);
  await tick();
  resource.receive({ id: 'new', state: 'running' });
  reads[1].resolve([]);
  await load;
  assert.deepEqual(applied, [[{ id: 'new', state: 'running' }]]);
});
