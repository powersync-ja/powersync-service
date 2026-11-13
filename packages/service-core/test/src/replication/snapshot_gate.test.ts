import { expect, test } from 'vitest';
import { SnapshotGate } from '../../../src/replication/SnapshotGate.js';

test('snapshots wait for every backlogged stream, including failed or stopped attempts', async () => {
  const gate = new SnapshotGate();
  using first = gate.trackStreaming();
  const second = gate.trackStreaming();
  first.setBacklogged(true);
  first.setBacklogged(true);
  second.setBacklogged(true);
  let resumed = false;
  const waiting = gate.wait(new AbortController().signal).then(() => {
    resumed = true;
  });
  first.setBacklogged(false);
  await Promise.resolve();
  expect(resumed).toBe(false);
  second[Symbol.dispose]();
  second[Symbol.dispose]();
  await waiting;
  expect(resumed).toBe(true);
  second.setBacklogged(true);
  expect(gate.isPaused).toBe(false);
});

test('shutdown cancels a paused snapshot without releasing another stream backlog', async () => {
  const gate = new SnapshotGate();
  using stream = gate.trackStreaming();
  stream.setBacklogged(true);
  const abort = new AbortController();
  const failure = new Error('shutdown');
  const waiting = expect(gate.wait(abort.signal)).rejects.toBe(failure);
  abort.abort(failure);
  await waiting;
  expect(gate.isPaused).toBe(true);
  stream.setBacklogged(false);
  await gate.wait(new AbortController().signal);
  await expect(gate.wait(abort.signal)).rejects.toBe(failure);
});

test('a newly backlogged stream keeps an awakened snapshot paused', async () => {
  const gate = new SnapshotGate();
  using stream = gate.trackStreaming();
  stream.setBacklogged(true);
  let resumed = false;
  const waiting = gate.wait(new AbortController().signal).then(() => {
    resumed = true;
  });
  stream.setBacklogged(false);
  stream.setBacklogged(true);
  await Promise.resolve();
  expect(resumed).toBe(false);
  stream.setBacklogged(false);
  await waiting;
});
