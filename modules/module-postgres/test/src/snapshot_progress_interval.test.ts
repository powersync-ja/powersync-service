import * as bson from 'bson';
import { performance } from 'node:perf_hooks';
import { expect, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

const cases = ['key scan', 'cursor'].flatMap((mode) => [
  { mode, interval: undefined, times: [0, 9999, 10000, 10001], counts: [3, 4] },
  { mode, interval: 50, times: [0, 49, 50, 51], counts: [3, 4] },
  { mode, interval: 0, times: [0, 0, 0, 0], counts: [1, 2, 3, 4, 4] },
  { mode, interval: 60000, times: [0, 1, 2, 3], counts: [4] }
]);

async function open(mode: string, snapshotProgressIntervalMs?: number) {
  const context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
    storageVersion: 4,
    walStreamOptions: { snapshotChunkLength: 1, snapshotProgressIntervalMs }
  });
  await context.updateSyncRules(`
config:
  edition: 3
streams:
  items:
    query: SELECT * FROM test_data
`);
  await context.pool.query(
    mode === 'key scan'
      ? 'CREATE TABLE test_data(id integer primary key, other integer, description text)'
      : 'CREATE TABLE test_data(id integer, other integer, description text, PRIMARY KEY(id, other))'
  );
  await context.pool.query(
    "INSERT INTO test_data VALUES (1, 1, 'first'), (2, 2, 'second'), (3, 3, 'third'), (4, 4, 'fourth')"
  );
  return context;
}

test.skipIf(!env.TEST_MONGO_STORAGE).each(cases)(
  'snapshot progress interval $interval ($mode)',
  async ({ mode, interval, times, counts }) => {
    await using context = await open(mode, interval);
    let now = 0;
    using _clock = vi.spyOn(performance, 'now').mockImplementation(() => now);
    const observed: number[] = [];
    const order: string[] = [];
    let rows = 0;
    const create = context.storage!.createWriter.bind(context.storage!);
    using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (...args) => {
      const writer = await create(...args);
      const save = writer.save.bind(writer);
      vi.spyOn(writer, 'save').mockImplementation(async (record) => {
        const result = await save(record);
        now = times[rows++];
        return result;
      });
      const flush = writer.flush.bind(writer);
      vi.spyOn(writer, 'flush').mockImplementation(async (...args) => {
        const result = await flush(...args);
        order.push('flush');
        return result;
      });
      const update = writer.updateTableProgress.bind(writer);
      vi.spyOn(writer, 'updateTableProgress').mockImplementation(async (table, status) => {
        if (status.replicatedCount != null) {
          expect(order.at(-1)).toBe('flush');
          if (mode === 'key scan') {
            expect(Number(bson.deserialize(status.lastKey!).id)).toBe(status.replicatedCount);
          } else {
            expect(status.lastKey).toBeUndefined();
          }
          observed.push(status.replicatedCount);
          order.push('progress');
        }
        return update(table, status);
      });
      return writer;
    });
    await context.replicateSnapshot();
    expect(rows).toBe(4);
    expect(observed).toEqual(counts);
  }
);

test.skipIf(!env.TEST_MONGO_STORAGE)('graceful abort persists the last complete page before the interval', async () => {
  const context = await open('key scan', 60000);
  let disposal: Promise<void> | undefined;
  let rows = 0;
  const observed: number[] = [];
  const create = context.storage!.createWriter.bind(context.storage!);
  using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (...args) => {
    const writer = await create(...args);
    const save = writer.save.bind(writer);
    vi.spyOn(writer, 'save').mockImplementation(async (record) => {
      const result = await save(record);
      if (++rows === 2) {
        // dispose sets the abort flag immediately, then waits for replication.
        // Do not await it from inside the page it is allowing to finish.
        disposal = context.dispose();
      }
      return result;
    });
    const update = writer.updateTableProgress.bind(writer);
    vi.spyOn(writer, 'updateTableProgress').mockImplementation(async (...args) => {
      const result = await update(...args);
      if (args[1].replicatedCount != null) {
        observed.push(args[1].replicatedCount);
      }
      return result;
    });
    return writer;
  });
  try {
    await expect(context.replicateSnapshot()).rejects.toThrow();
  } finally {
    await (disposal ?? context.dispose());
  }
  expect(rows).toBe(2);
  expect(observed).toEqual([2]);
});
