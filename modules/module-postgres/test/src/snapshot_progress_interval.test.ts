import { storage } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { MongoBucketStorageOptions, test_utils as mongoTestUtils } from '@powersync/service-module-mongodb-storage';
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

async function open(
  mode: string,
  snapshotProgressIntervalMs?: number,
  options?: { factory: storage.TestStorageFactory; storageVersion: number; abortSignal: AbortSignal }
) {
  const context = await WalStreamTestContext.open(options?.factory ?? INITIALIZED_MONGO_STORAGE_FACTORY.factory, {
    storageVersion: options?.storageVersion ?? 4,
    walStreamOptions: {
      snapshotChunkLength: 1,
      snapshotProgressIntervalMs,
      ...(options == null ? {} : { abort_signal: options.abortSignal })
    }
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

const abortCases = [3, 4].flatMap((storageVersion) => ['key scan', 'cursor'].map((mode) => ({ storageVersion, mode })));

test.skipIf(!env.TEST_MONGO_STORAGE).each(abortCases)(
  'graceful abort persists offloaded snapshot progress before the interval (v$storageVersion, $mode)',
  async ({ storageVersion, mode }) => {
    const abort = new AbortController();
    const objects = new Map<
      string,
      { data: Uint8Array; metadata: { contentType: string; contentEncoding: string | null } }
    >();
    // Only external object I/O is in memory; packing, uploads and MongoDB publication are real.
    const objectStorage: NonNullable<MongoBucketStorageOptions['objectStorage']> = {
      async put(path, data, metadata, options) {
        options?.signal?.throwIfAborted();
        expect(abort.signal.aborted).toBe(true);
        objects.set(path, { data, metadata });
      },
      async get(path, options) {
        options?.signal?.throwIfAborted();
        const object = objects.get(path);
        if (object == null) {
          throw new Error(`Object not found: ${path}`);
        }
        return object;
      },
      async *list(prefix, options) {
        for (const path of objects.keys()) {
          options?.signal?.throwIfAborted();
          if (path.startsWith(prefix)) {
            yield path;
          }
        }
      },
      async delete(paths, options) {
        options?.signal?.throwIfAborted();
        for (const path of paths) {
          objects.delete(path);
        }
      },
      async deletePrefix(prefix, options) {
        const paths = [];
        for await (const path of this.list(prefix, options)) {
          paths.push(path);
        }
        await this.delete(paths, options);
        return { objectCount: paths.length };
      }
    };
    const { factory } = mongoTestUtils.mongoTestStorageFactoryGenerator({
      url: env.MONGO_TEST_URL,
      isCI: env.CI,
      objectStorage,
      inlineThresholdBytes: 0
    });
    await using context = await open(mode, 60000, { factory, storageVersion, abortSignal: abort.signal });
    let rows = 0;
    let sourceTable: storage.SourceTable | undefined;
    const observed: number[] = [];
    const create = context.storage!.createWriter.bind(context.storage!);
    using _create = vi.spyOn(context.storage!, 'createWriter').mockImplementation(async (...args) => {
      const writer = await create(...args);
      const save = writer.save.bind(writer);
      vi.spyOn(writer, 'save').mockImplementation(async (record) => {
        const result = await save(record);
        sourceTable = record.sourceTable;
        if (++rows === 2) {
          // Cancel source reading before the progress interval, with both pages still buffered.
          expect(objects.size).toBe(0);
          abort.abort();
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
    await expect(context.replicateSnapshot()).rejects.toThrow('Initial replication interrupted');
    expect(rows).toBe(2);
    expect(observed).toEqual([2]);
    expect(objects.size).toBeGreaterThan(0);
    // Read progress back from storage, rather than relying only on the progress callback.
    await using reader = await context.storage!.createWriter(test_utils.BATCH_OPTIONS);
    const table = await reader.getSourceTableStatus(sourceTable!);
    expect(table).toMatchObject({ snapshotComplete: false, snapshotStatus: { replicatedCount: 2 } });
    if (mode === 'key scan') {
      expect(Number(bson.deserialize(table!.snapshotStatus!.lastKey!).id)).toBe(2);
    }
    expect((await context.storage!.getCheckpoint()).lsn).toBeNull();
  }
);
