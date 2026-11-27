import { mongo } from '@powersync/lib-service-mongodb';
import { OplogEntry, type RustAddon } from '@powersync/service-core';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import { describe, expect, vi } from 'vitest';

import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { env } from './env.js';
import { testTimeout } from './test-timeouts.js';
import { rustModuleTest } from './util.js';

const SYNC_CONFIG = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    lists:
      auto_subscribe: true
      query: SELECT _id AS id, owner, name, created, tags, meta, amount FROM lists WHERE owner = auth.user_id()
    todos:
      auto_subscribe: true
      query: |
        SELECT todos._id AS id, todos.description, todos.n FROM todos
          INNER JOIN lists ON todos.list = lists._id
          WHERE lists.owner = auth.user_id()
`;

const BUCKETS = ['lists|0["u1"]', 'lists|0["u2"]', 'lists|0["u3"]', 'todos|0["l1"]', 'todos|0["l2"]', 'todos|0["l3"]'];

describe.skipIf(!env.TEST_MONGO_STORAGE).each([1, 3, 4])(
  'Rust evaluation with storage v%s',
  { timeout: testTimeout(30_000) },
  (storageVersion) => {
    /**
     * Replicates the same source data through a snapshot and streaming changes, returning everything storage persisted
     * for bucket data and parameter lookups.
     */
    async function replicate(rustAddon: typeof RustAddon | undefined) {
      const generator = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
        url: env.MONGO_TEST_URL,
        isCI: env.CI,
        supportsMultipleSyncConfigs: true,
        rustAddon
      });
      await using context = await openChangeStreamTestContext(generator.factory, { storageVersion });
      const { db } = context;
      await context.updateSyncRules(SYNC_CONFIG);

      const lists = db.collection<{ _id: string } & mongo.Document>('lists');
      const todos = db.collection<{ _id: string } & mongo.Document>('todos');
      await lists.insertMany([
        {
          _id: 'l1',
          owner: 'u1',
          name: 'List 1',
          created: new Date('2024-01-02T03:04:05.678Z'),
          tags: ['a', 'b'],
          meta: { color: 'red', nested: [1, 2.5, null] }
        },
        { _id: 'l2', owner: 'u2', name: 'List 2', amount: mongo.Decimal128.fromString('1.5') }
      ]);
      await todos.insertMany([
        { _id: 't1', list: 'l1', description: 'First', n: 1 },
        { _id: 't2', list: 'l2', description: 'Second', n: 2.5 }
      ]);

      await context.replicateSnapshot();
      context.startStreaming();

      await lists.insertOne({ _id: 'l3', owner: 'u3', name: 'List 3', created: new Date(0) });
      // Moves the list to a different bucket, and changes its parameter lookup.
      await lists.updateOne({ _id: 'l1' }, { $set: { owner: 'u2' } });
      await todos.insertOne({ _id: 't3', list: 'l3', description: 'Third', n: 3n });
      await todos.deleteOne({ _id: 't1' });

      const buckets: Record<string, unknown[]> = {};
      for (const bucket of BUCKETS) {
        buckets[bucket] = (await context.getBucketData(bucket)).map(normalizeOperation);
      }

      return {
        buckets,
        parameters: await parameterIndexes(context.factory as mongo_storage.storage.MongoBucketStorage)
      };
    }

    rustModuleTest('matches JavaScript evaluation', async ({ rust }) => {
      const expected = await replicate(undefined);
      // Make sure the comparison is meaningful.
      expect(Object.values(expected.buckets).filter((data) => data.length > 0)).toHaveLength(BUCKETS.length);

      using evaluate = vi.spyOn(rust.SyncStreamEvaluator.prototype, 'evaluateBsonBlockAsync');
      const actual = await replicate(rust);
      if (storageVersion >= 3) {
        expect(evaluate).toHaveBeenCalled();
      } else {
        // Older storage versions decode rows with the source's converter instead.
        expect(evaluate).not.toHaveBeenCalled();
      }

      expect(actual).toEqual(expected);
    });

    rustModuleTest.skipIf(storageVersion < 3)(
      'reports rows Rust cannot evaluate instead of evaluating them in JavaScript',
      async ({ rust }) => {
        const generator = mongo_storage.test_utils.mongoTestStorageFactoryGenerator({
          url: env.MONGO_TEST_URL,
          isCI: env.CI,
          supportsMultipleSyncConfigs: true,
          rustAddon: rust
        });
        await using context = await openChangeStreamTestContext(generator.factory, { storageVersion });
        const { db } = context;
        await context.updateSyncRules(SYNC_CONFIG);

        const lists = db.collection<{ _id: string } & mongo.Document>('lists');
        await lists.insertOne({ _id: 'l1', owner: 'u1', name: 'List 1' });
        await context.replicateSnapshot();
        context.startStreaming();

        // Dates outside the range of JavaScript dates can't be decoded in Rust.
        await lists.updateOne({ _id: 'l1' }, [
          { $set: { created: { $toDate: mongo.Long.fromString('9000000000000000') } } }
        ]);
        // Replication continues afterwards.
        await lists.insertOne({ _id: 'l2', owner: 'u1', name: 'List 2' });

        const data = await context.getBucketData('lists|0["u1"]');
        expect(data.map(({ op, object_id }) => ({ op, object_id }))).toEqual([
          { op: 'PUT', object_id: 'l1' },
          // The row that couldn't be evaluated is saved without outputs, removing it from its previous bucket.
          { op: 'REMOVE', object_id: 'l1' },
          { op: 'PUT', object_id: 'l2' }
        ]);

        const stats = (context.factory as mongo_storage.storage.MongoBucketStorage).rustEvaluationStats;
        expect(stats).toMatchObject({ evaluatedInRust: 2, evaluatedInJavaScript: 0, failedInRust: 1 });
      }
    );
  }
);

/**
 * Returns persisted parameter lookups of v3+ storage (older versions store them in a collection shared with bucket
 * data, which we compare through bucket data instead).
 */
async function parameterIndexes(factory: mongo_storage.storage.MongoBucketStorage) {
  const storageDb = factory.db.db;
  const collections = (await storageDb.listCollections().toArray())
    .map((collection) => collection.name)
    .filter((name) => name.startsWith('parameter_index_'))
    .sort();

  const parameters: Record<string, unknown> = {};
  for (const name of collections) {
    parameters[name] = await storageDb
      .collection(name)
      .find({}, { projection: { _id: 1, 'key.k': 1, lookup: 1, bucket_parameters: 1 } })
      .sort({ _id: 1 })
      .toArray();
  }
  return parameters;
}

/**
 * Removes parts of operations that are expected to differ between replication runs or evaluators.
 *
 * Subkeys (and thus checksums of REMOVE operations) include the source table id, which is generated for each run. Rust
 * intentionally emits `null` for columns missing in the source document while JavaScript omits them, which also changes
 * checksums of PUT operations.
 */
function normalizeOperation({ subkey: _subkey, checksum: _checksum, data, ...operation }: OplogEntry) {
  const parsed = data == null ? null : JSON.parse(data as string);
  return {
    ...operation,
    data: parsed == null ? null : Object.fromEntries(Object.entries(parsed).filter(([, value]) => value != null))
  };
}
