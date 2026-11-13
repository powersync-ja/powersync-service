import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import {
  ParameterIndexLookupCreator,
  ScopedParameterLookup,
  UnscopedParameterLookup
} from '@powersync/service-sync-rules';
import { describe, expect, test, vi } from 'vitest';
import { PostgresWriteCheckpointAPI } from '../../src/storage/checkpoints/PostgresWriteCheckpointAPI.js';
import { BUCKET_DATA_OP_ID_INDEX } from '../../src/storage/PostgresSyncRulesStorage.js';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const SYNC_CONFIG = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    a:
      auto_subscribe: true
      query: SELECT * FROM a WHERE p IN (SELECT id FROM param_a WHERE u = auth.user_id())
`;

for (const storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Postgres checkpoint changes - v${storageVersion}`, () => {
    const config = { ...POSTGRES_STORAGE_FACTORY, storageVersion };

    async function setup() {
      const factory = await POSTGRES_STORAGE_FACTORY.factory();
      const { stream: replicationStream, content: syncRules } = await test_utils.deploySyncRules(
        factory,
        updateSyncRulesFromYaml(SYNC_CONFIG, { storageVersion })
      );
      const bucketStorage = await getTestStorage(factory, replicationStream);
      const parsed = syncRules.parsed(test_utils.PARSE_OPTIONS);
      const syncConfig = parsed.syncConfigs[0].config;

      const source: ParameterIndexLookupCreator = syncConfig.bucketParameterLookupSources.find((s) =>
        [...s.getSourceTables()].some((t) => t.name == 'param_a')
      )!;
      const lookupFor = (user: string) =>
        ScopedParameterLookup.normalized(
          parsed.hydrationState.getParameterIndexLookupScope(source),
          UnscopedParameterLookup.normalized([user])
        );

      return { factory, bucketStorage, lookupFor };
    }

    test('only lookups written between two checkpoints are reported', async () => {
      const { factory, bucketStorage, lookupFor } = await setup();
      await using _factory = factory;
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const paramTable = await test_utils.resolveTestTable(writer, 'param_a', ['id'], config, 1);
      const dataTable = await test_utils.resolveTestTable(writer, 'a', ['id'], config, 2);
      await writer.markAllSnapshotDone('1/1');

      await writer.save({
        sourceTable: paramTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g1', u: 'user1' },
        afterReplicaId: test_utils.rid('g1')
      });
      await writer.commit('1/1');
      const cp1 = await bucketStorage.getCheckpoint();

      // Data only: no parameter lookup changes.
      await writer.save({
        sourceTable: dataTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'd1', p: 'g1' },
        afterReplicaId: test_utils.rid('d1')
      });
      await writer.commit('1/2');
      const cp2 = await bucketStorage.getCheckpoint();
      expect(cp2.checkpoint).toBeGreaterThan(cp1.checkpoint);

      const dataOnly = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp1, nextCheckpoint: cp2 });
      expect(dataOnly.invalidateParameterBuckets).toBe(false);
      expect([...dataOnly.updatedParameterLookups]).toEqual([]);

      // Moving the parameter row from user1 to user2 changes both lookups.
      await writer.save({
        sourceTable: paramTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'g1', u: 'user2' },
        afterReplicaId: test_utils.rid('g1')
      });
      await writer.commit('1/3');
      const cp3 = await bucketStorage.getCheckpoint();

      const moved = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp2, nextCheckpoint: cp3 });
      expect(moved.invalidateParameterBuckets).toBe(false);
      // Must match what BucketChecksumState records for the lookups of a connection.
      expect([...moved.updatedParameterLookups].sort()).toEqual(
        [lookupFor('user1').serializedRepresentation, lookupFor('user2').serializedRepresentation].sort()
      );

      // A delete removes the remaining lookup.
      await writer.save({
        sourceTable: paramTable,
        tag: storage.SaveOperationTag.DELETE,
        before: { id: 'g1' },
        beforeReplicaId: test_utils.rid('g1')
      });
      await writer.commit('1/4');
      const cp4 = await bucketStorage.getCheckpoint();

      const deleted = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp3, nextCheckpoint: cp4 });
      expect([...deleted.updatedParameterLookups]).toEqual([lookupFor('user2').serializedRepresentation]);

      // A range over several checkpoints includes everything in between.
      const all = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp1, nextCheckpoint: cp4 });
      expect(all.updatedParameterLookups.size).toBe(2);

      // Going backwards cannot be described as a diff.
      const backwards = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp4, nextCheckpoint: cp1 });
      expect(backwards.invalidateParameterBuckets).toBe(true);
    });

    test('too many changed lookups invalidate all parameter buckets', async () => {
      const { factory, bucketStorage } = await setup();
      await using _factory = factory;
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const paramTable = await test_utils.resolveTestTable(writer, 'param_a', ['id'], config, 1);
      await writer.markAllSnapshotDone('1/1');
      // A row before cp1, so that the range is not mistaken for a cleared storage.
      await writer.save({
        sourceTable: paramTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g', u: 'user' },
        afterReplicaId: test_utils.rid('g')
      });
      await writer.commit('1/1');
      const cp1 = await bucketStorage.getCheckpoint();

      for (let i = 0; i < 1001; i++) {
        await writer.save({
          sourceTable: paramTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: `g${i}`, u: `user${i}` },
          afterReplicaId: test_utils.rid(`g${i}`)
        });
      }
      await writer.commit('1/2');
      const cp2 = await bucketStorage.getCheckpoint();

      const changes = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp1, nextCheckpoint: cp2 });
      expect(changes.invalidateParameterBuckets).toBe(true);
      expect(changes.updatedParameterLookups.size).toBe(0);
    });

    test('data bucket changes need the optional op_id index', async () => {
      const { factory, bucketStorage } = await setup();
      await using _factory = factory;
      const db = (bucketStorage as any).db;
      await db.sql`DROP INDEX IF EXISTS bucket_data_op_id_index`.execute();

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const paramTable = await test_utils.resolveTestTable(writer, 'param_a', ['id'], config, 1);
      const dataTable = await test_utils.resolveTestTable(writer, 'a', ['id'], config, 2);
      await writer.markAllSnapshotDone('1/1');
      for (const [id, u] of [
        ['g1', 'user1'],
        ['g2', 'user2']
      ]) {
        await writer.save({
          sourceTable: paramTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id, u },
          afterReplicaId: test_utils.rid(id)
        });
      }
      await writer.commit('1/1');
      const cp1 = await bucketStorage.getCheckpoint();

      await writer.save({
        sourceTable: dataTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'd1', p: 'g1' },
        afterReplicaId: test_utils.rid('d1')
      });
      await writer.commit('1/2');
      const cp2 = await bucketStorage.getCheckpoint();

      const warn = vi.spyOn(bucketStorage.logger, 'warn');
      const withoutIndex = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp1, nextCheckpoint: cp2 });
      expect(withoutIndex.invalidateDataBuckets).toBe(true);
      expect(warn).toHaveBeenCalledTimes(1);
      expect(warn.mock.calls[0][0]).toContain('CREATE INDEX CONCURRENTLY bucket_data_op_id_index');

      try {
        expect(BUCKET_DATA_OP_ID_INDEX).toBe('bucket_data_op_id_index');
        await db.sql`CREATE INDEX bucket_data_op_id_index ON bucket_data (group_id, op_id)`.execute();
        // New storage instance: the index check is cached.
        const fresh = await getTestStorage(factory, (bucketStorage as any).replicationStream);
        const changes = await fresh.getCheckpointChanges({ lastCheckpoint: cp1, nextCheckpoint: cp2 });
        expect(changes.invalidateDataBuckets).toBe(false);
        expect(changes.updatedDataBuckets.size).toBe(1);
        const [bucket] = [...changes.updatedDataBuckets];
        expect(bucket).toContain('"g1"');

        const none = await fresh.getCheckpointChanges({ lastCheckpoint: cp2, nextCheckpoint: cp2 });
        expect(none.invalidateDataBuckets).toBe(false);
        expect(none.updatedDataBuckets.size).toBe(0);
      } finally {
        // The migration creates the index for small storage; leave it in place for the other tests.
        await db.sql`CREATE INDEX IF NOT EXISTS bucket_data_op_id_index ON bucket_data (group_id, op_id)`.execute();
      }
    });

    test('watchCheckpointChanges reports parameter changes after the first checkpoint', async () => {
      const { factory, bucketStorage, lookupFor } = await setup();
      await using _factory = factory;
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const paramTable = await test_utils.resolveTestTable(writer, 'param_a', ['id'], config, 1);
      const dataTable = await test_utils.resolveTestTable(writer, 'a', ['id'], config, 2);
      await writer.markAllSnapshotDone('1/1');
      await writer.save({
        sourceTable: paramTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g1', u: 'user1' },
        afterReplicaId: test_utils.rid('g1')
      });
      await writer.commit('1/1');

      const abort = new AbortController();
      const iter = bucketStorage
        .watchCheckpointChanges({ user_id: 'user1', signal: abort.signal })
        [Symbol.asyncIterator]();

      const first = await iter.next();
      expect(first.value!.update).toEqual(storage.CHECKPOINT_INVALIDATE_ALL);

      await writer.save({
        sourceTable: dataTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'd1', p: 'g1' },
        afterReplicaId: test_utils.rid('d1')
      });
      await writer.commit('1/2');
      const second = await iter.next();
      expect(second.value!.update.invalidateParameterBuckets).toBe(false);
      expect(second.value!.update.updatedParameterLookups.size).toBe(0);

      await writer.save({
        sourceTable: paramTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g2', u: 'user1' },
        afterReplicaId: test_utils.rid('g2')
      });
      await writer.commit('1/3');
      const third = await iter.next();
      expect(third.value!.update.invalidateParameterBuckets).toBe(false);
      expect([...third.value!.update.updatedParameterLookups]).toEqual([lookupFor('user1').serializedRepresentation]);

      abort.abort();
      await iter.return?.();
    });

    test('clearing the storage invalidates everything', async () => {
      const { factory, bucketStorage } = await setup();
      await using _factory = factory;
      {
        await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
        const paramTable = await test_utils.resolveTestTable(writer, 'param_a', ['id'], config, 1);
        await writer.markAllSnapshotDone('1/1');
        await writer.save({
          sourceTable: paramTable,
          tag: storage.SaveOperationTag.INSERT,
          after: { id: 'g1', u: 'user1' },
          afterReplicaId: test_utils.rid('g1')
        });
        await writer.commit('1/1');
      }
      const cp1 = await bucketStorage.getCheckpoint();

      // Re-snapshot after clear(): user1's lookup disappears, op ids continue above cp1.
      await bucketStorage.clear();
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const paramTable = await test_utils.resolveTestTable(writer, 'param_a', ['id'], config, 1);
      await writer.markAllSnapshotDone('2/1');
      await writer.save({
        sourceTable: paramTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'g2', u: 'user2' },
        afterReplicaId: test_utils.rid('g2')
      });
      await writer.commit('2/1');
      const cp2 = await bucketStorage.getCheckpoint();
      expect(cp2.checkpoint).toBeGreaterThan(cp1.checkpoint);

      const changes = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp1, nextCheckpoint: cp2 });
      expect(changes.invalidateParameterBuckets).toBe(true);
      expect(changes.invalidateDataBuckets).toBe(true);
    });

    test('write checkpoint lookups of concurrent connections are combined', async () => {
      const { factory, bucketStorage } = await setup();
      await using _factory = factory;
      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      await writer.markAllSnapshotDone('1/1');

      const created = await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '5/0' } },
        { user_id: 'user2', heads: { '1': '5/0' } }
      ]);

      const api: PostgresWriteCheckpointAPI = (bucketStorage as any).writeCheckpointAPI;
      const batched = vi.spyOn(api, 'lastWriteCheckpoints');
      const single = vi.spyOn(api, 'lastWriteCheckpoint');

      const abort = new AbortController();
      const watch = (user_id: string) =>
        bucketStorage.watchCheckpointChanges({ user_id, signal: abort.signal })[Symbol.asyncIterator]();
      const iters = [watch('user1'), watch('user2'), watch('user3')];

      await writer.keepalive('5/0');
      const results = await Promise.all(iters.map((iter) => iter.next()));

      expect(results.map((r) => r.value!.writeCheckpoint)).toEqual([
        created.writeCheckpoints.get('user1'),
        created.writeCheckpoints.get('user2'),
        null
      ]);
      expect(single).not.toHaveBeenCalled();
      expect(batched).toHaveBeenCalledTimes(1);
      expect([...batched.mock.calls[0][0].user_ids].sort()).toEqual(['user1', 'user2', 'user3']);

      abort.abort();
      await Promise.all(iters.map((iter) => iter.return?.()));
    });

    test('combined write checkpoint lookups match single lookups with custom write checkpoints', async () => {
      const { factory, bucketStorage } = await setup();
      await using _factory = factory;
      const api: PostgresWriteCheckpointAPI = (bucketStorage as any).writeCheckpointAPI;
      const sync_rules_id: number = (bucketStorage as any).replicationStreamId;
      bucketStorage.setWriteCheckpointMode({ mode: storage.WriteCheckpointMode.CUSTOM } as any);

      await api.batchCreateCustomWriteCheckpoints(
        [
          { user_id: 'user1', checkpoint: 7n, sync_rules_id },
          { user_id: 'user2', checkpoint: 9n, sync_rules_id }
        ],
        0n
      );

      const batched = await api.lastWriteCheckpoints({
        user_ids: ['user1', 'user2', 'user3'],
        sync_rules_id,
        heads: {}
      });
      expect(batched.get('user1')).toBe(7n);
      expect(batched.get('user2')).toBe(9n);
      expect(batched.has('user3')).toBe(false);
      for (const user_id of ['user1', 'user2', 'user3']) {
        const single = await api.lastWriteCheckpoint({ user_id, sync_rules_id, heads: {} });
        expect(batched.get(user_id) ?? null).toEqual(single);
      }
    });

    test('write checkpoint lookups for different LSNs are not combined, and failures do not stick', async () => {
      const { factory, bucketStorage } = await setup();
      await using _factory = factory;
      const internals = bucketStorage as any;
      const api: PostgresWriteCheckpointAPI = internals.writeCheckpointAPI;
      const created = await bucketStorage.createManagedWriteCheckpoints([
        { user_id: 'user1', heads: { '1': '5/0' } },
        { user_id: 'user2', heads: { '1': '7/0' } }
      ]);
      const spy = vi.spyOn(api, 'lastWriteCheckpoints');

      // Two LSNs in flight at the same time: one query each. user2 only qualifies at 7/0.
      const [a1, a2, b1, b2] = await Promise.all([
        internals.batchedLastWriteCheckpoint('user1', '5/0'),
        internals.batchedLastWriteCheckpoint('user2', '5/0'),
        internals.batchedLastWriteCheckpoint('user1', '7/0'),
        internals.batchedLastWriteCheckpoint('user2', '7/0')
      ]);
      expect(spy).toHaveBeenCalledTimes(2);
      expect(a1).toBe(created.writeCheckpoints.get('user1'));
      expect(a2).toBeNull();
      expect(b1).toBe(created.writeCheckpoints.get('user1'));
      expect(b2).toBe(created.writeCheckpoints.get('user2'));

      // A failing query rejects every lookup of that batch, and the next lookup starts a new batch.
      spy.mockRejectedValueOnce(new Error('query failed'));
      const failed = await Promise.allSettled([
        internals.batchedLastWriteCheckpoint('user1', '5/0'),
        internals.batchedLastWriteCheckpoint('user2', '5/0')
      ]);
      expect(failed.map((r) => r.status)).toEqual(['rejected', 'rejected']);
      expect(internals.writeCheckpointLookups.size).toBe(0);
      expect(await internals.batchedLastWriteCheckpoint('user1', '5/0')).toBe(created.writeCheckpoints.get('user1'));
    });
  });
}
