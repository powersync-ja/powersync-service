import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { getTestStorage, test_utils } from '@powersync/service-core-tests';
import {
  ParameterIndexLookupCreator,
  ScopedParameterLookup,
  UnscopedParameterLookup
} from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { BUCKET_DATA_OP_ID_INDEX } from '../../src/storage/PostgresSyncRulesStorage.js';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const SYNC_CONFIG = `
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

      const withoutIndex = await bucketStorage.getCheckpointChanges({ lastCheckpoint: cp1, nextCheckpoint: cp2 });
      expect(withoutIndex.invalidateDataBuckets).toBe(true);

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
        await db.sql`DROP INDEX IF EXISTS bucket_data_op_id_index`.execute();
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
  });
}
