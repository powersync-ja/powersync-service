import { storage } from '@powersync/service-core';
import { ScopedParameterLookup } from '@powersync/service-sync-rules';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

export function registerParameterCompactTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;

  const TEST_TABLE = test_utils.makeTestTable('test', ['id'], config);

  test('compacting parameters', async () => {
    await using factory = await generateStorageFactory();
    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  test:
    parameters: select id from test where id = request.user_id()
    data: []
    `
    });
    const bucketStorage = factory.getInstance(syncRules);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.markAllSnapshotDone('1/1');
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2'
        },
        afterReplicaId: 't2'
      });

      await batch.commit('1/1');
    });

    const lookup = ScopedParameterLookup.direct({ lookupName: 'test', queryId: '1' }, ['t1']);

    const checkpoint1 = await bucketStorage.getCheckpoint();
    const parameters1 = await checkpoint1.getParameterSets([lookup]);
    expect(parameters1).toEqual([{ id: 't1' }]);

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.UPDATE,
        before: {
          id: 't1'
        },
        beforeReplicaId: 't1',
        after: {
          id: 't1'
        },
        afterReplicaId: 't1'
      });

      await batch.save({
        sourceTable: TEST_TABLE,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't1'
        },
        beforeReplicaId: 't1'
      });
      await batch.commit('1/2');
    });
    const checkpoint2 = await bucketStorage.getCheckpoint();
    const parameters2 = await checkpoint2.getParameterSets([lookup]);
    expect(parameters2).toEqual([]);

    const statsBefore = await bucketStorage.factory.getStorageMetrics();
    await bucketStorage.compact({ compactParameterData: true });

    // Check consistency
    const parameters1b = await checkpoint1.getParameterSets([lookup]);
    const parameters2b = await checkpoint2.getParameterSets([lookup]);
    expect(parameters1b).toEqual([{ id: 't1' }]);
    expect(parameters2b).toEqual([]);

    // Check storage size
    const statsAfter = await bucketStorage.factory.getStorageMetrics();
    expect(statsAfter.parameters_size_bytes).toBeLessThan(statsBefore.parameters_size_bytes);
  });

  for (let cacheLimit of [1, 10]) {
    test(`compacting deleted parameters with cache size ${cacheLimit}`, async () => {
      await using factory = await generateStorageFactory();
      const syncRules = await factory.updateSyncRules({
        content: `
bucket_definitions:
  test:
    parameters: select id from test where uid = request.user_id()
    data: []
    `
      });
      const bucketStorage = factory.getInstance(syncRules);

      await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
        await batch.markAllSnapshotDone('1/1');
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 't1',
            uid: 'u1'
          },
          afterReplicaId: 't1'
        });
        // Interleave with another operation, to evict the other cache entry when compacting.
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: 't2',
            uid: 'u1'
          },
          afterReplicaId: 't2'
        });

        await batch.commit('1/1');
      });

      await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.DELETE,
          before: {
            id: 't1',
            uid: 'u1'
          },
          beforeReplicaId: 't1'
        });
        await batch.commit('2/1');
      });

      await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
        await batch.save({
          sourceTable: TEST_TABLE,
          tag: storage.SaveOperationTag.UPDATE,
          after: {
            id: 't2',
            uid: 'u2'
          },
          afterReplicaId: 't2'
        });
        await batch.commit('3/1');
      });

      const lookup = ScopedParameterLookup.direct({ lookupName: 'test', queryId: '1' }, ['u1']);

      const checkpoint1 = await bucketStorage.getCheckpoint();
      const parameters1 = await checkpoint1.getParameterSets([lookup]);
      expect(parameters1).toEqual([]);

      const statsBefore = await bucketStorage.factory.getStorageMetrics();
      await bucketStorage.compact({ compactParameterData: true, compactParameterCacheLimit: cacheLimit });

      // Check consistency
      const parameters1b = await checkpoint1.getParameterSets([lookup]);
      expect(parameters1b).toEqual([]);

      // Check storage size
      const statsAfter = await bucketStorage.factory.getStorageMetrics();
      expect(statsAfter.parameters_size_bytes).toBeLessThan(statsBefore.parameters_size_bytes);
    });
  }
}
