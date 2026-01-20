import { storage } from '@powersync/service-core';
import { ScopedParameterLookup } from '@powersync/service-sync-rules';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

export function registerParameterCompactTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;

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
    await using writer = await factory.createCombinedWriter([bucketStorage], test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't1'
      },
      afterReplicaId: 't1'
    });

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: {
        id: 't2'
      },
      afterReplicaId: 't2'
    });

    await writer.commitAll('1/1');

    const lookup = ScopedParameterLookup.direct({ lookupName: '20002', queryId: '', source: null as any }, ['t1']);

    const checkpoint1 = await bucketStorage.getCheckpoint();
    const parameters1 = await checkpoint1.getParameterSets([lookup]);
    expect(parameters1).toEqual([{ id: 't1' }]);

    await writer.save({
      sourceTable: testTable,
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

    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.DELETE,
      before: {
        id: 't1'
      },
      beforeReplicaId: 't1'
    });
    await writer.commitAll('1/2');
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
      await using writer = await factory.createCombinedWriter([bucketStorage], test_utils.BATCH_OPTIONS);
      const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);

      await writer.markAllSnapshotDone('1/1');
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't1',
          uid: 'u1'
        },
        afterReplicaId: 't1'
      });
      // Interleave with another operation, to evict the other cache entry when compacting.
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 't2',
          uid: 'u1'
        },
        afterReplicaId: 't2'
      });

      await writer.commitAll('1/1');

      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.DELETE,
        before: {
          id: 't1',
          uid: 'u1'
        },
        beforeReplicaId: 't1'
      });
      await writer.commitAll('2/1');

      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: {
          id: 't2',
          uid: 'u2'
        },
        afterReplicaId: 't2'
      });
      await writer.commitAll('3/1');

      const lookup = ScopedParameterLookup.direct({ lookupName: 'test', queryId: '1', source: null as any }, ['u1']);

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
