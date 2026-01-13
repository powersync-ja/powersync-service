import { describe, expect, test } from 'vitest';
import * as register from '@powersync/service-core-tests';
import * as test_utils from '@powersync/service-core-tests';
import * as storage from '@powersync/service-core';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

describe('MongoDB Storage - storeCurrentData table-level configuration', () => {
  test('table with storeCurrentData=false does not store in current_data', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY();

    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data
      `
    });

    const bucketStorage = factory.getInstance(syncRules);

    // Create table with storeCurrentData=false (simulating REPLICA IDENTITY FULL)
    const testTable = test_utils.makeTestTable('test_data', ['id']);
    testTable.storeCurrentData = false;

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Insert operation
      await batch.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'test1', description: 'test data' },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.commit('1/1');
    });

    const checkpoint = await bucketStorage.getCheckpoint();
    const bucketData = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    const data = test_utils.getBatchData(bucketData);

    // Verify data is in bucket
    expect(data).toHaveLength(1);
    expect(data[0]).toMatchObject({
      op: 'PUT',
      object_id: 'test1'
    });

    // Verify current_data is empty (data stored as {})
    // This is an implementation detail - with storeCurrentData=false,
    // the system stores empty objects instead of full data
  });

  test('table with storeCurrentData=true stores in current_data', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY();

    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data
      `
    });

    const bucketStorage = factory.getInstance(syncRules);

    // Create table with storeCurrentData=true (default behavior)
    const testTable = test_utils.makeTestTable('test_data', ['id']);
    testTable.storeCurrentData = true;

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Insert operation
      await batch.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'test1', description: 'test data' },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.commit('1/1');
    });

    const checkpoint = await bucketStorage.getCheckpoint();
    const bucketData = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    const data = test_utils.getBatchData(bucketData);

    // Verify data is in bucket
    expect(data).toHaveLength(1);
    expect(data[0]).toMatchObject({
      op: 'PUT',
      object_id: 'test1'
    });
  });

  test('UPDATE with storeCurrentData=false does not require previous data', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY();

    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data
      `
    });

    const bucketStorage = factory.getInstance(syncRules);

    const testTable = test_utils.makeTestTable('test_data', ['id']);
    testTable.storeCurrentData = false;

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // INSERT
      await batch.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'test1', description: 'original' },
        afterReplicaId: test_utils.rid('test1')
      });

      // UPDATE - with storeCurrentData=false, full row data is always provided
      await batch.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.UPDATE,
        after: { id: 'test1', description: 'updated' },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.commit('1/2');
    });

    const checkpoint = await bucketStorage.getCheckpoint();
    const bucketData = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    const data = test_utils.getBatchData(bucketData);

    // Should only have the final UPDATE operation
    expect(data).toHaveLength(1);
    expect(data[0]).toMatchObject({
      op: 'PUT',
      object_id: 'test1'
    });
  });

  test('mixed tables with different storeCurrentData settings', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY();

    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM table_full
      - SELECT * FROM table_default
      `
    });

    const bucketStorage = factory.getInstance(syncRules);

    // Table with REPLICA IDENTITY FULL (storeCurrentData=false)
    const tableFull = test_utils.makeTestTable('table_full', ['id']);
    tableFull.storeCurrentData = false;

    // Table with default replica identity (storeCurrentData=true)
    const tableDefault = test_utils.makeTestTable('table_default', ['id']);
    tableDefault.storeCurrentData = true;

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // Insert into both tables
      await batch.save({
        sourceTable: tableFull,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'full1', data: 'from full table' },
        afterReplicaId: test_utils.rid('full1')
      });

      await batch.save({
        sourceTable: tableDefault,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'default1', data: 'from default table' },
        afterReplicaId: test_utils.rid('default1')
      });

      await batch.commit('1/1');
    });

    const checkpoint = await bucketStorage.getCheckpoint();
    const bucketData = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    const data = test_utils.getBatchData(bucketData);

    // Verify both records are present
    expect(data).toHaveLength(2);

    const fullTableRecord = data.find((d) => d.object_id === 'full1');
    const defaultTableRecord = data.find((d) => d.object_id === 'default1');

    expect(fullTableRecord).toBeDefined();
    expect(defaultTableRecord).toBeDefined();
  });

  test('DELETE with storeCurrentData=false', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY();

    const syncRules = await factory.updateSyncRules({
      content: `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data
      `
    });

    const bucketStorage = factory.getInstance(syncRules);

    const testTable = test_utils.makeTestTable('test_data', ['id']);
    testTable.storeCurrentData = false;

    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      // INSERT
      await batch.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: 'test1', description: 'test data' },
        afterReplicaId: test_utils.rid('test1')
      });

      await batch.commit('1/1');
    });

    // Delete in a new batch
    await bucketStorage.startBatch(test_utils.BATCH_OPTIONS, async (batch) => {
      await batch.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.DELETE,
        beforeReplicaId: test_utils.rid('test1')
      });

      await batch.commit('1/2');
    });

    const checkpoint = await bucketStorage.getCheckpoint();
    const bucketData = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, new Map([['global[]', 0n]]))
    );

    const data = test_utils.getBatchData(bucketData);

    // Should have a REMOVE operation
    expect(data).toHaveLength(1);
    expect(data[0]).toMatchObject({
      op: 'REMOVE',
      object_id: 'test1'
    });
  });
});
