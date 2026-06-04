import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoBucketStorage } from '../../src/storage/MongoBucketStorage.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

/**
 * Tests for the per-table `storeCurrentData` flag in MongoDB storage (both v1 and v3).
 *
 * Two things are exercised:
 *  1. `resolveTables` derives `storeCurrentData = !sendsCompleteRows` in memory on every call
 *     (it is not persisted; an unreported source defaults to keeping a copy).
 *  2. A batch honours the flag: when `storeCurrentData` is false, the row payload is NOT kept in
 *     the current_data collection, while the bucket data is still produced as usual.
 *
 * The mapping from a source's replica identity to `sendsCompleteRows` is a Postgres concern and is
 * covered in module-postgres; here we only exercise the storage layer's boolean handling.
 */

const SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`;

function descriptor(name: string, sendsCompleteRows?: boolean): storage.SourceEntityDescriptor {
  return {
    connectionTag: storage.SourceTable.DEFAULT_TAG,
    objectId: name,
    schema: 'public',
    name,
    replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }],
    sendsCompleteRows
  };
}

function singleUseIdGenerator(hex: string) {
  let used = false;
  return () => {
    if (used) {
      throw new Error(`Can only generate a single id using ${hex}`);
    }
    used = true;
    return new bson.ObjectId(hex);
  };
}

/**
 * Returns the row payload stored in current_data for the (single) record of `table`, or null if no
 * payload is stored. v1 stores an empty BSON object ({}) when storeCurrentData is false; v3 stores
 * null. Both are normalised to null here so the assertions are version-agnostic.
 */
async function storedRowPayload(
  storageVersion: number,
  factory: storage.BucketStorageFactory,
  bucketStorage: storage.SyncRulesBucketStorage,
  syncRulesId: number,
  table: storage.SourceTable
): Promise<Record<string, any> | null> {
  let data: bson.Binary | null | undefined;
  if (storageVersion < 3) {
    const db = (factory as MongoBucketStorage).db;
    const doc = await db.current_data.findOne({ '_id.g': syncRulesId });
    data = doc?.data;
  } else {
    const db = (bucketStorage as any).db;
    const doc = await db.sourceRecordsV3(syncRulesId, table.id).findOne({});
    data = doc?.data;
  }
  if (data == null) {
    return null;
  }
  const decoded = bson.deserialize(data.buffer);
  return Object.keys(decoded).length === 0 ? null : decoded;
}

function registerStoreCurrentDataTests(storageVersion: number) {
  test('resolveTables derives storeCurrentData fresh each call, with no persisted memory', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);
    const syncRulesContent = (await factory.getReplicationStreamConfigs(syncRules.id))[0];

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

    // Sources that always send complete rows (e.g. Postgres REPLICA IDENTITY FULL) don't need current_data.
    const complete = await writer.resolveTables({
      connection_id: 1,
      source: descriptor('test_complete', true),
      idGenerator: singleUseIdGenerator('6544e3899293153fa7b38301')
    });
    expect(complete.tables[0].storeCurrentData).toBe(false);

    // Sources that may send partial/key data still need current_data.
    const partial = await writer.resolveTables({
      connection_id: 1,
      source: descriptor('test_partial', false),
      idGenerator: singleUseIdGenerator('6544e3899293153fa7b38302')
    });
    expect(partial.tables[0].storeCurrentData).toBe(true);

    // Re-resolving an already-resolved table with no completeness info (sendsCompleteRows undefined)
    // does not remember the earlier value: nothing is persisted, so it falls back to the default
    // (keep a copy).
    const reResolved = await writer.resolveTables({
      connection_id: 1,
      source: descriptor('test_complete')
    });
    expect(reResolved.tables[0].storeCurrentData).toBe(true);
  });

  test('storeCurrentData=false omits the row payload from current_data, data still syncs', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);
    const syncRulesContent = (await factory.getReplicationStreamConfigs(syncRules.id))[0];

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const table = await test_utils.resolveTestTable(writer, 'test_data', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);
    table.storeCurrentData = false;

    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'test1', description: 'test data' },
      afterReplicaId: test_utils.rid('test1')
    });
    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRulesContent, 'global[]', 0n)])
    );
    expect(test_utils.getBatchData(batch)).toMatchObject([{ op: 'PUT', object_id: 'test1' }]);

    // No row payload retained in current_data.
    expect(await storedRowPayload(storageVersion, factory, bucketStorage, syncRules.id, table)).toBeNull();
  });

  test('storeCurrentData=true retains the row payload in current_data', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);
    const syncRulesContent = (await factory.getReplicationStreamConfigs(syncRules.id))[0];

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const table = await test_utils.resolveTestTable(writer, 'test_data', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);
    table.storeCurrentData = true;

    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'test1', description: 'test data' },
      afterReplicaId: test_utils.rid('test1')
    });
    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRulesContent, 'global[]', 0n)])
    );
    expect(test_utils.getBatchData(batch)).toMatchObject([{ op: 'PUT', object_id: 'test1' }]);

    expect(await storedRowPayload(storageVersion, factory, bucketStorage, syncRules.id, table)).toMatchObject({
      id: 'test1'
    });
  });

  test('storeCurrentData=false processes UPDATE without a stored copy', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);
    const syncRulesContent = (await factory.getReplicationStreamConfigs(syncRules.id))[0];

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const table = await test_utils.resolveTestTable(writer, 'test_data', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);
    table.storeCurrentData = false;

    // With REPLICA IDENTITY FULL the UPDATE carries the full row, so it is applied from `after`
    // rather than a stored copy of the previous row.
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'test1', description: 'original' },
      afterReplicaId: test_utils.rid('test1')
    });
    await writer.save({
      sourceTable: table,
      tag: storage.SaveOperationTag.UPDATE,
      after: { id: 'test1', description: 'updated' },
      afterReplicaId: test_utils.rid('test1')
    });
    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRulesContent, 'global[]', 0n)])
    );
    const data = test_utils.getBatchData(batch);
    expect(data.length).toBeGreaterThan(0);
    expect(data.at(-1)).toMatchObject({ op: 'PUT', object_id: 'test1' });

    expect(await storedRowPayload(storageVersion, factory, bucketStorage, syncRules.id, table)).toBeNull();
  });
}

describe('MongoDB storage - storeCurrentData', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`storage v${storageVersion}`, () => {
      registerStoreCurrentDataTests(storageVersion);
    });
  }
});
