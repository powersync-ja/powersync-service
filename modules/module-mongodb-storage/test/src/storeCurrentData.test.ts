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
 *  1. `resolveTables` computes and persists `storeCurrentData = replicationIdentity !== 'full'`
 *     (and leaves the persisted value untouched when no replica identity is reported).
 *  2. A batch honours the flag: when `storeCurrentData` is false, the row payload is NOT kept in
 *     the current_data collection, while the bucket data is still produced as usual.
 */

const SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "%"
`;

type ReplicationIdentity = storage.SourceEntityDescriptor['replicationIdentity'];

function descriptor(name: string, replicationIdentity?: ReplicationIdentity): storage.SourceEntityDescriptor {
  return {
    connectionTag: storage.SourceTable.DEFAULT_TAG,
    objectId: name,
    schema: 'public',
    name,
    replicaIdColumns: [{ name: 'id', type: 'VARCHAR', typeId: 25 }],
    replicationIdentity
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
  test('resolveTables computes storeCurrentData from replicationIdentity', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

    // REPLICA IDENTITY FULL always sends complete rows, so current_data is not needed.
    const full = await writer.resolveTables({
      connection_id: 1,
      source: descriptor('test_full', 'full'),
      idGenerator: singleUseIdGenerator('6544e3899293153fa7b38301')
    });
    expect(full.tables[0].storeCurrentData).toBe(false);

    // Every other replica identity only sends partial/key data, so current_data is still needed.
    for (const [name, identity, hex] of [
      ['test_default', 'default', '6544e3899293153fa7b38302'],
      ['test_index', 'index', '6544e3899293153fa7b38303'],
      ['test_nothing', 'nothing', '6544e3899293153fa7b38304']
    ] as const) {
      const resolved = await writer.resolveTables({
        connection_id: 1,
        source: descriptor(name, identity),
        idGenerator: singleUseIdGenerator(hex)
      });
      expect(resolved.tables[0].storeCurrentData).toBe(true);
    }
  });

  test('resolveTables without a replica identity keeps the persisted storeCurrentData', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);

    // First resolution (snapshot path) reports FULL and persists storeCurrentData=false.
    const initial = await writer.resolveTables({
      connection_id: 1,
      source: descriptor('test_full', 'full'),
      idGenerator: singleUseIdGenerator('6544e3899293153fa7b38301')
    });
    expect(initial.tables[0].storeCurrentData).toBe(false);

    // A later resolution from the streaming relation path reports no replica identity. The persisted
    // value must be preserved (the flag must not be clobbered back to true).
    const reResolved = await writer.resolveTables({
      connection_id: 1,
      source: descriptor('test_full'),
      idGenerator: singleUseIdGenerator('6544e3899293153fa7b38305')
    });
    expect(reResolved.tables[0].storeCurrentData).toBe(false);
  });

  test('storeCurrentData=false omits the row payload from current_data, data still syncs', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);

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
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules, 'global[]', 0n)])
    );
    expect(test_utils.getBatchData(batch)).toMatchObject([{ op: 'PUT', object_id: 'test1' }]);

    // No row payload retained in current_data.
    expect(await storedRowPayload(storageVersion, factory, bucketStorage, syncRules.id, table)).toBeNull();
  });

  test('storeCurrentData=true retains the row payload in current_data', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES, { storageVersion }));
    const bucketStorage = factory.getInstance(syncRules);

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
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules, 'global[]', 0n)])
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
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules, 'global[]', 0n)])
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
