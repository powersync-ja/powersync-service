import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import type { SyncRuleDocumentV1 } from '../../src/storage/implementation/v1/models.js';
import { MongoParameterCompactorV1 } from '../../src/storage/implementation/v1/MongoParameterCompactorV1.js';
import { MongoSyncBucketStorageV1 } from '../../src/storage/implementation/v1/MongoSyncBucketStorageV1.js';
import type { VersionedPowerSyncMongoV1 } from '../../src/storage/implementation/v1/VersionedPowerSyncMongoV1.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const PARAMETER_RULES = `
bucket_definitions:
  test:
    parameters: select id from test where id = request.user_id()
    data: []
`;

async function createActiveStorage() {
  const factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
  const syncRules = await factory.updateSyncRules(
    updateSyncRulesFromYaml(PARAMETER_RULES, { storageVersion: storage.STORAGE_VERSION_2 })
  );
  const processingStorage = factory.getInstance(syncRules);
  await using writer = await processingStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone('1/1');
  await writer.commit('1/1');

  const active = await factory.getActiveSyncConfig();
  if (active == null) {
    throw new Error('Expected an active sync config');
  }
  return { factory, storage: active.storage as MongoSyncBucketStorageV1, streamId: syncRules.replicationStreamId };
}

function parameterDocument(
  id: bigint,
  groupId: number,
  key: { t: bson.ObjectId; k: string },
  lookup: bson.Binary,
  bucket_parameters: Record<string, string>[]
): Record<string, unknown> {
  return { _id: id, key: { ...key, g: groupId }, lookup, bucket_parameters };
}

describe('Mongo parameter compaction V1', () => {
  test('compacts incrementally with an _id-only scan and persists the cursor', async () => {
    const { factory, storage: bucketStorage, streamId } = await createActiveStorage();
    await using _factory = factory;

    const db = bucketStorage.db as VersionedPowerSyncMongoV1;
    const collection = db.parameterIndexV1 as any;
    const key = { t: new bson.ObjectId(), k: 'row' };
    const lookup = new bson.Binary(Buffer.from('lookup'));

    await collection.insertMany([
      parameterDocument(90n, streamId + 1, key, lookup, [{ id: 'other-stream' }]),
      parameterDocument(100n, streamId, key, lookup, [{ id: 'old' }]),
      parameterDocument(110n, streamId, key, lookup, [{ id: 'new' }]),
      parameterDocument(120n, streamId, { ...key, k: 'deleted' }, lookup, [{ id: 'delete-me' }]),
      parameterDocument(130n, streamId, { ...key, k: 'deleted' }, lookup, []),
      parameterDocument(200n, streamId, key, lookup, [{ id: 'at-target' }])
    ]);

    await bucketStorage.compact({
      compactBuckets: [],
      compactParameterData: true,
      incrementalOnly: true,
      maxOpId: 200n
    });

    const firstPass = await collection.find({}, { sort: { _id: 1 } }).toArray();
    expect(firstPass.map((document: any) => BigInt(document._id))).toEqual([90n, 110n, 200n]);
    const firstStreamDoc = (await db.sync_rules.findOne({ _id: streamId })) as SyncRuleDocumentV1;
    expect(BigInt(firstStreamDoc.parameter_compaction!.compacted_before)).toBe(200n);

    await collection.insertMany([
      parameterDocument(210n, streamId, key, lookup, [{ id: 'later' }]),
      parameterDocument(220n, streamId, { ...key, k: 'row' }, lookup, [])
    ]);
    const incremental = new MongoParameterCompactorV1(db, streamId, 300n, {});
    await incremental.compact();

    const secondPass = await collection.find({}, { sort: { _id: 1 } }).toArray();
    expect(secondPass.map((document: any) => BigInt(document._id))).toEqual([90n]);
    const secondStreamDoc = (await db.sync_rules.findOne({ _id: streamId })) as SyncRuleDocumentV1;
    expect(BigInt(secondStreamDoc.parameter_compaction!.compacted_before)).toBe(300n);
  });

  test('seeds the compaction cursor when a V1 stream is created', async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
    // Stand in for parameter history written by earlier deployments: all V1 streams share the
    // `main` op id sequence and the `bucket_parameters` collection.
    await factory.db.op_id_sequence.updateOne({ _id: 'main' }, { $set: { op_id: 500n } }, { upsert: true });

    const syncRules = await factory.updateSyncRules(
      updateSyncRulesFromYaml(PARAMETER_RULES, { storageVersion: storage.STORAGE_VERSION_2 })
    );

    // Entries for this stream can only be written above the sequence head, so its first compaction
    // does not have to scan the older entries of other streams.
    const streamDoc = (await factory.db.sync_rules.findOne({
      _id: syncRules.replicationStreamId
    })) as SyncRuleDocumentV1;
    expect(BigInt(streamDoc.parameter_compaction!.compacted_before)).toBe(500n);
  });

  test('clearing a V1 stream clears the parameter compaction cursor', async () => {
    const { factory, storage: bucketStorage, streamId } = await createActiveStorage();
    await using _factory = factory;

    const db = bucketStorage.db as VersionedPowerSyncMongoV1;
    await db.sync_rules.updateOne({ _id: streamId }, {
      $set: { 'parameter_compaction.compacted_before': 123n }
    } as any);
    await bucketStorage.clear();

    const streamDoc = (await db.sync_rules.findOne({ _id: streamId })) as SyncRuleDocumentV1;
    expect(streamDoc.parameter_compaction).toBeUndefined();
  });
});
