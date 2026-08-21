import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoParameterCompactorV3 } from '../../src/storage/implementation/v3/MongoParameterCompactorV3.js';
import { MongoSyncBucketStorageV3 } from '../../src/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import type { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import type { ReplicationStreamDocumentV3 } from '../../src/storage/implementation/v3/models.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

const PARAMETER_RULES = `
bucket_definitions:
  test:
    parameters: select id from test where id = request.user_id()
    data: []
`;

/** Two parameter indexes over the same lookup values, so both store identical (key, lookup) pairs. */
const TWO_INDEX_PARAMETER_RULES = `
bucket_definitions:
  test1:
    parameters: select id from test where id = request.user_id()
    data: []
  test2:
    parameters: select id from test where id = request.user_id()
    data: []
`;

async function createActiveStorage(rules = PARAMETER_RULES) {
  const factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();
  const syncRules = await factory.updateSyncRules(
    updateSyncRulesFromYaml(rules, { storageVersion: storage.STORAGE_VERSION_3 })
  );
  const processingStorage = factory.getInstance(syncRules);
  await using writer = await processingStorage.createWriter(test_utils.BATCH_OPTIONS);
  await writer.markAllSnapshotDone('1/1');
  await writer.commit('1/1');

  const active = await factory.getActiveSyncConfig();
  if (active == null) {
    throw new Error('Expected an active sync config');
  }

  return { factory, storage: active.storage as MongoSyncBucketStorageV3, streamId: syncRules.replicationStreamId };
}

function parameterDocument(
  id: bigint,
  key: { t: bson.ObjectId; k: string },
  lookup: bson.Binary,
  bucket_parameters: Record<string, string>[]
): Record<string, unknown> {
  return { _id: id, key, lookup, bucket_parameters };
}

describe('Mongo parameter compaction V3', () => {
  test('compacts incrementally, honors the target boundary, and persists the cursor', async () => {
    const { factory, storage: bucketStorage, streamId } = await createActiveStorage();
    await using _factory = factory;

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const parameterCollections = await db.listParameterIndexCollections(streamId);
    expect(parameterCollections).toHaveLength(1);
    const collection = parameterCollections[0].collection as any;
    const key = { t: new bson.ObjectId(), k: 'row' };
    const lookup = new bson.Binary(Buffer.from('lookup'));

    await collection.insertMany([
      parameterDocument(100n, key, lookup, [{ id: 'old' }]),
      parameterDocument(110n, key, lookup, [{ id: 'new' }]),
      parameterDocument(120n, { ...key, k: 'deleted' }, lookup, [{ id: 'delete-me' }]),
      parameterDocument(130n, { ...key, k: 'deleted' }, lookup, []),
      parameterDocument(200n, key, lookup, [{ id: 'at-target' }])
    ]);

    await bucketStorage.compact({
      compactBuckets: [],
      compactParameterData: true,
      maxOpId: 200n
    });

    const firstPass = await collection.find({}, { sort: { _id: 1 } }).toArray();
    expect(firstPass.map((document: any) => BigInt(document._id))).toEqual([110n, 200n]);
    const firstStreamDoc = (await db.sync_rules.findOne({ _id: streamId })) as ReplicationStreamDocumentV3;
    expect(BigInt(firstStreamDoc.parameter_compaction!.compacted_before)).toBe(200n);

    // A repeated pass at the same target has no eligible range to scan.
    await bucketStorage.compact({
      compactBuckets: [],
      compactParameterData: true,
      maxOpId: 200n
    });
    await expect(collection.countDocuments({})).resolves.toBe(2);

    await collection.insertMany([
      parameterDocument(210n, key, lookup, [{ id: 'later' }]),
      parameterDocument(220n, { ...key, k: 'row' }, lookup, [])
    ]);

    // Use a small batch to exercise identities that span multiple reads. The exact-target entry
    // becomes eligible only after the target advances.
    const incremental = new MongoParameterCompactorV3(db, streamId, 300n, {}, 2);
    await incremental.compact();

    const secondPass = await collection.find({}, { sort: { _id: 1 } }).toArray();
    expect(secondPass.map((document: any) => BigInt(document._id))).toEqual([]);
    const secondStreamDoc = (await db.sync_rules.findOne({ _id: streamId })) as ReplicationStreamDocumentV3;
    expect(BigInt(secondStreamDoc.parameter_compaction!.compacted_before)).toBe(300n);
  });

  test('keeps identities scoped per parameter index while compacting them in lock-step', async () => {
    const { factory, storage: bucketStorage, streamId } = await createActiveStorage(TWO_INDEX_PARAMETER_RULES);
    await using _factory = factory;

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const parameterCollections = await db.listParameterIndexCollections(streamId);
    expect(parameterCollections).toHaveLength(2);
    const [first, second] = parameterCollections.map(({ collection }) => collection as any);

    // The same source row and lookup values in both indexes. V3 keeps the index id in the
    // collection name rather than in the lookup, so these documents are byte-identical apart from
    // their op ids - the compactor must not carry what it deleted in one index over to the other.
    const key = { t: new bson.ObjectId(), k: 'row' };
    const lookup = new bson.Binary(Buffer.from('lookup'));
    await first.insertMany([
      parameterDocument(10n, key, lookup, [{ id: 'first' }]),
      parameterDocument(40n, key, lookup, [])
    ]);
    await second.insertMany([
      parameterDocument(20n, key, lookup, [{ id: 'second' }]),
      parameterDocument(30n, key, lookup, [])
    ]);

    // One document per batch, so the two indexes are processed in interleaved turns.
    await new MongoParameterCompactorV3(db, streamId, 100n, {}, 1).compact();

    // Each tombstone removed its own index's history, and only that.
    await expect(first.countDocuments({})).resolves.toBe(0);
    await expect(second.countDocuments({})).resolves.toBe(0);
  });

  test('persists progress during a pass', async () => {
    const { factory, storage: bucketStorage, streamId } = await createActiveStorage(TWO_INDEX_PARAMETER_RULES);
    await using _factory = factory;

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const parameterCollections = await db.listParameterIndexCollections(streamId);
    const [first, second] = parameterCollections.map(({ collection }) => collection as any);
    const lookup = new bson.Binary(Buffer.from('lookup'));
    const key = (k: string) => ({ t: new bson.ObjectId(), k });
    await first.insertMany([
      parameterDocument(10n, key('a'), lookup, [{ id: 'a' }]),
      parameterDocument(30n, key('b'), lookup, [{ id: 'b' }])
    ]);
    await second.insertMany([
      parameterDocument(20n, key('c'), lookup, [{ id: 'c' }]),
      parameterDocument(40n, key('d'), lookup, [{ id: 'd' }])
    ]);

    const persisted: bigint[] = [];
    class RecordingCompactor extends MongoParameterCompactorV3 {
      protected override async persistCompactedBefore(compactedBefore: bigint): Promise<void> {
        persisted.push(compactedBefore);
        return super.persistCompactedBefore(compactedBefore);
      }
    }

    // One document per batch, and no throttling of progress writes.
    await new RecordingCompactor(db, streamId, 100n, {}, 1, 0).compact();

    // The cursor only ever covers what both indexes have passed, and it moves before the pass
    // completes, so an interruption does not lose all progress.
    expect(persisted).toEqual([...persisted].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)));
    expect(persisted.some((value) => value > 0n && value < 100n)).toBe(true);
    expect(persisted.at(-1)).toBe(100n);
  });

  test('clearing a V3 stream clears the parameter compaction cursor', async () => {
    const { factory, storage: bucketStorage, streamId } = await createActiveStorage();
    await using _factory = factory;

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    await db.sync_rules.updateOne({ _id: streamId }, {
      $set: { 'parameter_compaction.compacted_before': 123n }
    } as any);

    await bucketStorage.clear();

    const streamDoc = (await db.sync_rules.findOne({ _id: streamId })) as ReplicationStreamDocumentV3;
    expect(streamDoc.parameter_compaction).toBeUndefined();
  });
});
