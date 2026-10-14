import { mongo } from '@powersync/lib-service-mongodb';
import { SingleSyncConfigBucketDefinitionMapping, storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { SqlSyncRules, withBucketSource } from '@powersync/service-sync-rules';
import { expect, test } from 'vitest';
import { PowerSyncMongo } from '../../src/storage/implementation/db.js';
import { getMongoStorageConfig } from '../../src/storage/implementation/models.js';
import { MongoIdSequence } from '../../src/storage/implementation/MongoIdSequence.js';
import { PersistedBatchV1 } from '../../src/storage/implementation/v1/PersistedBatchV1.js';

class InspectableBatch extends PersistedBatchV1 {
  get puts() {
    return this.bucketData;
  }
}

test('serialized data produces the same queued payload, checksum and size as object data', async () => {
  // This tests in-memory operation construction only; no connection or database writes.
  const client = new mongo.MongoClient('mongodb://127.0.0.1:27017');
  try {
    const db = new PowerSyncMongo(client).versioned(getMongoStorageConfig(1));
    const { config } = SqlSyncRules.fromYaml('bucket_definitions:\n  global:\n    data:\n      - SELECT * FROM docs', {
      defaultSchema: 'public',
      throwOnError: true
    });
    const table = new storage.SourceTable({
      id: new mongo.ObjectId(),
      ref: { connectionTag: 'default', schema: 'public', name: 'docs' },
      objectId: undefined,
      replicaIdColumns: [],
      snapshotComplete: true,
      bucketDataSources: config.bucketDataSources,
      parameterLookupSources: []
    });
    const data = { id: 'x', big: 9007199254740993n, real: 1.0, text: '"\\\n😀' };
    const serialized = JSONBig.stringify(data);
    const batches = [
      withBucketSource({ bucket: 'global[]', table: 'docs', id: 'x', data }, config.bucketDataSources[0]),
      withBucketSource({ bucket: 'global[]', table: 'docs', id: 'x', data: serialized }, config.bucketDataSources[0])
    ].map((evaluated) => {
      const batch = new InspectableBatch(db, 1, new SingleSyncConfigBucketDefinitionMapping(), 0);
      batch.saveBucketData({
        table,
        sourceKey: 'x',
        op_seq: new MongoIdSequence(0n),
        before_buckets: [],
        evaluated: [evaluated]
      });
      return batch;
    });
    expect(batches[1].puts).toEqual(batches[0].puts);
    expect(batches[1].currentSize).toBe(batches[0].currentSize);
    expect(batches[1].puts[0]).toMatchObject({
      data: serialized,
      checksum: BigInt(utils.hashData('docs', 'x', serialized))
    });
  } finally {
    await client.close();
  }
});
