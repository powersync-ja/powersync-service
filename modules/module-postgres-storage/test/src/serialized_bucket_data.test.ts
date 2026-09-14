import { storage, utils } from '@powersync/service-core';
import { JSONBig } from '@powersync/service-jsonbig';
import { SqlSyncRules, withBucketSource } from '@powersync/service-sync-rules';
import { expect, test } from 'vitest';
import { PostgresPersistedBatch } from '../../src/storage/batch/PostgresPersistedBatch.js';

class InspectableBatch extends PostgresPersistedBatch {
  get puts() {
    return this.bucketDataInserts;
  }
}

test('serialized data produces the same stored payload, checksum and size as object data', () => {
  const { config } = SqlSyncRules.fromYaml('bucket_definitions:\n  global:\n    data:\n      - SELECT * FROM docs', {
    defaultSchema: 'public',
    throwOnError: true
  });
  const table = new storage.SourceTable({
    id: '1',
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
    const batch = new InspectableBatch({
      group_id: 1,
      storageConfig: storage.STORAGE_VERSION_CONFIG[2]!,
      max_estimated_size: 1000000,
      max_record_count: 1000,
      max_current_data_batch_size: 1000000
    });
    batch.saveBucketData({
      table,
      source_key: 'x',
      before_buckets: [],
      evaluated: [evaluated]
    });
    return batch;
  });
  expect(batches[1].puts).toEqual(batches[0].puts);
  expect(batches[1].currentSize).toBe(batches[0].currentSize);
  expect(batches[1].puts[0]).toMatchObject({ data: serialized, checksum: utils.hashData('docs', 'x', serialized) });
});
