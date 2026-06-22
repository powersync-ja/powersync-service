import * as zstd from '@mongodb-js/zstd';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import * as bson from 'bson';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { VersionedPowerSyncMongoV3 } from '../../src/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { env } from './env.js';
import { createS3TestStorageSuite } from './helpers/s3TestFactory.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

function s3Factory() {
  const { objectStorage, factoryGen } = createS3TestStorageSuite({ url: env.MONGO_TEST_URL, isCI: env.CI });
  return { memoryStorage: objectStorage, factory: factoryGen };
}

describe('S3 read path (Phase 2c red tests)', () => {
  test('1. Round-trip write → read through S3', async () => {
    if (process.env.MINIO_ENDPOINT) return;
    const { memoryStorage, factory: factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Write two ops
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'item1', description: 'hello' },
      afterReplicaId: test_utils.rid('item1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'item2', description: 'world' },
      afterReplicaId: test_utils.rid('item2')
    });

    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    // Confirm S3 objects were uploaded (baseline)
    const storedPaths = (memoryStorage as any).store as Map<string, Buffer>;
    expect(storedPaths.size).toBeGreaterThan(0);

    // Read back via getBucketDataBatch.
    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules as any, 'global[]', 0n)])
    );
    const data = test_utils.getBatchData(batch);

    expect(data.length).toBe(2);
    expect(data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ op: 'PUT', object_id: 'item1' }),
        expect.objectContaining({ op: 'PUT', object_id: 'item2' })
      ])
    );
  });

  test('2. Missing S3 object is a hard error', async () => {
    const { memoryStorage, factory: factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'real1', description: 'real data' },
      afterReplicaId: test_utils.rid('real1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'real2', description: 'real data' },
      afterReplicaId: test_utils.rid('real2')
    });

    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const actualBucket = bucketRequest(syncRules as any, 'global[]', 0n).bucket;

    await collection.insertOne({
      _id: { b: actualBucket, o: 50n },
      min_op: 1n,
      checksum: 0n,
      count: 0,
      size: 0,
      target_op: null,
      storage_ref: {
        path: 'nonexistent/missing-object/path',
        compressed_size: 100
      }
    } as any);

    // A missing S3 object should be a hard error, not silently skipped.
    await expect(
      test_utils.fromAsync(
        bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules as any, 'global[]', 0n)])
      )
    ).rejects.toThrow('nonexistent/missing-object/path');
  });

  test('3. Read with mixed inline + S3 docs', async () => {
    if (process.env.MINIO_ENDPOINT) return;
    const { memoryStorage, factory: factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    // Write two ops through S3 writer (these go to S3 with storage_ref)
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 's3_item1', description: 'from s3' },
      afterReplicaId: test_utils.rid('s3_item1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 's3_item2', description: 'from s3' },
      afterReplicaId: test_utils.rid('s3_item2')
    });

    const flushResult = await writer.flush();
    const checkpoint = flushResult!.flushed_op;

    // Extract source_table and source_key from the S3-stored ops to reuse
    // in the inline document, ensuring valid identifiers for mapOpEntry.
    const storedPaths = (memoryStorage as any).store as Map<string, Buffer>;
    const [_, compressed] = [...storedPaths.entries()][0];
    const decompressed = await zstd.decompress(compressed);
    const wrapper = bson.deserialize(decompressed, { promoteValues: false });
    const s3Ops = wrapper.ops as any[];
    const s3SourceTable = s3Ops[0].source_table as bson.ObjectId;
    const s3SourceKey = s3Ops[0].source_key as bson.UUID;

    // Directly insert an inline document (ops present, no storage_ref)
    // into the same bucket_data collection.
    const db = bucketStorage.db as VersionedPowerSyncMongoV3;
    const definitionId = bucketStorage.mapping.allBucketDefinitionIds()[0];
    const collection = db.bucketData(bucketStorage.replicationStreamId, definitionId);
    const actualBucket = bucketRequest(syncRules as any, 'global[]', 0n).bucket;

    await collection.insertOne({
      _id: { b: actualBucket, o: 100n },
      min_op: 1n,
      checksum: 999n,
      count: 1,
      size: 50,
      target_op: null,
      ops: [
        {
          o: 2n,
          op: 'PUT',
          source_table: s3SourceTable,
          source_key: s3SourceKey,
          table: 'items',
          row_id: 'inline_item1',
          checksum: 999n,
          data: JSON.stringify({ id: 'inline_item1', description: 'inline' })
        }
      ]
      // No storage_ref — this doc stores ops inline
    } as any);

    // Read back. Both S3-backed and inline ops should be returned.
    const batch = await test_utils.fromAsync(
      bucketStorage.getBucketDataBatch(checkpoint, [bucketRequest(syncRules as any, 'global[]', 0n)])
    );
    const data = test_utils.getBatchData(batch);

    // When implemented: 3 ops total (2 from S3 + 1 inline).
    // Currently: only 1 op (inline) because S3 ops are not fetched.
    expect(data.length).toBe(3);
    expect(data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ op: 'PUT', object_id: 's3_item1' }),
        expect.objectContaining({ op: 'PUT', object_id: 's3_item2' }),
        expect.objectContaining({ op: 'PUT', object_id: 'inline_item1' })
      ])
    );
  });
});
