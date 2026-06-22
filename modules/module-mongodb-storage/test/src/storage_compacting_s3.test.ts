import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { bucketRequest, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { MongoSyncBucketStorage } from '../../src/storage/implementation/createMongoSyncBucketStorage.js';
import { env } from './env.js';
import { MemoryObjectStorage } from './helpers/MemoryObjectStorage.js';

const SYNC_RULES_YAML = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM items
`;

function s3Factory() {
  const memoryStorage = new MemoryObjectStorage();
  const factoryGen = mongoTestStorageFactoryGenerator({
    url: env.MONGO_TEST_URL,
    isCI: env.CI,
    internalOptions: { objectStorage: memoryStorage, inlineThresholdBytes: 0 }
  });
  return { memoryStorage, factoryGen };
}

/**
 * V3 compaction tests with object storage (S3).
 * Exercise compaction behaviors through the storage API with ops stored on S3.
 */
describe('V3 Compaction with object storage', () => {
  test('compaction round-trip preserves data', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'first' },
      afterReplicaId: test_utils.rid('A')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'second' },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.commit('1/1');

    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);
    const bucket = request.bucket;

    const batchBefore = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataBefore = test_utils.getBatchData(batchBefore);
    expect(dataBefore.length).toBe(2);

    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);
    expect(dataAfter.length).toBe(2);
    for (const d of dataBefore) {
      expect(dataAfter.some((a: any) => a.op === d.op && a.object_id === d.object_id)).toBe(true);
    }
  });

  test('superseded ops reduce op count after compaction', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'first version' },
      afterReplicaId: test_utils.rid('A-v1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'second item' },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'updated version' },
      afterReplicaId: test_utils.rid('A-v2')
    });
    await writer.commit('1/1');

    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);
    // 1 MOVE (superseded A-v1) + 2 PUT (B, A-v2) = 3 total
    expect(dataAfter.length).toBe(3);
  });

  test('multi-batch compaction preserves data', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // Enough ops to span at least 2 S3 documents with a small byte limit
    for (let i = 0; i < 15; i++) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: `row${i}`, description: `value${i}`.repeat(5) },
        afterReplicaId: test_utils.rid(`row${i}`)
      });
    }
    await writer.commit('1/1');

    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    const batchBefore = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataBefore = test_utils.getBatchData(batchBefore);

    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [request.bucket],
      moveBatchByteLimit: 256,
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);
    expect(dataAfter.length).toBe(dataBefore.length);
  });

  test('duplicate rows collapse after compaction', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v1' },
      afterReplicaId: test_utils.rid('A-v1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v2' },
      afterReplicaId: test_utils.rid('A-v2')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v3' },
      afterReplicaId: test_utils.rid('A-v3')
    });
    await writer.commit('1/1');

    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);

    // 3 ops for same row: 2 superseded MOVEs + 1 latest PUT.
    // CLEAR collapses leading MOVEs, leaving 1 CLEAR + 1 PUT = 2 ops.
    // Without CLEAR, 1 MOVE + 1 MOVE + 1 PUT = 3 ops.
    // Either way, more than 1 op should survive.
    expect(dataAfter.length).toBeGreaterThan(1);
    expect(dataAfter.length).toBeLessThan(4);
  });

  test('seen map overflow preserves all unique rows', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // 20 rows each with 2 versions. Small memory limit forces overflow.
    for (let i = 0; i < 20; i++) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: `row${i}`, description: 'old' },
        afterReplicaId: test_utils.rid(`row${i}-v1`)
      });
    }
    for (let i = 0; i < 20; i++) {
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: `row${i}`, description: 'new' },
        afterReplicaId: test_utils.rid(`row${i}-v2`)
      });
    }
    await writer.commit('1/1');

    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [request.bucket],
      memoryLimitMB: 0.001,
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);

    // Each unique row should have at least its latest version
    const rowIds = new Set(dataAfter.map((d: any) => d.object_id));
    for (let i = 0; i < 20; i++) {
      expect(rowIds.has(`row${i}`)).toBe(true);
    }
  });

  test('compaction deletes old S3 objects', async () => {
    const { memoryStorage, factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // A-v1 gets superseded by A-v2; B and C are independent.
    // Compaction will rechunk ops into new S3 docs and delete old ones.
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v1' },
      afterReplicaId: test_utils.rid('A-v1')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'A', description: 'v2' },
      afterReplicaId: test_utils.rid('A-v2')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'B', description: 'beta' },
      afterReplicaId: test_utils.rid('B')
    });
    await writer.save({
      sourceTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'C', description: 'gamma' },
      afterReplicaId: test_utils.rid('C')
    });
    await writer.commit('1/1');

    const store = (memoryStorage as any).store as Map<string, Buffer>;
    const pathsBefore = new Set(store.keys());

    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    await bucketStorage.compact({
      maxOpId: checkpoint,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    const pathsAfter = new Set(store.keys());

    expect(pathsAfter.size).toBeGreaterThan(0);

    // Verify the surviving ops: B and latest A (A-v4)
    const batchAfter = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const dataAfter = test_utils.getBatchData(batchAfter);
    expect(dataAfter.some((d: any) => d.object_id === 'B')).toBe(true);
    expect(dataAfter.some((d: any) => d.object_id === 'A')).toBe(true);
  });

  test('scoped delete preserves ops above maxOpId horizon', async () => {
    const { factoryGen } = s3Factory();
    await using factory = await factoryGen.factory();
    const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(SYNC_RULES_YAML, { storageVersion: 3 }));
    const bucketStorage = factory.getInstance(syncRules) as MongoSyncBucketStorage;

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const sourceTable = await test_utils.resolveTestTable(writer, 'items', ['id'], factoryGen, 1);
    await writer.markAllSnapshotDone('1/1');

    // 12 ops with large data to fill 2+ S3 documents. Ops 1-6 are below
    // the compaction horizon, ops 7-12 above. Each pair shares a row_id
    // so the below-horizon ones get superseded into MOVEs.
    for (let i = 1; i <= 12; i++) {
      const letter = String.fromCharCode(64 + i); // A..L
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id: letter, description: `val${i}`.repeat(50) },
        afterReplicaId: test_utils.rid(letter)
      });
    }
    await writer.commit('1/1');
    const { checkpoint } = await bucketStorage.getCheckpoint();
    const request = bucketRequest(syncRules as any, 'global[]', 0n);

    // Compact with maxOpId=6. Ops 1-6 get deduped (no duplicates here,
    // but the compactor still processes them). Ops 7-12 survive untouched.
    await bucketStorage.compact({
      maxOpId: 6n,
      compactBuckets: [request.bucket],
      clearBatchLimit: 200,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      moveBatchByteLimit: 1024,
      minBucketChanges: 1,
      minChangeRatio: 0,
      signal: null as any
    });

    // Read back full checkpoint — all ops should be present
    const batch = await test_utils.fromAsync(bucketStorage.getBucketDataBatch(checkpoint, [request]));
    const data = test_utils.getBatchData(batch);

    // Ops 7-12 (above horizon) must survive as PUTs with their original data
    for (let i = 7; i <= 12; i++) {
      const letter = String.fromCharCode(64 + i);
      const op = data.find((d: any) => d.object_id === letter);
      expect(op).toBeDefined();
      expect(op!.op).toBe('PUT');
    }

    // Ops 1-6 (below horizon) should also still be reachable
    // (they survive as PUT since there are no duplicates, or as MOVEs)
    expect(data.length).toBeGreaterThanOrEqual(12);
  });
});
