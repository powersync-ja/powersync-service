import { MongoSyncBucketStorageV3 } from '@module/storage/implementation/v3/MongoSyncBucketStorageV3.js';
import { VersionedPowerSyncMongoV3 } from '@module/storage/implementation/v3/VersionedPowerSyncMongoV3.js';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';

/**
 * Above the sampling threshold the report ranks a sample of bucket_state instead of scanning it in full,
 * and scales the totals back up. Replicating enough buckets to cross the threshold would dominate the
 * test's runtime, so this replicates one real bucket and clones its state document past the threshold.
 *
 * The clones are identical, which makes the scaled estimates exact (the sample-rate factor cancels:
 * `sampleCount * count * (matched / sampleCount) == matched * count`), so the assertions can use equality
 * instead of tolerances.
 */
describe('bucket report sampling - mongodb v3', () => {
  // Seeding 55k bucket_state documents takes longer than the default test timeout.
  test('samples and scales above the bucket threshold', { timeout: 60_000 }, async () => {
    await using factory = await INITIALIZED_MONGO_STORAGE_FACTORY.factory();

    const deployed = await factory.updateSyncRules(
      updateSyncRulesFromYaml(
        `
config:
  edition: 3

streams:
  by_owner:
    query: SELECT * FROM todos WHERE owner_id = subscription.parameter('owner_id')
`,
        { storageVersion: 3 }
      )
    );
    const bucketStorage = factory.getInstance(deployed) as MongoSyncBucketStorageV3;
    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const todosTable = await test_utils.resolveTestTable(writer, 'todos', ['id'], INITIALIZED_MONGO_STORAGE_FACTORY);
    await writer.save({
      sourceTable: todosTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 'todo-1', owner_id: 'user-1' },
      afterReplicaId: test_utils.rid('todo-1')
    });
    await writer.markAllSnapshotDone('1/1');
    await writer.commit('1/1');
    await writer.flush();

    // Below the threshold the report is an exact scan.
    const exactReport = await bucketStorage.getBucketReport();
    expect(exactReport.totals).toEqual({ bucketCount: 1, operations: 1, operationBytes: expect.any(Number), estimated: false });

    const bucketStateCollection = (bucketStorage.db as VersionedPowerSyncMongoV3).bucketState(
      bucketStorage.replicationStreamId
    );
    const seed = await bucketStateCollection.findOne({});
    if (seed == null) {
      throw new Error('Expected a bucket_state document for the replicated bucket');
    }
    const definition = seed._id.b.split('[')[0];

    const CLONES = 55_000;
    const INSERT_BATCH = 10_000;
    // last_op has a unique index (bucket_updates), so every clone needs its own value.
    const baseOp = BigInt(String(seed.last_op));
    for (let offset = 0; offset < CLONES; offset += INSERT_BATCH) {
      const batch = Array.from({ length: Math.min(INSERT_BATCH, CLONES - offset) }, (_, i) => ({
        ...seed,
        _id: { d: seed._id.d, b: `${definition}["clone${offset + i}"]` },
        last_op: baseOp + BigInt(offset + i + 1)
      }));
      await bucketStateCollection.insertMany(batch, { ordered: false });
    }

    const totalBuckets = CLONES + 1;
    const operationsPerBucket = seed.bucket_stats.count;
    const bytesPerBucket = Number(String(seed.bucket_stats.bytes));

    const limit = 10;
    const report = await bucketStorage.getBucketReport({ limit });

    expect(report.totals.estimated).toBe(true);
    // The matched-bucket count is exact even when sampling.
    expect(report.totals.bucketCount).toBe(totalBuckets);
    expect(report.totals.operations).toBe(totalBuckets * operationsPerBucket);
    expect(report.totals.operationBytes).toBe(totalBuckets * bytesPerBucket);

    // The returned buckets are real sampled documents, so their per-bucket stats are exact, not scaled.
    expect(report.buckets).toHaveLength(limit);
    for (const bucket of report.buckets) {
      expect(bucket.operations).toBe(operationsPerBucket);
      expect(bucket.operationBytes).toBe(bytesPerBucket);
    }
    expect(report.bucketsTruncated).toBe(true);

    expect(report.definitions).toHaveLength(1);
    expect(report.definitions[0].definition).toBe(definition);
    expect(report.definitions[0].bucketCount).toBe(totalBuckets);
    expect(report.definitions[0].operations).toBe(totalBuckets * operationsPerBucket);
    // No full compact has run, so no row stats exist to derive from.
    expect(report.definitions[0].rows).toBeNull();
    expect(report.definitions[0].suggestedAction).toBe('unknown');
  });
});
