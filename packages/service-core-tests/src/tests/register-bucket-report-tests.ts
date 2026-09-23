import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';
import { compactActive } from './util.js';

/**
 * Tests for {@link storage.SyncRulesBucketStorage.getBucketReport}: per-bucket operations, with row counts
 * and fragmentation derived from each bucket's last full compact.
 *
 * The report reads only bucket_state, never the operation history. Operation counts are exact for every
 * storage version; row-derived fields exist only on storage versions that capture full-compact statistics
 * (v3), and only after a bucket's first full compact. On v1/v2 they are always null with a suggested action
 * of `unknown`.
 */
export function registerBucketReportTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;
  const storageVersion = config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;
  // v3 bucket_state captures full-compact statistics (rows, fragmentation, compact scheduling).
  const capturesCompactStats = storageVersion >= 3;

  const GLOBAL_SYNC_RULES = `
bucket_definitions:
  global:
    data: [select * from test]
`;

  // A constant parameter query keeps op_ids stable across backends (no bucket_parameter records); the data
  // query routes each row into a bucket keyed by its own `b` value, so rows land in grouped["b1"]/grouped["b2"].
  const GROUPED_SYNC_RULES = ` bucket_definitions:
    grouped:
      parameters: select 'b' as b
      data:
        - select * from test where b = bucket.b`;

  const getReport = (bucketStorage: storage.SyncRulesBucketStorage, options?: storage.GetBucketReportOptions) => {
    if (bucketStorage.getBucketReport == null) {
      throw new Error('Storage backend does not implement getBucketReport');
    }
    return bucketStorage.getBucketReport(options);
  };

  // An explicit per-bucket compact always runs a full compact of that bucket, regardless of scheduling.
  // Compact through the active sync config: the instance used by the writer retains its original
  // PROCESSING stream snapshot, which some storage versions refuse to compact.
  const compactBucket = (factory: storage.BucketStorageFactory, bucket: string) =>
    compactActive(factory, {
      compactBuckets: [bucket],
      clearBatchLimit: 10,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

  test('reports operation counts for a single bucket', async () => {
    await using factory = await generateStorageFactory();
    const { stream, content } = await test_utils.deploySyncRules(
      factory,
      updateSyncRulesFromYaml(GLOBAL_SYNC_RULES, { storageVersion })
    );
    const bucketStorage = factory.getInstance(stream);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    for (const id of ['t1', 't2', 't3']) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id },
        afterReplicaId: test_utils.rid(id)
      });
    }
    await writer.commit('1/1');
    await writer.flush();

    const bucket = test_utils.bucketRequest(content, 'global[]').bucket;
    const report = await getReport(bucketStorage);

    expect(report.totals.bucketCount).toEqual(1);
    expect(report.bucketsTruncated).toEqual(false);
    expect(report.definitionsTruncated).toEqual(false);

    const stats = report.buckets.find((b) => b.bucket === bucket)!;
    // Three inserts of distinct ids: three operations. The bucket has never been fully compacted, so no
    // row-derived fields exist yet.
    expect(stats).toMatchObject({
      operations: 3,
      // Never compacted: the whole history counts as uncompacted.
      uncompactedOperations: 3,
      rows: null,
      fragmentation: null,
      lastFullCompactAt: null,
      suggestedAction: 'unknown'
    });
    expect(stats.operationBytes).toBeGreaterThan(0);
    expect(report.totals).toMatchObject({ operations: 3, estimated: false });

    // The definition rollup aggregates the single bucket. The definition name is the bucket-name prefix.
    expect(report.definitions).toHaveLength(1);
    expect(report.definitions[0]).toMatchObject({
      definition: bucket.split('[')[0],
      bucketCount: 1,
      operations: 3,
      rows: null,
      suggestedAction: 'unknown'
    });
  });

  test('derives rows and fragmentation from the last full compact', async () => {
    await using factory = await generateStorageFactory();
    const { stream, content } = await test_utils.deploySyncRules(
      factory,
      updateSyncRulesFromYaml(GLOBAL_SYNC_RULES, { storageVersion })
    );
    const bucketStorage = factory.getInstance(stream);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    // Two rows, each inserted then updated twice: six operations over two live rows.
    for (const id of ['t1', 't2']) {
      for (const value of ['a', 'b', 'c']) {
        await writer.save({
          sourceTable: testTable,
          tag: value === 'a' ? storage.SaveOperationTag.INSERT : storage.SaveOperationTag.UPDATE,
          after: { id, value },
          afterReplicaId: test_utils.rid(id)
        });
      }
    }
    await writer.commit('1/1');
    await writer.flush();

    const bucket = test_utils.bucketRequest(content, 'global[]').bucket;

    const before = await getReport(bucketStorage);
    const beforeStats = before.buckets.find((b) => b.bucket === bucket)!;
    // Operation counts are exact even before any compact; rows are unknown until one runs.
    expect(beforeStats).toMatchObject({
      operations: 6,
      uncompactedOperations: 6,
      rows: null,
      fragmentation: null,
      suggestedAction: 'unknown'
    });
    if (capturesCompactStats) {
      // Writers schedule the bucket for compaction, which the report surfaces.
      expect(beforeStats.nextCompactAt).toBeInstanceOf(Date);
    }

    await compactBucket(factory, bucket);

    const after = await getReport(bucketStorage);
    const afterStats = after.buckets.find((b) => b.bucket === bucket)!;
    if (capturesCompactStats) {
      // The full compact counted two live rows and shrank the history toward them.
      expect(afterStats.rows).toEqual(2);
      expect(afterStats.operations).toBeLessThan(beforeStats.operations);
      expect(afterStats.operations).toBeGreaterThanOrEqual(2);
      expect(afterStats.fragmentation).toEqual(afterStats.operations / 2);
      expect(afterStats.lastFullCompactAt).toBeInstanceOf(Date);
      // The compact covered the whole history, so the row stats are fully fresh.
      expect(afterStats.uncompactedOperations).toEqual(0);

      // The definition rollup derives its rows from the same compact statistics.
      expect(after.definitions).toHaveLength(1);
      expect(after.definitions[0]).toMatchObject({ bucketCount: 1, rows: 2 });
    } else {
      // v1/v2 storage does not capture compact statistics: the report stays limited to operation counts.
      expect(afterStats.rows).toBeNull();
      expect(afterStats.fragmentation).toBeNull();
      expect(afterStats.suggestedAction).toEqual('unknown');
      // The compact itself still shrinks the operation history.
      expect(afterStats.operations).toBeLessThan(beforeStats.operations);
    }
  });

  test('reports every bucket, ranks worst-first, and totals across all buckets', async () => {
    await using factory = await generateStorageFactory();
    const { stream, content } = await test_utils.deploySyncRules(
      factory,
      updateSyncRulesFromYaml(GROUPED_SYNC_RULES, { storageVersion })
    );
    const bucketStorage = factory.getInstance(stream);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    // grouped["b1"]: one row, three operations (insert + two updates).
    for (const value of ['a', 'b', 'c']) {
      await writer.save({
        sourceTable: testTable,
        tag: value === 'a' ? storage.SaveOperationTag.INSERT : storage.SaveOperationTag.UPDATE,
        after: { id: 't1', b: 'b1', value },
        afterReplicaId: test_utils.rid('t1')
      });
    }
    // grouped["b2"]: two rows, two operations.
    for (const id of ['t2', 't3']) {
      await writer.save({
        sourceTable: testTable,
        tag: storage.SaveOperationTag.INSERT,
        after: { id, b: 'b2' },
        afterReplicaId: test_utils.rid(id)
      });
    }
    await writer.commit('1/1');
    await writer.flush();

    const b1 = test_utils.bucketRequest(content, 'grouped["b1"]').bucket;
    const b2 = test_utils.bucketRequest(content, 'grouped["b2"]').bucket;

    const report = await getReport(bucketStorage);
    expect(report.totals.bucketCount).toEqual(2);
    expect(report.totals).toMatchObject({ operations: 5, estimated: false });

    // Ranked worst-first by operation count: b1 (3) before b2 (2).
    expect(report.buckets.map((b) => b.bucket)).toEqual([b1, b2]);
    expect(report.buckets.find((b) => b.bucket === b1)).toMatchObject({ operations: 3 });
    expect(report.buckets.find((b) => b.bucket === b2)).toMatchObject({ operations: 2 });

    // Both buckets belong to one definition; the rollup sums them.
    expect(report.definitions).toHaveLength(1);
    expect(report.definitions[0]).toMatchObject({
      definition: b1.split('[')[0],
      bucketCount: 2,
      operations: 5
    });

    // operationBytes is an aggregated ($toDouble) sum; assert every bucket is non-zero and that the
    // per-bucket bytes add up to the instance total.
    expect(report.totals.operationBytes).toBeGreaterThan(0);
    for (const bucket of report.buckets) {
      expect(bucket.operationBytes).toBeGreaterThan(0);
    }
    const summedBytes = report.buckets.reduce((total, bucket) => total + bucket.operationBytes, 0);
    expect(summedBytes).toEqual(report.totals.operationBytes);
  });

  test('limit truncates the bucket list but totals still span all buckets', async () => {
    await using factory = await generateStorageFactory();
    const { stream, content } = await test_utils.deploySyncRules(
      factory,
      updateSyncRulesFromYaml(GROUPED_SYNC_RULES, { storageVersion })
    );
    const bucketStorage = factory.getInstance(stream);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    // grouped["b1"]: two operations; grouped["b2"]: one operation.
    for (const value of ['a', 'b']) {
      await writer.save({
        sourceTable: testTable,
        tag: value === 'a' ? storage.SaveOperationTag.INSERT : storage.SaveOperationTag.UPDATE,
        after: { id: 't1', b: 'b1', value },
        afterReplicaId: test_utils.rid('t1')
      });
    }
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 't2', b: 'b2' },
      afterReplicaId: test_utils.rid('t2')
    });
    await writer.commit('1/1');
    await writer.flush();

    const b1 = test_utils.bucketRequest(content, 'grouped["b1"]').bucket;

    const report = await getReport(bucketStorage, { limit: 1 });
    expect(report.bucketsTruncated).toEqual(true);
    expect(report.buckets.map((b) => b.bucket)).toEqual([b1]);
    // Totals still cover every bucket, not just the truncated list.
    expect(report.totals.bucketCount).toEqual(2);
    expect(report.totals).toMatchObject({ operations: 3, estimated: false });
  });

  test('caps the definition rollup and flags the truncation', async () => {
    // Two definitions past the rollup cap; a single row lands in every definition's global bucket.
    const definitionCount = storage.BUCKET_REPORT_DEFINITION_LIMIT + 2;
    const manyDefinitions =
      'bucket_definitions:\n' +
      Array.from({ length: definitionCount }, (_, i) => `  def${i}:\n    data: [select * from test]\n`).join('');

    await using factory = await generateStorageFactory();
    const { stream } = await test_utils.deploySyncRules(
      factory,
      updateSyncRulesFromYaml(manyDefinitions, { storageVersion })
    );
    const bucketStorage = factory.getInstance(stream);

    await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
    const testTable = await test_utils.resolveTestTable(writer, 'test', ['id'], config);
    await writer.markAllSnapshotDone('1/1');
    await writer.save({
      sourceTable: testTable,
      tag: storage.SaveOperationTag.INSERT,
      after: { id: 't1' },
      afterReplicaId: test_utils.rid('t1')
    });
    await writer.commit('1/1');
    await writer.flush();

    const report = await getReport(bucketStorage);
    expect(report.totals.bucketCount).toEqual(definitionCount);
    expect(report.bucketsTruncated).toEqual(false);
    expect(report.definitions).toHaveLength(storage.BUCKET_REPORT_DEFINITION_LIMIT);
    expect(report.definitionsTruncated).toEqual(true);
  });
}
