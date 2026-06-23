import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { expect, test } from 'vitest';
import * as test_utils from '../test-utils/test-utils-index.js';

/**
 * Tests for {@link storage.SyncRulesBucketStorage.getBucketReport}: per-bucket operations vs live rows.
 *
 * Asserts on stable counts (operations, rows, fragmentation, totals) rather than op_ids or checksums,
 * which differ between storage backends and versions.
 */
export function registerBucketReportTests(config: storage.TestStorageConfig) {
  const generateStorageFactory = config.factory;
  const storageVersion = config.storageVersion ?? storage.CURRENT_STORAGE_VERSION;

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

  test('reports operations and live rows for a single bucket', async () => {
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
    expect(report.truncated).toEqual(false);

    const stats = report.buckets.find((b) => b.bucket === bucket)!;
    // Three inserts of distinct ids: three operations, three live rows, fully compacted (ratio 1).
    expect(stats).toMatchObject({ operations: 3, rows: 3, fragmentation: 1 });
    expect(stats.operationBytes).toBeGreaterThan(0);
    expect(report.totals).toMatchObject({ operations: 3, rows: 3 });
  });

  test('operations exceed live rows after updates, and compaction reduces fragmentation', async () => {
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
    expect(beforeStats).toMatchObject({ operations: 6, rows: 2, fragmentation: 3 });

    await bucketStorage.compact({
      clearBatchLimit: 10,
      moveBatchLimit: 10,
      moveBatchQueryLimit: 10,
      minBucketChanges: 1,
      minChangeRatio: 0
    });

    const after = await getReport(bucketStorage);
    const afterStats = after.buckets.find((b) => b.bucket === bucket)!;
    // Live rows are unchanged; the operation history shrinks toward the live row count.
    expect(afterStats.rows).toEqual(2);
    expect(afterStats.operations).toBeLessThan(beforeStats.operations);
    expect(afterStats.operations).toBeGreaterThanOrEqual(afterStats.rows);
    expect(afterStats.fragmentation).toBeLessThan(beforeStats.fragmentation);
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
    expect(report.totals).toMatchObject({ operations: 5, rows: 3 });

    // Ranked worst-first by operation count: b1 (3) before b2 (2).
    expect(report.buckets.map((b) => b.bucket)).toEqual([b1, b2]);
    expect(report.buckets.find((b) => b.bucket === b1)).toMatchObject({ operations: 3, rows: 1 });
    expect(report.buckets.find((b) => b.bucket === b2)).toMatchObject({ operations: 2, rows: 2 });
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
    expect(report.truncated).toEqual(true);
    expect(report.buckets.map((b) => b.bucket)).toEqual([b1]);
    // Totals still cover every bucket, not just the truncated list.
    expect(report.totals.bucketCount).toEqual(2);
    expect(report.totals).toMatchObject({ operations: 3, rows: 2 });
  });
}
