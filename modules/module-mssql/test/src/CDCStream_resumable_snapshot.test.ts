import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { createTestTableWithBasicId, describeWithStorage, waitForPendingCDCChanges } from './util.js';
import { TestStorageFactory } from '@powersync/service-core';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import * as timers from 'node:timers/promises';
import { logger, ReplicationAbortedError } from '@powersync/lib-services-framework';
import { CDCStreamTestContext } from './CDCStreamTestContext.js';
import { getLatestLSN } from '@module/utils/mssql.js';

describe.skipIf(!(env.CI || env.SLOW_TESTS))('batch replication', function () {
  describeWithStorage({ timeout: 240_000 }, function (factory) {
    test('resuming initial replication (1)', async () => {
      // Stop early - likely to not include deleted row in first replication attempt.
      await testResumingReplication(factory, 2000);
    });
    test('resuming initial replication (2)', async () => {
      // Stop late - likely to include deleted row in first replication attempt.
      await testResumingReplication(factory, 8000);
    });
  });
});

async function testResumingReplication(factory: TestStorageFactory, stopAfter: number) {
  // This tests interrupting and then resuming initial replication.
  // We interrupt replication after test_data1 has fully replicated, and
  // test_data2 has partially replicated.
  // This test relies on interval behavior that is not 100% deterministic:
  // 1. We attempt to abort initial replication once a certain number of
  //    rows have been replicated, but this is not exact. Our only requirement
  //    is that we have not fully replicated test_data2 yet.
  // 2. Order of replication is not deterministic, so which specific rows
  //    have been / have not been replicated at that point is not deterministic.
  //    We do allow for some variation in the test results to account for this.

  await using context = await CDCStreamTestContext.open(factory, { cdcStreamOptions: { snapshotBatchSize: 1000 } });

  await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data1
      - SELECT * FROM test_data2`);
  const { connectionManager } = context;

  await createTestTableWithBasicId(connectionManager, 'test_data1');
  await createTestTableWithBasicId(connectionManager, 'test_data2');

  await connectionManager.query(`WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 1000) INSERT INTO test_data1(description) SELECT 'value' FROM nums OPTION (MAXRECURSION 1000)`);
  let beforeLSN = await getLatestLSN(connectionManager);
  await connectionManager.query(`WITH nums AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM nums WHERE n < 10000) INSERT INTO test_data2(description) SELECT 'value' FROM nums OPTION (MAXRECURSION 10000)`);
  await waitForPendingCDCChanges(beforeLSN, connectionManager);

  const p = context.replicateSnapshot();

  let done = false;

  const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
  try {
    (async () => {
      while (!done) {
        const count =
          ((await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0) - startRowCount;

        if (count >= stopAfter) {
          logger.info(`Stopped initial replication after replicating ${count} rows.`);
          break;
        }
        await timers.setTimeout(1);
      }
      // This interrupts initial replication
      await context.dispose();
    })();
    // This confirms that initial replication was interrupted
    const error = await p.catch((e) => e);
    expect(error).toBeInstanceOf(ReplicationAbortedError);
    done = true;
  } finally {
    done = true;
  }

  // Bypass the usual "clear db on factory open" step.
  await using context2 = await CDCStreamTestContext.open(factory, {
    doNotClear: true,
    cdcStreamOptions: { snapshotBatchSize: 1000 }
  });

  // This delete should be using one of the ids already replicated
  const {
    recordset: [deleteResult]
  } = await context2.connectionManager.query(`DELETE TOP (1) FROM test_data2 OUTPUT DELETED.id`);
  // This update should also be using one of the ids already replicated
  const id1 = deleteResult.id;
  logger.info(`Deleted row with id: ${id1}`);
  const {
    recordset: [updateResult]
  } = await context2.connectionManager.query(
    `UPDATE test_data2 SET description = 'update1' OUTPUT INSERTED.id WHERE id = (SELECT TOP 1 id FROM test_data2)`
  );
  const id2 = updateResult.id;
  logger.info(`Updated row with id: ${id2}`);
  beforeLSN = await getLatestLSN(context2.connectionManager);
  const {
    recordset: [insertResult]
  } = await context2.connectionManager.query(
    `INSERT INTO test_data2(description) OUTPUT INSERTED.id VALUES ('insert1')`
  );
  const id3 = insertResult.id;
  logger.info(`Inserted row with id: ${id3}`);
  await waitForPendingCDCChanges(beforeLSN, context2.connectionManager);

  await context2.loadNextSyncRules();
  await context2.replicateSnapshot();

  await context2.startStreaming();
  const data = await context2.getBucketData('global[]', undefined, {});

  const deletedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === String(id1));
  const updatedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === String(id2));
  const insertedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === String(id3));

  if (deletedRowOps.length != 0) {
    // The deleted row was part of the first replication batch,
    // so it is removed by streaming replication.
    expect(deletedRowOps.length).toEqual(2);
    expect(deletedRowOps[1].op).toEqual('REMOVE');
  } else {
    // The deleted row was not part of the first replication batch,
    // so it's not in the resulting ops at all.
  }

  expect(updatedRowOps.length).toEqual(2);
  // description for the first op could be 'foo' or 'update1'.
  // We only test the final version.
  expect(JSON.parse(updatedRowOps[1].data as string).description).toEqual('update1');

  expect(insertedRowOps.length).toEqual(2);
  expect(JSON.parse(insertedRowOps[0].data as string).description).toEqual('insert1');
  expect(JSON.parse(insertedRowOps[1].data as string).description).toEqual('insert1');

  // 1000 of test_data1 during first replication attempt.
  // N >= 1000 of test_data2 during first replication attempt.
  // 10000 - N - 1 + 1 of test_data2 during second replication attempt.
  // An additional update during streaming replication (2x total for this row).
  // An additional insert during streaming replication (2x total for this row).
  // If the deleted row was part of the first replication batch, it's removed by streaming replication.
  // This adds 2 ops.
  // We expect this to be 11002 for stopAfter: 2000, and 11004 for stopAfter: 8000.
  // However, this is not deterministic.
  const expectedCount = 11002 + deletedRowOps.length;
  expect(data.length).toEqual(expectedCount);

  const replicatedCount =
    ((await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0) - startRowCount;

  // With resumable replication, there should be no need to re-replicate anything.
  expect(replicatedCount).toEqual(expectedCount);
}
