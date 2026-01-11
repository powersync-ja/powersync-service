import { TestStorageFactory } from '@powersync/service-core';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { ReplicationMetric } from '@powersync/service-types';
import * as timers from 'node:timers/promises';
import { describe, expect, test } from 'vitest';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { env } from './env.js';
import { describeWithStorage } from './util.js';

describe.skipIf(!(env.CI || env.SLOW_TESTS))('batch replication', function () {
  describeWithStorage({ timeout: 240_000 }, function (config) {
    test('resuming initial replication (1)', async () => {
      // Stop early - likely to not include deleted row in first replication attempt.
      await testResumingReplication(config.factory, 2000);
    });
    test('resuming initial replication (2)', async () => {
      // Stop late - likely to include deleted row in first replication attempt.
      await testResumingReplication(config.factory, 8000);
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

  let startRowCount: number;

  {
    await using context = await ChangeStreamTestContext.open(factory, { streamOptions: { snapshotChunkLength: 1000 } });

    await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM test_data1
      - SELECT _id as id, description FROM test_data2`);
    const { db } = context;

    let batch = db.collection('test_data1').initializeUnorderedBulkOp();
    for (let i = 1; i <= 1000; i++) {
      batch.insert({ _id: i, description: 'foo' });
    }
    await batch.execute();
    batch = db.collection('test_data2').initializeUnorderedBulkOp();
    for (let i = 1; i <= 10000; i++) {
      batch.insert({ _id: i, description: 'foo' });
    }
    await batch.execute();

    const p = context.replicateSnapshot().catch((e) => ({ error: e }));

    let done = false;

    startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    try {
      (async () => {
        while (!done) {
          const count =
            ((await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0) - startRowCount;

          if (count >= stopAfter) {
            break;
          }
          await timers.setTimeout(1);
        }
        // This interrupts initial replication
        // We don't dispose the context here yet, since closing the database connection while in use
        // results in unpredictable error conditions.
        context.abort();
      })();
      // This confirms that initial replication was interrupted
      await expect(await p).haveOwnProperty('error');
    } finally {
      done = true;
    }
  }

  {
    // Bypass the usual "clear db on factory open" step.
    await using context2 = await ChangeStreamTestContext.open(factory, {
      doNotClear: true,
      streamOptions: { snapshotChunkLength: 1000 }
    });

    const { db } = context2;

    // This delete should be using one of the ids already replicated
    await db.collection('test_data2').deleteOne({ _id: 1 as any });
    await db.collection('test_data2').updateOne({ _id: 2 as any }, { $set: { description: 'update1' } });
    await db.collection('test_data2').insertOne({ _id: 10001 as any, description: 'insert1' });

    await context2.loadNextSyncRules();
    await context2.replicateSnapshot();

    context2.startStreaming();
    const data = await context2.getBucketData('global[]', undefined, {});

    const deletedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === '1');
    const updatedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === '2');
    const insertedRowOps = data.filter((row) => row.object_type == 'test_data2' && row.object_id === '10001');

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
}
