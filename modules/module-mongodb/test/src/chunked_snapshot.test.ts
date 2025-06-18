import { mongo } from '@powersync/lib-service-mongodb';
import { reduceBucket, TestStorageFactory } from '@powersync/service-core';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { JSONBig } from '@powersync/service-jsonbig';
import { SqliteJsonValue } from '@powersync/service-sync-rules';
import * as timers from 'timers/promises';
import { describe, expect, test } from 'vitest';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { describeWithStorage } from './util.js';

describe('chunked snapshots', () => {
  describeWithStorage({ timeout: 120_000 }, defineBatchTests);
});

function defineBatchTests(factory: TestStorageFactory) {
  // This is not as sensitive to the id type as postgres, but we still test a couple of cases
  test('chunked snapshot (int32)', async () => {
    await testChunkedSnapshot({
      generateId(i) {
        return i;
      },
      idToSqlite(id: number) {
        return BigInt(id);
      }
    });
  });

  test('chunked snapshot (Timestamp)', async () => {
    await testChunkedSnapshot({
      generateId(i) {
        return mongo.Timestamp.fromBits(Math.floor(i / 1000), i % 1000);
      },
      idToSqlite(id: mongo.Timestamp) {
        return id.toBigInt();
      }
    });
  });

  test('chunked snapshot (compound)', async () => {
    await testChunkedSnapshot({
      generateId(i) {
        return { a: Math.floor(i / 100), b: i % 100 };
      },
      idToSqlite(id: any) {
        return JSON.stringify(id);
      }
    });
  });

  test('chunked snapshot (float)', async () => {
    await testChunkedSnapshot({
      generateId(i) {
        // Floating-point operations are not exact, but it should be consistent at least
        return i / Math.PI;
      },
      idToSqlite(id: any) {
        return id;
      }
    });
  });

  async function testChunkedSnapshot(options: {
    generateId: (i: number) => any;
    idToSqlite?: (id: any) => SqliteJsonValue;
  }) {
    // This is not quite as much of an edge cases as with Postgres. We do still test that
    // updates applied while replicating are applied correctly.
    const idToSqlite = options.idToSqlite ?? ((n) => n);
    const idToString = (id: any) => String(idToSqlite(id));

    await using context = await ChangeStreamTestContext.open(factory, {
      // We need to use a smaller chunk size here, so that we can run a query in between chunks
      streamOptions: { snapshotChunkLength: 100 }
    });

    await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      - SELECT _id as id, description FROM test_data`);
    const { db } = context;

    let batch = db.collection('test_data').initializeUnorderedBulkOp();

    // 1. Start with 2k rows...
    for (let i = 1; i <= 2000; i++) {
      batch.insert({ _id: options.generateId(i), description: 'foo' });
    }
    await batch.execute();

    // 2. Replicate one batch of rows
    // Our "stopping point" here is not quite deterministic.
    const p = context.replicateSnapshot();

    const stopAfter = 100;
    const startRowCount = (await METRICS_HELPER.getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;

    while (true) {
      const count =
        ((await METRICS_HELPER.getMetricValueForTests('powersync_rows_replicated_total')) ?? 0) - startRowCount;

      if (count >= stopAfter) {
        break;
      }
      await timers.setTimeout(1);
    }

    // 3. Update some records
    const idA = options.generateId(2000);
    const idB = options.generateId(1);
    await db.collection('test_data').updateOne({ _id: idA }, { $set: { description: 'bar' } });
    await db.collection('test_data').updateOne({ _id: idB }, { $set: { description: 'baz' } });

    // 4. Delete
    const idC = options.generateId(1999);
    await db.collection('test_data').deleteOne({ _id: idC });

    // 5. Insert
    const idD = options.generateId(2001);
    await db.collection('test_data').insertOne({ _id: idD, description: 'new' });

    // 4. Replicate the rest of the table.
    await p;

    context.startStreaming();

    const data = await context.getBucketData('global[]');
    const reduced = reduceBucket(data);

    expect(reduced.find((row) => row.object_id == idToString(idA))?.data).toEqual(
      JSONBig.stringify({
        id: idToSqlite(idA),
        description: 'bar'
      })
    );

    expect(reduced.find((row) => row.object_id == idToString(idB))?.data).toEqual(
      JSONBig.stringify({
        id: idToSqlite(idB),
        description: 'baz'
      })
    );

    expect(reduced.find((row) => row.object_id == idToString(idC))).toBeUndefined();

    expect(reduced.find((row) => row.object_id == idToString(idD))?.data).toEqual(
      JSONBig.stringify({
        id: idToSqlite(idD),
        description: 'new'
      })
    );
    expect(reduced.length).toEqual(2001);
  }
}
