import { reduceBucket, TestStorageFactory } from '@powersync/service-core';
import { METRICS_HELPER } from '@powersync/service-core-tests';
import { SqliteJsonValue } from '@powersync/service-sync-rules';
import * as crypto from 'node:crypto';
import * as timers from 'timers/promises';
import { describe, expect, test } from 'vitest';
import { describeWithStorage } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

describe('chunked snapshots', () => {
  describeWithStorage({ timeout: 120_000 }, defineBatchTests);
});

function defineBatchTests(factory: TestStorageFactory) {
  // We need to test every supported type, since chunking could be quite sensitive to
  // how each specific type is handled.
  test('chunked snapshot edge case (int2)', async () => {
    await testChunkedSnapshot({
      idType: 'int2',
      genId: 'i',
      lastId: '2000',
      moveTo: '0',
      moveToJs: 0
    });
  });

  test('chunked snapshot edge case (int4)', async () => {
    await testChunkedSnapshot({
      idType: 'int4',
      genId: 'i',
      lastId: '2000',
      moveTo: '0',
      moveToJs: 0
    });
  });

  test('chunked snapshot edge case (int8)', async () => {
    await testChunkedSnapshot({
      idType: 'int8',
      genId: 'i',
      lastId: '2000',
      moveTo: '0',
      moveToJs: 0
    });
  });

  test('chunked snapshot edge case (text)', async () => {
    await testChunkedSnapshot({
      idType: 'text',
      genId: `to_char(i, 'fm0000')`,
      lastId: `'2000'`,
      moveTo: `'0000'`,
      moveToJs: '0000'
    });
  });

  test('chunked snapshot edge case (varchar)', async () => {
    await testChunkedSnapshot({
      idType: 'varchar',
      genId: `to_char(i, 'fm0000')`,
      lastId: `'2000'`,
      moveTo: `'0000'`,
      moveToJs: '0000'
    });
  });

  test('chunked snapshot edge case (uuid)', async () => {
    await testChunkedSnapshot({
      idType: 'uuid',
      // Generate a uuid by using the first part of a uuid and appending a 4-digit number.
      genId: `('00000000-0000-4000-8000-00000000' || to_char(i, 'fm0000')) :: uuid`,
      lastId: `'00000000-0000-4000-8000-000000002000'`,
      moveTo: `'00000000-0000-4000-8000-000000000000'`,
      moveToJs: '00000000-0000-4000-8000-000000000000'
    });
  });

  async function testChunkedSnapshot(options: {
    idType: string;
    genId: string;
    lastId: string;
    moveTo: string;
    moveToJs: SqliteJsonValue;
  }) {
    // 1. Start with 2k rows, one row with id = 2000, and a large TOAST value in another column.
    // 2. Replicate one batch of rows (id < 2000).
    // 3. `UPDATE table SET id = 0 WHERE id = 2000`
    // 4. Replicate the rest of the table.
    // 5. Logical replication picks up the UPDATE above, but it is missing the TOAST column.
    // 6. We end up with a row that has a missing TOAST column.

    await using context = await WalStreamTestContext.open(factory, {
      // We need to use a smaller chunk size here, so that we can run a query in between chunks
      walStreamOptions: { snapshotChunkLength: 100 }
    });

    await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data`);
    const { pool } = context;

    await pool.query(`CREATE TABLE test_data(id ${options.idType} primary key, description text)`);

    // 1. Start with 2k rows, one row with id = 2000...
    await pool.query({
      statement: `INSERT INTO test_data(id, description) SELECT ${options.genId}, 'foo' FROM generate_series(1, 2000) i`
    });

    // ...and a large TOAST value in another column.
    // Toast value, must be > 8kb after compression
    const largeDescription = crypto.randomBytes(20_000).toString('hex');
    await pool.query({
      statement: `UPDATE test_data SET description = $1 WHERE id = ${options.lastId} :: ${options.idType}`,
      params: [{ type: 'varchar', value: largeDescription }]
    });

    // 2. Replicate one batch of rows (id < 100).
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

    // 3. `UPDATE table SET id = 0 WHERE id = 2000`
    const rs = await pool.query(
      `UPDATE test_data SET id = ${options.moveTo} WHERE id = ${options.lastId} RETURNING id`
    );
    expect(rs.rows.length).toEqual(1);

    // 4. Replicate the rest of the table.
    await p;

    // 5. Logical replication picks up the UPDATE above, but it is missing the TOAST column.
    context.startStreaming();

    // 6. If all went well, the "resnapshot" process would take care of this.
    const data = await context.getBucketData('global[]', undefined, {});
    const reduced = reduceBucket(data);

    const movedRow = reduced.find((row) => row.object_id == String(options.moveToJs));
    expect(movedRow?.data).toEqual(JSON.stringify({ id: options.moveToJs, description: largeDescription }));

    expect(reduced.length).toEqual(2001);
  }
}
