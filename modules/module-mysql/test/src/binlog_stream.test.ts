import { putOp, removeOp } from '@core-tests/stream_utils.js';
import { MONGO_STORAGE_FACTORY } from '@core-tests/util.js';
import { BucketStorageFactory, Metrics } from '@powersync/service-core';
import * as crypto from 'crypto';
import { describe, expect, test } from 'vitest';
import { binlogStreamTest } from './binlog_stream_utils.js';
import { logger } from '@powersync/lib-services-framework';

type StorageFactory = () => Promise<BucketStorageFactory>;

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

describe(
  ' Binlog stream - mongodb',
  function () {
    defineBinlogStreamTests(MONGO_STORAGE_FACTORY);
  },
  { timeout: 20_000 }
);

function defineBinlogStreamTests(factory: StorageFactory) {
  test(
    'Replicate basic values',
    binlogStreamTest(factory, async (context) => {
      const { connectionManager } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description, num FROM "test_data"`);

      await connectionManager.query(
        `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description TEXT, num BIGINT)`
      );

      await context.replicateSnapshot();

      const startRowCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const startTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      context.startStreaming();
      await connectionManager.query(`INSERT INTO test_data(description, num) VALUES('test1', 1152921504606846976)`);
      const [[result]] = await connectionManager.query(
        `SELECT id AS test_id FROM test_data WHERE description = 'test1' AND num = 1152921504606846976`
      );
      const testId = result.test_id;
      logger.info('Finished Inserting data with id:' + testId);

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1', num: 1152921504606846976n })]);
      const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const endTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;
      expect(endRowCount - startRowCount).toEqual(1);
      expect(endTxCount - startTxCount).toEqual(1);
    })
  );

  // test(
  //   'replicating case sensitive table',
  //   binlogStreamTest(factory, async (context) => {
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(`
  //     bucket_definitions:
  //       global:
  //         data:
  //           - SELECT id, description FROM "test_DATA"
  //     `);

  //     await connectionManager.query(
  //       `CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description text)`
  //     );

  //     await context.replicateSnapshot();

  //     const startRowCount =
  //       (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
  //     const startTxCount =
  //       (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

  //     context.startStreaming();

  //     await connectionManager.query(`INSERT INTO test_DATA(description) VALUES('test1')`);
  //     const [[result]] = await connectionManager.query(
  //       `SELECT id AS test_id FROM test_data WHERE description = 'test1'`
  //     );
  //     const testId = result.test_id;

  //     const data = await context.getBucketData('global[]');

  //     expect(data).toMatchObject([putOp('test_DATA', { id: testId, description: 'test1' })]);
  //     const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
  //     const endTxCount =
  //       (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;
  //     expect(endRowCount - startRowCount).toEqual(1);
  //     expect(endTxCount - startTxCount).toEqual(1);
  //   })
  // );

  // //   TODO: Not supported yet
  // //   test(
  // //     'replicating TRUNCATE',
  // //     binlogStreamTest(factory, async (context) => {
  // //       const { connectionManager } = context;
  // //       const syncRuleContent = `
  // // bucket_definitions:
  // //   global:
  // //     data:
  // //       - SELECT id, description FROM "test_data"
  // //   by_test_data:
  // //     parameters: SELECT id FROM test_data WHERE id = token_parameters.user_id
  // //     data: []
  // // `;
  // //       await context.updateSyncRules(syncRuleContent);
  // //       await connectionManager.query(`DROP TABLE IF EXISTS test_data`);
  // //       await connectionManager.query(
  // //         `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`
  // //       );
  // //
  // //       await context.replicateSnapshot();
  // //       context.startStreaming();
  // //
  // //       const [{ test_id }] = pgwireRows(
  // //         await connectionManager.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
  // //       );
  // //       await connectionManager.query(`TRUNCATE test_data`);
  // //
  // //       const data = await context.getBucketData('global[]');
  // //
  // //       expect(data).toMatchObject([
  // //         putOp('test_data', { id: test_id, description: 'test1' }),
  // //         removeOp('test_data', test_id)
  // //       ]);
  // //     })
  // //   );

  // test(
  //   'replicating changing primary key',
  //   binlogStreamTest(factory, async (context) => {
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     await connectionManager.query(
  //       `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description text)`
  //     );

  //     await context.replicateSnapshot();
  //     context.startStreaming();

  //     await connectionManager.query(`INSERT INTO test_data(description) VALUES('test1')`);
  //     const [[result1]] = await connectionManager.query(
  //       `SELECT id AS test_id FROM test_data WHERE description = 'test1'`
  //     );
  //     const testId1 = result1.test_id;

  //     await connectionManager.query(`UPDATE test_data SET id = UUID(), description = 'test2a' WHERE id = '${testId1}'`);
  //     const [[result2]] = await connectionManager.query(
  //       `SELECT id AS test_id FROM test_data WHERE description = 'test2a'`
  //     );
  //     const testId2 = result2.test_id;

  //     // This update may fail replicating with:
  //     // Error: Update on missing record public.test_data:074a601e-fc78-4c33-a15d-f89fdd4af31d :: {"g":1,"t":"651e9fbe9fec6155895057ec","k":"1a0b34da-fb8c-5e6f-8421-d7a3c5d4df4f"}
  //     await connectionManager.query(`UPDATE test_data SET description = 'test2b' WHERE id = '${testId2}'`);

  //     // Re-use old id again
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('${testId1}', 'test1b')`);
  //     await connectionManager.query(`UPDATE test_data SET description = 'test1c' WHERE id = '${testId1}'`);

  //     const data = await context.getBucketData('global[]');
  //     expect(data).toMatchObject([
  //       // Initial insert
  //       putOp('test_data', { id: testId1, description: 'test1' }),
  //       // Update id, then description
  //       removeOp('test_data', testId1),
  //       putOp('test_data', { id: testId2, description: 'test2a' }),
  //       putOp('test_data', { id: testId2, description: 'test2b' }),
  //       // Re-use old id
  //       putOp('test_data', { id: testId1, description: 'test1b' }),
  //       putOp('test_data', { id: testId1, description: 'test1c' })
  //     ]);
  //   })
  // );

  // test(
  //   'initial sync',
  //   binlogStreamTest(factory, async (context) => {
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     await connectionManager.query(
  //       `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description text)`
  //     );

  //     await connectionManager.query(`INSERT INTO test_data(description) VALUES('test1')`);
  //     const [[result]] = await connectionManager.query(
  //       `SELECT id AS test_id FROM test_data WHERE description = 'test1'`
  //     );
  //     const testId = result.test_id;

  //     await context.replicateSnapshot();
  //     context.startStreaming();

  //     const data = await context.getBucketData('global[]');
  //     expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1' })]);
  //   })
  // );

  // // test(
  // //   'record too large',
  // //   binlogStreamTest(factory, async (context) => {
  // //     await context.updateSyncRules(`bucket_definitions:
  // //     global:
  // //       data:
  // //         - SELECT id, description, other FROM "test_data"`);
  // //     const { connectionManager } = context;
  // //
  // //     await connectionManager.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);
  // //
  // //     await context.replicateSnapshot();
  // //
  // //     // 4MB
  // //     const largeDescription = crypto.randomBytes(2_000_000).toString('hex');
  // //     // 18MB
  // //     const tooLargeDescription = crypto.randomBytes(9_000_000).toString('hex');
  // //
  // //     await connectionManager.query({
  // //       statement: `INSERT INTO test_data(id, description, other) VALUES('t1', $1, 'foo')`,
  // //       params: [{ type: 'varchar', value: tooLargeDescription }]
  // //     });
  // //     await connectionManager.query({
  // //       statement: `UPDATE test_data SET description = $1 WHERE id = 't1'`,
  // //       params: [{ type: 'varchar', value: largeDescription }]
  // //     });
  // //
  // //     context.startStreaming();
  // //
  // //     const data = await context.getBucketData('global[]');
  // //     expect(data.length).toEqual(1);
  // //     const row = JSON.parse(data[0].data as string);
  // //     delete row.description;
  // //     expect(row).toEqual({ id: 't1', other: 'foo' });
  // //     delete data[0].data;
  // //     expect(data[0]).toMatchObject({ object_id: 't1', object_type: 'test_data', op: 'PUT', op_id: '1' });
  // //   })
  // // );

  // test(
  //   'table not in sync rules',
  //   binlogStreamTest(factory, async (context) => {
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);

  //     await connectionManager.query(
  //       `CREATE TABLE test_donotsync (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description text)`
  //     );

  //     await context.replicateSnapshot();

  //     const startRowCount =
  //       (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
  //     const startTxCount =
  //       (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

  //     context.startStreaming();

  //     await connectionManager.query(`INSERT INTO test_donotsync(description) VALUES('test1')`);
  //     const data = await context.getBucketData('global[]');

  //     expect(data).toMatchObject([]);
  //     const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
  //     const endTxCount =
  //       (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

  //     // There was a transaction, but we should not replicate any actual data
  //     expect(endRowCount - startRowCount).toEqual(0);
  //     expect(endTxCount - startTxCount).toEqual(1);
  //   })
  // );
}
