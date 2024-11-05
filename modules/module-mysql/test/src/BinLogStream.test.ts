import { putOp, removeOp } from '@core-tests/stream_utils.js';
import { MONGO_STORAGE_FACTORY } from '@core-tests/util.js';
import { BucketStorageFactory, Metrics } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';
import { binlogStreamTest } from './BinlogStreamUtils.js';

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
      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1', num: 1152921504606846976n })]);
      const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const endTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;
      expect(endRowCount - startRowCount).toEqual(1);
      expect(endTxCount - startTxCount).toEqual(1);
    })
  );

  test(
    'Replicate case sensitive table',
    binlogStreamTest(factory, async (context) => {
      const { connectionManager } = context;
      await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT id, description FROM "test_DATA"
      `);

      await connectionManager.query(
        `CREATE TABLE test_DATA (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description TEXT)`
      );

      await context.replicateSnapshot();

      const startRowCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const startTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      context.startStreaming();

      await connectionManager.query(`INSERT INTO test_DATA(description) VALUES('test1')`);
      const [[result]] = await connectionManager.query(
        `SELECT id AS test_id FROM test_DATA WHERE description = 'test1'`
      );
      const testId = result.test_id;

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([putOp('test_DATA', { id: testId, description: 'test1' })]);
      const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const endTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;
      expect(endRowCount - startRowCount).toEqual(1);
      expect(endTxCount - startTxCount).toEqual(1);
    })
  );

  //   TODO: Not supported yet
  //   test(
  //     'replicating TRUNCATE',
  //     binlogStreamTest(factory, async (context) => {
  //       const { connectionManager } = context;
  //       const syncRuleContent = `
  // bucket_definitions:
  //   global:
  //     data:
  //       - SELECT id, description FROM "test_data"
  //   by_test_data:
  //     parameters: SELECT id FROM test_data WHERE id = token_parameters.user_id
  //     data: []
  // `;
  //       await context.updateSyncRules(syncRuleContent);
  //       await connectionManager.query(`DROP TABLE IF EXISTS test_data`);
  //       await connectionManager.query(
  //         `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description TEXT)`
  //       );
  //
  //       await context.replicateSnapshot();
  //       context.startStreaming();
  //
  //       const [{ test_id }] = pgwireRows(
  //         await connectionManager.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
  //       );
  //       await connectionManager.query(`TRUNCATE test_data`);
  //
  //       const data = await context.getBucketData('global[]');
  //
  //       expect(data).toMatchObject([
  //         putOp('test_data', { id: test_id, description: 'test1' }),
  //         removeOp('test_data', test_id)
  //       ]);
  //     })
  //   );

  test(
    'Replicate row with changed primary key',
    binlogStreamTest(factory, async (context) => {
      const { connectionManager } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await connectionManager.query(
        `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description TEXT)`
      );

      await context.replicateSnapshot();
      context.startStreaming();

      await connectionManager.query(`INSERT INTO test_data(description) VALUES('test1')`);
      const [[result1]] = await connectionManager.query(
        `SELECT id AS test_id FROM test_data WHERE description = 'test1'`
      );
      const testId1 = result1.test_id;

      await connectionManager.query(`UPDATE test_data SET id = UUID(), description = 'test2a' WHERE id = '${testId1}'`);
      const [[result2]] = await connectionManager.query(
        `SELECT id AS test_id FROM test_data WHERE description = 'test2a'`
      );
      const testId2 = result2.test_id;

      // This update may fail replicating with:
      // Error: Update on missing record public.test_data:074a601e-fc78-4c33-a15d-f89fdd4af31d :: {"g":1,"t":"651e9fbe9fec6155895057ec","k":"1a0b34da-fb8c-5e6f-8421-d7a3c5d4df4f"}
      await connectionManager.query(`UPDATE test_data SET description = 'test2b' WHERE id = '${testId2}'`);

      // Re-use old id again
      await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('${testId1}', 'test1b')`);
      await connectionManager.query(`UPDATE test_data SET description = 'test1c' WHERE id = '${testId1}'`);

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([
        // Initial insert
        putOp('test_data', { id: testId1, description: 'test1' }),
        // Update id, then description
        removeOp('test_data', testId1),
        putOp('test_data', { id: testId2, description: 'test2a' }),
        putOp('test_data', { id: testId2, description: 'test2b' }),
        // Re-use old id
        putOp('test_data', { id: testId1, description: 'test1b' }),
        putOp('test_data', { id: testId1, description: 'test1c' })
      ]);
    })
  );

  test(
    'Initial sync / snapshot',
    binlogStreamTest(factory, async (context) => {
      const { connectionManager } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await connectionManager.query(
        `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description TEXT)`
      );

      await connectionManager.query(`INSERT INTO test_data(description) VALUES('test1')`);
      const [[result]] = await connectionManager.query(
        `SELECT id AS test_id FROM test_data WHERE description = 'test1'`
      );
      const testId = result.test_id;

      await context.replicateSnapshot();

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([putOp('test_data', { id: testId, description: 'test1' })]);
    })
  );

  test(
    'Snapshot with date values',
    binlogStreamTest(factory, async (context) => {
      const { connectionManager } = context;
      await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT * FROM "test_data"
      `);

      await connectionManager.query(
        `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description TEXT, date DATE, datetime DATETIME, timestamp TIMESTAMP)`
      );

      await connectionManager.query(`
        INSERT INTO test_data(description, date, datetime, timestamp) VALUES('testDates', '2023-03-06', '2023-03-06 15:47', '2023-03-06 15:47')
      `);
      const [[result]] = await connectionManager.query(
        `SELECT id AS test_id FROM test_data WHERE description = 'testDates'`
      );
      const testId = result.test_id;

      await context.replicateSnapshot();

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([
        putOp('test_data', {
          id: testId,
          description: 'testDates',
          date: `2023-03-06`,
          datetime: '2023-03-06T15:47:00.000Z',
          timestamp: '2023-03-06T15:47:00.000Z'
        })
      ]);
    })
  );

  test(
    'Replicate data values',
    binlogStreamTest(factory, async (context) => {
      const { connectionManager } = context;
      await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT * FROM "test_data"
      `);

      await connectionManager.query(
        `CREATE TABLE test_data (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description TEXT, date DATE, datetime DATETIME, timestamp TIMESTAMP)`
      );

      await context.replicateSnapshot();

      const startRowCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const startTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      context.startStreaming();

      await connectionManager.query(`
        INSERT INTO test_data(description, date, datetime, timestamp) VALUES('testDates', '2023-03-06', '2023-03-06 15:47', '2023-03-06 15:47')
      `);
      const [[result]] = await connectionManager.query(
        `SELECT id AS test_id FROM test_data WHERE description = 'testDates'`
      );
      const testId = result.test_id;

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([
        putOp('test_data', {
          id: testId,
          description: 'testDates',
          date: `2023-03-06`,
          datetime: '2023-03-06T15:47:00.000Z',
          timestamp: '2023-03-06T15:47:00.000Z'
        })
      ]);
      const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const endTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;
      expect(endRowCount - startRowCount).toEqual(1);
      expect(endTxCount - startTxCount).toEqual(1);
    })
  );

  test(
    'Ignore replication for tables not in sync rules',
    binlogStreamTest(factory, async (context) => {
      const { connectionManager } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await connectionManager.query(
        `CREATE TABLE test_donotsync (id CHAR(36) PRIMARY KEY DEFAULT (UUID()), description TEXT)`
      );

      await context.replicateSnapshot();

      const startRowCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const startTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      context.startStreaming();

      await connectionManager.query(`INSERT INTO test_donotsync(description) VALUES('test1')`);
      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([]);
      const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const endTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      // There was a transaction, but we should not replicate any actual data
      expect(endRowCount - startRowCount).toEqual(0);
      expect(endTxCount - startTxCount).toEqual(1);
    })
  );
}
