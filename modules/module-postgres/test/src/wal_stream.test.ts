import { BucketStorageFactory, Metrics } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { pgwireRows } from '@powersync/service-jpgwire';
import * as crypto from 'crypto';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { walStreamTest } from './wal_stream_utils.js';

type StorageFactory = () => Promise<BucketStorageFactory>;

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

describe(
  'wal stream - mongodb',
  function () {
    defineWalStreamTests(INITIALIZED_MONGO_STORAGE_FACTORY);
  },
  { timeout: 20_000 }
);

function defineWalStreamTests(factory: StorageFactory) {
  test(
    'replicating basic values',
    walStreamTest(factory, async (context) => {
      const { pool } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description, num FROM "test_data"`);

      await pool.query(`DROP TABLE IF EXISTS test_data`);
      await pool.query(
        `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text, num int8)`
      );

      await context.replicateSnapshot();

      const startRowCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const startTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      context.startStreaming();

      const [{ test_id }] = pgwireRows(
        await pool.query(
          `INSERT INTO test_data(description, num) VALUES('test1', 1152921504606846976) returning id as test_id`
        )
      );

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        test_utils.putOp('test_data', { id: test_id, description: 'test1', num: 1152921504606846976n })
      ]);
      const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const endTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;
      expect(endRowCount - startRowCount).toEqual(1);
      expect(endTxCount - startTxCount).toEqual(1);
    })
  );

  test(
    'replicating case sensitive table',
    walStreamTest(factory, async (context) => {
      const { pool } = context;
      await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT id, description FROM "test_DATA"
      `);

      await pool.query(`DROP TABLE IF EXISTS "test_DATA"`);
      await pool.query(`CREATE TABLE "test_DATA"(id uuid primary key default uuid_generate_v4(), description text)`);

      await context.replicateSnapshot();

      const startRowCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const startTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      context.startStreaming();

      const [{ test_id }] = pgwireRows(
        await pool.query(`INSERT INTO "test_DATA"(description) VALUES('test1') returning id as test_id`)
      );

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([test_utils.putOp('test_DATA', { id: test_id, description: 'test1' })]);
      const endRowCount = (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const endTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;
      expect(endRowCount - startRowCount).toEqual(1);
      expect(endTxCount - startTxCount).toEqual(1);
    })
  );

  test(
    'replicating TOAST values',
    walStreamTest(factory, async (context) => {
      const { pool } = context;
      await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT id, name, description FROM "test_data"
      `);

      await pool.query(`DROP TABLE IF EXISTS test_data`);
      await pool.query(
        `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), name text, description text)`
      );

      await context.replicateSnapshot();
      context.startStreaming();

      // Must be > 8kb after compression
      const largeDescription = crypto.randomBytes(20_000).toString('hex');
      const [{ test_id }] = pgwireRows(
        await pool.query({
          statement: `INSERT INTO test_data(name, description) VALUES('test1', $1) returning id as test_id`,
          params: [{ type: 'varchar', value: largeDescription }]
        })
      );

      await pool.query(`UPDATE test_data SET name = 'test2' WHERE id = '${test_id}'`);

      const data = await context.getBucketData('global[]');
      expect(data.slice(0, 1)).toMatchObject([
        test_utils.putOp('test_data', { id: test_id, name: 'test1', description: largeDescription })
      ]);
      expect(data.slice(1)).toMatchObject([
        test_utils.putOp('test_data', { id: test_id, name: 'test2', description: largeDescription })
      ]);
    })
  );

  test(
    'replicating TRUNCATE',
    walStreamTest(factory, async (context) => {
      const { pool } = context;
      const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
  by_test_data:
    parameters: SELECT id FROM test_data WHERE id = token_parameters.user_id
    data: []
`;
      await context.updateSyncRules(syncRuleContent);
      await pool.query(`DROP TABLE IF EXISTS test_data`);
      await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

      await context.replicateSnapshot();
      context.startStreaming();

      const [{ test_id }] = pgwireRows(
        await pool.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
      );
      await pool.query(`TRUNCATE test_data`);

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        test_utils.putOp('test_data', { id: test_id, description: 'test1' }),
        test_utils.removeOp('test_data', test_id)
      ]);
    })
  );

  test(
    'replicating changing primary key',
    walStreamTest(factory, async (context) => {
      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);
      await pool.query(`DROP TABLE IF EXISTS test_data`);
      await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

      await context.replicateSnapshot();
      context.startStreaming();

      const [{ test_id }] = pgwireRows(
        await pool.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
      );

      const [{ test_id: test_id2 }] = pgwireRows(
        await pool.query(
          `UPDATE test_data SET id = uuid_generate_v4(), description = 'test2a' WHERE id = '${test_id}' returning id as test_id`
        )
      );

      // This update may fail replicating with:
      // Error: Update on missing record public.test_data:074a601e-fc78-4c33-a15d-f89fdd4af31d :: {"g":1,"t":"651e9fbe9fec6155895057ec","k":"1a0b34da-fb8c-5e6f-8421-d7a3c5d4df4f"}
      await pool.query(`UPDATE test_data SET description = 'test2b' WHERE id = '${test_id2}'`);

      // Re-use old id again
      await pool.query(`INSERT INTO test_data(id, description) VALUES('${test_id}', 'test1b')`);
      await pool.query(`UPDATE test_data SET description = 'test1c' WHERE id = '${test_id}'`);

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([
        // Initial insert
        test_utils.putOp('test_data', { id: test_id, description: 'test1' }),
        // Update id, then description
        test_utils.removeOp('test_data', test_id),
        test_utils.putOp('test_data', { id: test_id2, description: 'test2a' }),
        test_utils.putOp('test_data', { id: test_id2, description: 'test2b' }),
        // Re-use old id
        test_utils.putOp('test_data', { id: test_id, description: 'test1b' }),
        test_utils.putOp('test_data', { id: test_id, description: 'test1c' })
      ]);
    })
  );

  test(
    'initial sync',
    walStreamTest(factory, async (context) => {
      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`DROP TABLE IF EXISTS test_data`);
      await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

      const [{ test_id }] = pgwireRows(
        await pool.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
      );

      await context.replicateSnapshot();
      context.startStreaming();

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([test_utils.putOp('test_data', { id: test_id, description: 'test1' })]);
    })
  );

  test(
    'record too large',
    walStreamTest(factory, async (context) => {
      await context.updateSyncRules(`bucket_definitions:
      global:
        data:
          - SELECT id, description, other FROM "test_data"`);
      const { pool } = context;

      await pool.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);

      await context.replicateSnapshot();

      // 4MB
      const largeDescription = crypto.randomBytes(2_000_000).toString('hex');
      // 18MB
      const tooLargeDescription = crypto.randomBytes(9_000_000).toString('hex');

      await pool.query({
        statement: `INSERT INTO test_data(id, description, other) VALUES('t1', $1, 'foo')`,
        params: [{ type: 'varchar', value: tooLargeDescription }]
      });
      await pool.query({
        statement: `UPDATE test_data SET description = $1 WHERE id = 't1'`,
        params: [{ type: 'varchar', value: largeDescription }]
      });

      context.startStreaming();

      const data = await context.getBucketData('global[]');
      expect(data.length).toEqual(1);
      const row = JSON.parse(data[0].data as string);
      delete row.description;
      expect(row).toEqual({ id: 't1', other: 'foo' });
      delete data[0].data;
      expect(data[0]).toMatchObject({ object_id: 't1', object_type: 'test_data', op: 'PUT', op_id: '1' });
    })
  );

  test(
    'table not in sync rules',
    walStreamTest(factory, async (context) => {
      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_donotsync(id uuid primary key default uuid_generate_v4(), description text)`);

      await context.replicateSnapshot();

      const startRowCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_rows_replicated_total')) ?? 0;
      const startTxCount =
        (await Metrics.getInstance().getMetricValueForTests('powersync_transactions_replicated_total')) ?? 0;

      context.startStreaming();

      const [{ test_id }] = pgwireRows(
        await pool.query(`INSERT INTO test_donotsync(description) VALUES('test1') returning id as test_id`)
      );

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
