import { MissingReplicationSlotError } from '@module/replication/WalStream.js';
import { storage } from '@powersync/service-core';
import { METRICS_HELPER, putOp, removeOp } from '@powersync/service-core-tests';
import { pgwireRows } from '@powersync/service-jpgwire';
import { JSONBig } from '@powersync/service-jsonbig';
import { ReplicationMetric } from '@powersync/service-types';
import * as crypto from 'crypto';
import { describe, expect, test } from 'vitest';
import { describeWithStorage } from './util.js';
import { WalStreamTestContext, withMaxWalSize } from './wal_stream_utils.js';

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;

describe('wal stream', () => {
  describeWithStorage({ timeout: 20_000 }, defineWalStreamTests);
});

function defineWalStreamTests(config: storage.TestStorageConfig) {
  const { factory } = config;

  test('replicating basic values', async () => {
    await using context = await WalStreamTestContext.open(factory);
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

    await context.initializeReplication();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    const [{ test_id }] = pgwireRows(
      await pool.query(
        `INSERT INTO test_data(description, num) VALUES('test1', 1152921504606846976) returning id as test_id`
      )
    );

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_data', { id: test_id, description: 'test1', num: 1152921504606846976n })]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(1);
    // In some rare cases there may be additional empty transactions, so we allow for that.
    expect(endTxCount - startTxCount).toBeGreaterThanOrEqual(1);
  });

  test('replicating case sensitive table', async () => {
    await using context = await WalStreamTestContext.open(factory);
    const { pool } = context;
    await context.updateSyncRules(`
      bucket_definitions:
        global:
          data:
            - SELECT id, description FROM "test_DATA"
      `);

    await pool.query(`DROP TABLE IF EXISTS "test_DATA"`);
    await pool.query(`CREATE TABLE "test_DATA"(id uuid primary key default uuid_generate_v4(), description text)`);

    await context.initializeReplication();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    const [{ test_id }] = pgwireRows(
      await pool.query(`INSERT INTO "test_DATA"(description) VALUES('test1') returning id as test_id`)
    );

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([putOp('test_DATA', { id: test_id, description: 'test1' })]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;
    expect(endRowCount - startRowCount).toEqual(1);
    expect(endTxCount - startTxCount).toBeGreaterThanOrEqual(1);
  });

  test('replicating TOAST values', async () => {
    await using context = await WalStreamTestContext.open(factory);
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
      putOp('test_data', { id: test_id, name: 'test1', description: largeDescription })
    ]);
    expect(data.slice(1)).toMatchObject([
      putOp('test_data', { id: test_id, name: 'test2', description: largeDescription })
    ]);
  });

  test('replicating TRUNCATE', async () => {
    await using context = await WalStreamTestContext.open(factory);
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

    await context.initializeReplication();

    const [{ test_id }] = pgwireRows(
      await pool.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
    );
    await pool.query(`TRUNCATE test_data`);

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([
      putOp('test_data', { id: test_id, description: 'test1' }),
      removeOp('test_data', test_id)
    ]);
  });

  test('replicating changing primary key', async () => {
    await using context = await WalStreamTestContext.open(factory);
    const { pool } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);
    await pool.query(`DROP TABLE IF EXISTS test_data`);
    await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

    await context.initializeReplication();

    const [{ test_id }] = pgwireRows(
      await pool.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
    );

    const [{ test_id: test_id2 }] = pgwireRows(
      await pool.query(
        `UPDATE test_data SET id = uuid_generate_v4(), description = 'test2a' WHERE id = '${test_id}' returning id as test_id`
      )
    );

    // Since we don't have an old copy of the record with the new primary key, this
    // may trigger a "resnapshot".
    await pool.query(`UPDATE test_data SET description = 'test2b' WHERE id = '${test_id2}'`);

    // Re-use old id again
    await pool.query(`INSERT INTO test_data(id, description) VALUES('${test_id}', 'test1b')`);
    await pool.query(`UPDATE test_data SET description = 'test1c' WHERE id = '${test_id}'`);

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([
      // Initial insert
      putOp('test_data', { id: test_id, description: 'test1' }),
      // Update id, then description
      removeOp('test_data', test_id),
      putOp('test_data', { id: test_id2, description: 'test2a' }),
      putOp('test_data', { id: test_id2, description: 'test2b' }),
      // Re-use old id
      putOp('test_data', { id: test_id, description: 'test1b' }),
      putOp('test_data', { id: test_id, description: 'test1c' })
    ]);
  });

  test('initial sync', async () => {
    await using context = await WalStreamTestContext.open(factory);
    const { pool } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await pool.query(`DROP TABLE IF EXISTS test_data`);
    await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

    const [{ test_id }] = pgwireRows(
      await pool.query(`INSERT INTO test_data(description) VALUES('test1') returning id as test_id`)
    );

    await context.replicateSnapshot();

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', { id: test_id, description: 'test1' })]);
  });

  test('record too large', async () => {
    await using context = await WalStreamTestContext.open(factory);
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

    const data = await context.getBucketData('global[]');
    expect(data.length).toEqual(1);
    const row = JSON.parse(data[0].data as string);
    delete row.description;
    expect(row).toEqual({ id: 't1', other: 'foo' });
    delete data[0].data;
    expect(data[0]).toMatchObject({ object_id: 't1', object_type: 'test_data', op: 'PUT', op_id: '1' });
  });

  test('table not in sync rules', async () => {
    await using context = await WalStreamTestContext.open(factory);
    const { pool } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await pool.query(`CREATE TABLE test_donotsync(id uuid primary key default uuid_generate_v4(), description text)`);

    await context.initializeReplication();

    const startRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const startTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    await pool.query(`INSERT INTO test_donotsync(description) VALUES('test1') returning id as test_id`);

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([]);
    const endRowCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.ROWS_REPLICATED)) ?? 0;
    const endTxCount = (await METRICS_HELPER.getMetricValueForTests(ReplicationMetric.TRANSACTIONS_REPLICATED)) ?? 0;

    // There was a transaction, but we should not replicate any actual data
    expect(endRowCount - startRowCount).toEqual(0);
    expect(endTxCount - startTxCount).toBeGreaterThanOrEqual(1);
  });

  test('reporting slot issues', async () => {
    {
      await using context = await WalStreamTestContext.open(factory);
      const { pool } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"`);

      await pool.query(
        `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text, num int8)`
      );
      await pool.query(
        `INSERT INTO test_data(id, description) VALUES('8133cd37-903b-4937-a022-7c8294015a3a', 'test1') returning id as test_id`
      );
      await context.replicateSnapshot();

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        putOp('test_data', {
          id: '8133cd37-903b-4937-a022-7c8294015a3a',
          description: 'test1'
        })
      ]);

      expect(await context.storage!.getStatus()).toMatchObject({ active: true, snapshot_done: true });
    }

    {
      await using context = await WalStreamTestContext.open(factory, { doNotClear: true });
      const { pool } = context;
      await pool.query('DROP PUBLICATION powersync');
      await pool.query(`UPDATE test_data SET description = 'updated'`);
      await pool.query('CREATE PUBLICATION powersync FOR ALL TABLES');

      const serverVersion = await context.connectionManager.getServerVersion();

      await context.loadActiveSyncRules();

      // Note: The actual error may be thrown either in replicateSnapshot(), or in getCheckpoint().

      if (serverVersion!.compareMain('18.0.0') >= 0) {
        // No error expected in Postres 18. Replication keeps on working depite the
        // publication being re-created.
        await context.replicateSnapshot();
        await context.getCheckpoint();
      } else {
        // await context.getCheckpoint();
        // Postgres < 18 invalidates the replication slot when the publication is re-created.
        // In the service, this error is handled in WalStreamReplicationJob,
        // creating a new replication slot.
        await expect(async () => {
          await context.replicateSnapshot();
          await context.getCheckpoint();
        }).rejects.toThrowError(MissingReplicationSlotError);
      }
    }
  });

  test('dropped replication slot', async () => {
    {
      await using context = await WalStreamTestContext.open(factory);
      const { pool } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"`);

      await pool.query(
        `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text, num int8)`
      );
      await pool.query(
        `INSERT INTO test_data(id, description) VALUES('8133cd37-903b-4937-a022-7c8294015a3a', 'test1') returning id as test_id`
      );
      await context.replicateSnapshot();

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        putOp('test_data', {
          id: '8133cd37-903b-4937-a022-7c8294015a3a',
          description: 'test1'
        })
      ]);

      expect(await context.storage!.getStatus()).toMatchObject({ active: true, snapshot_done: true });
    }

    {
      await using context = await WalStreamTestContext.open(factory, { doNotClear: true });
      const { pool } = context;
      const storage = await context.factory.getActiveStorage();

      // Here we explicitly drop the replication slot, which should always be handled.
      await pool.query({
        statement: `SELECT pg_drop_replication_slot($1)`,
        params: [{ type: 'varchar', value: storage?.slot_name! }]
      });

      await context.loadActiveSyncRules();

      // The error is handled on a higher level, which triggers
      // creating a new replication slot.
      await expect(async () => {
        await context.replicateSnapshot();
      }).rejects.toThrowError(MissingReplicationSlotError);
    }
  });

  test('replication slot lost', async () => {
    await using baseContext = await WalStreamTestContext.open(factory, { doNotClear: true });

    const serverVersion = await baseContext.connectionManager.getServerVersion();
    if (serverVersion!.compareMain('13.0.0') < 0) {
      console.warn(`max_slot_wal_keep_size not supported on postgres ${serverVersion} - skipping test.`);
      return;
    }

    // Configure max_slot_wal_keep_size for the test, reverting afterwards.
    await using s = await withMaxWalSize(baseContext.pool, '100MB');

    {
      await using context = await WalStreamTestContext.open(factory);
      const { pool } = context;
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"`);

      await pool.query(
        `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text, num int8)`
      );
      await pool.query(
        `INSERT INTO test_data(id, description) VALUES('8133cd37-903b-4937-a022-7c8294015a3a', 'test1') returning id as test_id`
      );
      await context.replicateSnapshot();

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        putOp('test_data', {
          id: '8133cd37-903b-4937-a022-7c8294015a3a',
          description: 'test1'
        })
      ]);

      expect(await context.storage!.getStatus()).toMatchObject({ active: true, snapshot_done: true });
    }

    {
      await using context = await WalStreamTestContext.open(factory, { doNotClear: true });
      const { pool } = context;
      const storage = await context.factory.getActiveStorage();
      const slotName = storage?.slot_name!;

      // Here, we write data to the WAL until the replication slot is lost.
      const TRIES = 100;
      for (let i = 0; i < TRIES; i++) {
        // Write something to the WAL.
        await pool.query(`select pg_logical_emit_message(true, 'test', 'x')`);
        // Switch WAL file. With default settings, each WAL file is around 16MB.
        await pool.query(`select pg_switch_wal()`);
        // Checkpoint command forces the old WAL files to be archived/removed.
        await pool.query(`checkpoint`);
        // Now check if the slot is still active.
        const slot = pgwireRows(
          await context.pool.query({
            statement: `select slot_name, wal_status, safe_wal_size, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as lag from pg_replication_slots where slot_name = $1`,
            params: [{ type: 'varchar', value: slotName }]
          })
        )[0];
        if (slot.wal_status == 'lost') {
          break;
        } else if (i == TRIES - 1) {
          throw new Error(
            `Could not generate test conditions to expire replication slot. Current status: ${JSONBig.stringify(slot)}`
          );
        }
      }

      await context.loadActiveSyncRules();

      // The error is handled on a higher level, which triggers
      // creating a new replication slot.
      await expect(async () => {
        await context.replicateSnapshot();
      }).rejects.toThrowError(MissingReplicationSlotError);
    }
  });

  test('old date format', async () => {
    await using context = await WalStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { pool } = context;
    await pool.query(`DROP TABLE IF EXISTS test_data`);
    await pool.query(`CREATE TABLE test_data(id text primary key, description timestamptz);`);

    await context.initializeReplication();
    await pool.query(`INSERT INTO test_data(id, description) VALUES ('t1', '2025-09-10 15:17:14+02')`);

    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_data', { id: 't1', description: '2025-09-10 13:17:14Z' })]);
  });

  test('new date format', async () => {
    await using context = await WalStreamTestContext.open(factory);
    await context.updateSyncRules(`
streams:
  stream:
    query: SELECT id, * FROM "test_data"

config:
  edition: 2
`);
    const { pool } = context;
    await pool.query(`DROP TABLE IF EXISTS test_data`);
    await pool.query(`CREATE TABLE test_data(id text primary key, description timestamptz);`);

    await context.initializeReplication();
    await pool.query(`INSERT INTO test_data(id, description) VALUES ('t1', '2025-09-10 15:17:14+02')`);

    const data = await context.getBucketData('stream|0[]');
    expect(data).toMatchObject([putOp('test_data', { id: 't1', description: '2025-09-10T13:17:14.000000Z' })]);
  });

  test('custom types', async () => {
    await using context = await WalStreamTestContext.open(factory);

    await context.updateSyncRules(`
streams:
  stream:
    query: SELECT id, * FROM "test_data"

config:
  edition: 2
`);

    const { pool } = context;
    await pool.query(`DROP TABLE IF EXISTS test_data`);
    await pool.query(`CREATE TYPE composite AS (foo bool, bar int4);`);
    await pool.query(`CREATE TABLE test_data(id text primary key, description composite, ts timestamptz);`);

    // Covered by initial replication
    await pool.query(
      `INSERT INTO test_data(id, description, ts) VALUES ('t1', ROW(TRUE, 1)::composite, '2025-11-17T09:11:00Z')`
    );

    await context.initializeReplication();
    // Covered by streaming replication
    await pool.query(
      `INSERT INTO test_data(id, description, ts) VALUES ('t2', ROW(TRUE, 2)::composite, '2025-11-17T09:12:00Z')`
    );

    const data = await context.getBucketData('stream|0[]');
    expect(data).toMatchObject([
      putOp('test_data', { id: 't1', description: '{"foo":1,"bar":1}', ts: '2025-11-17T09:11:00.000000Z' }),
      putOp('test_data', { id: 't2', description: '{"foo":1,"bar":2}', ts: '2025-11-17T09:12:00.000000Z' })
    ]);
  });

  test('custom types in primary key', async () => {
    await using context = await WalStreamTestContext.open(factory);

    await context.updateSyncRules(`
streams:
  stream:
    query: SELECT id, * FROM "test_data"

config:
  edition: 2
`);

    const { pool } = context;
    await pool.query(`DROP TABLE IF EXISTS test_data`);
    await pool.query(`CREATE DOMAIN test_id AS TEXT;`);
    await pool.query(`CREATE TABLE test_data(id test_id primary key);`);

    await context.initializeReplication();
    await pool.query(`INSERT INTO test_data(id) VALUES ('t1')`);

    const data = await context.getBucketData('stream|0[]');
    expect(data).toMatchObject([putOp('test_data', { id: 't1' })]);
  });

  test('replica identity handling', async () => {
    // This specifically test a case of timestamps being used as part of the replica identity.
    // There was a regression in versions 1.15.0-1.15.5, which this tests for.
    await using context = await WalStreamTestContext.open(factory);
    const { pool } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await pool.query(`DROP TABLE IF EXISTS test_data`);
    await pool.query(`CREATE TABLE test_data(id uuid primary key, description text, ts timestamptz)`);
    await pool.query(`ALTER TABLE test_data REPLICA IDENTITY FULL`);

    const test_id = `a9798b07-84de-4297-9a8e-aafb4dd0282f`;

    await pool.query(
      `INSERT INTO test_data(id, description, ts) VALUES('${test_id}', 'test1', '2025-01-01T00:00:00Z') returning id as test_id`
    );

    await context.replicateSnapshot();

    await pool.query(`UPDATE test_data SET description = 'test2' WHERE id = '${test_id}'`);

    const data = await context.getBucketData('global[]');
    // For replica identity full, each change changes the id, making it a REMOVE+PUT
    expect(data).toMatchObject([
      // Initial insert
      putOp('test_data', { id: test_id, description: 'test1' }),
      // Update
      removeOp('test_data', test_id),
      putOp('test_data', { id: test_id, description: 'test2' })
    ]);

    // subkey contains `${table id}/${replica identity}`.
    // table id changes from run to run, but replica identity should always stay constant.
    // This should not change if we make changes to the implementation
    // (unless specifically opting in to new behavior)
    expect(data[0].subkey).toContain('/c7b3f1a3-ec4d-5d44-b295-c7f2a32bb056');
    expect(data[1].subkey).toContain('/c7b3f1a3-ec4d-5d44-b295-c7f2a32bb056');
    expect(data[2].subkey).toContain('/984d457a-69f0-559a-a2f9-a511c28b968d');
  });
}
