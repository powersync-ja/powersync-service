import { compareIds, putOp, removeOp } from '@core-tests/stream_utils.js';
import { connectMongo } from '@core-tests/util.js';
import { BucketStorageFactory, MongoBucketStorage } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';
import { walStreamTest } from './wal_stream_utils.js';

type StorageFactory = () => Promise<BucketStorageFactory>;

export const INITIALIZED_MONGO_STORAGE_FACTORY: StorageFactory = async () => {
  const db = await connectMongo();
  // None of the PG tests insert data into this collection, so it was never created
  await db.db.createCollection('bucket_parameters');
  await db.clear();

  return new MongoBucketStorage(db, { slot_name_prefix: 'test_' });
};

describe(
  'schema changes',
  function () {
    defineTests(INITIALIZED_MONGO_STORAGE_FACTORY);
  },
  { timeout: 20_000 }
);

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, * FROM "test_data"
`;

const PUT_T1 = putOp('test_data', { id: 't1', description: 'test1' });
const PUT_T2 = putOp('test_data', { id: 't2', description: 'test2' });
const PUT_T3 = putOp('test_data', { id: 't3', description: 'test3' });

const REMOVE_T1 = removeOp('test_data', 't1');
const REMOVE_T2 = removeOp('test_data', 't2');

function defineTests(factory: StorageFactory) {
  test(
    're-create table',
    walStreamTest(factory, async (context) => {
      // Drop a table and re-create it.
      await context.updateSyncRules(BASIC_SYNC_RULES);
      const { pool } = context;

      await pool.query(`DROP TABLE IF EXISTS test_data`);
      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);

      await pool.query(
        { statement: `DROP TABLE test_data` },
        { statement: `CREATE TABLE test_data(id text primary key, description text)` },
        { statement: `INSERT INTO test_data(id, description) VALUES('t3', 'test3')` }
      );

      const data = await context.getBucketData('global[]');

      // Initial inserts
      expect(data.slice(0, 2)).toMatchObject([PUT_T1, PUT_T2]);

      // Truncate - order doesn't matter
      expect(data.slice(2, 4).sort(compareIds)).toMatchObject([REMOVE_T1, REMOVE_T2]);

      expect(data.slice(4)).toMatchObject([
        // Snapshot insert
        PUT_T3,
        // Replicated insert
        // We may eventually be able to de-duplicate this
        PUT_T3
      ]);
    })
  );

  test(
    'add table',
    walStreamTest(factory, async (context) => {
      // Add table after initial replication
      await context.updateSyncRules(BASIC_SYNC_RULES);
      const { pool } = context;

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        // Snapshot insert
        PUT_T1,
        // Replicated insert
        // We may eventually be able to de-duplicate this
        PUT_T1
      ]);
    })
  );

  test(
    'rename table (1)',
    walStreamTest(factory, async (context) => {
      const { pool } = context;

      await context.updateSyncRules(BASIC_SYNC_RULES);

      // Rename table not in sync rules -> in sync rules
      await pool.query(`CREATE TABLE test_data_old(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data_old(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(
        { statement: `ALTER TABLE test_data_old RENAME TO test_data` },
        // We need an operation to detect the change
        { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
      );

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2).sort(compareIds)).toMatchObject([
        // Snapshot insert
        PUT_T1,
        PUT_T2
      ]);
      expect(data.slice(2)).toMatchObject([
        // Replicated insert
        // We may eventually be able to de-duplicate this
        PUT_T2
      ]);
    })
  );

  test(
    'rename table (2)',
    walStreamTest(factory, async (context) => {
      // Rename table in sync rules -> in sync rules
      const { pool } = context;

      await context.updateSyncRules(`
    bucket_definitions:
      global:
        data:
          - SELECT id, * FROM "test_data%"
    `);

      await pool.query(`CREATE TABLE test_data1(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data1(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(
        { statement: `ALTER TABLE test_data1 RENAME TO test_data2` },
        // We need an operation to detect the change
        { statement: `INSERT INTO test_data2(id, description) VALUES('t2', 'test2')` }
      );

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2)).toMatchObject([
        // Initial replication
        putOp('test_data1', { id: 't1', description: 'test1' }),
        // Initial truncate
        removeOp('test_data1', 't1')
      ]);

      expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
        // Snapshot insert
        putOp('test_data2', { id: 't1', description: 'test1' }),
        putOp('test_data2', { id: 't2', description: 'test2' })
      ]);
      expect(data.slice(4)).toMatchObject([
        // Replicated insert
        // We may eventually be able to de-duplicate this
        putOp('test_data2', { id: 't2', description: 'test2' })
      ]);
    })
  );

  test(
    'rename table (3)',
    walStreamTest(factory, async (context) => {
      // Rename table in sync rules -> not in sync rules

      const { pool } = context;

      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(
        { statement: `ALTER TABLE test_data RENAME TO test_data_na` },
        // We need an operation to detect the change
        { statement: `INSERT INTO test_data_na(id, description) VALUES('t2', 'test2')` }
      );

      const data = await context.getBucketData('global[]');

      expect(data).toMatchObject([
        // Initial replication
        PUT_T1,
        // Truncate
        REMOVE_T1
      ]);
    })
  );

  test(
    'change replica id',
    walStreamTest(factory, async (context) => {
      // Change replica id from default to full
      // Causes a re-import of the table.

      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(
        { statement: `ALTER TABLE test_data REPLICA IDENTITY FULL` },
        // We need an operation to detect the change
        { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
      );

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2)).toMatchObject([
        // Initial inserts
        PUT_T1,
        // Truncate
        REMOVE_T1
      ]);

      // Snapshot - order doesn't matter
      expect(data.slice(2, 4).sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

      expect(data.slice(4).sort(compareIds)).toMatchObject([
        // Replicated insert
        // We may eventually be able to de-duplicate this
        PUT_T2
      ]);
    })
  );

  test(
    'change full replica id by adding column',
    walStreamTest(factory, async (context) => {
      // Change replica id from full by adding column
      // Causes a re-import of the table.
      // Other changes such as renaming column would have the same effect

      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`ALTER TABLE test_data REPLICA IDENTITY FULL`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(
        { statement: `ALTER TABLE test_data ADD COLUMN other TEXT` },
        { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
      );

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2)).toMatchObject([
        // Initial inserts
        PUT_T1,
        // Truncate
        REMOVE_T1
      ]);

      // Snapshot - order doesn't matter
      expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
        putOp('test_data', { id: 't1', description: 'test1', other: null }),
        putOp('test_data', { id: 't2', description: 'test2', other: null })
      ]);

      expect(data.slice(4).sort(compareIds)).toMatchObject([
        // Replicated insert
        // We may eventually be able to de-duplicate this
        putOp('test_data', { id: 't2', description: 'test2', other: null })
      ]);
    })
  );

  test(
    'change default replica id by changing column type',
    walStreamTest(factory, async (context) => {
      // Change default replica id by changing column type
      // Causes a re-import of the table.
      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(
        { statement: `ALTER TABLE test_data ALTER COLUMN id TYPE varchar` },
        { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
      );

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2)).toMatchObject([
        // Initial inserts
        PUT_T1,
        // Truncate
        REMOVE_T1
      ]);

      // Snapshot - order doesn't matter
      expect(data.slice(2, 4).sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

      expect(data.slice(4).sort(compareIds)).toMatchObject([
        // Replicated insert
        // We may eventually be able to de-duplicate this
        PUT_T2
      ]);
    })
  );

  test(
    'change index id by changing column type',
    walStreamTest(factory, async (context) => {
      // Change index replica id by changing column type
      // Causes a re-import of the table.
      // Secondary functionality tested here is that replica id column order stays
      // the same between initial and incremental replication.
      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text primary key, description text not null)`);
      await pool.query(`CREATE UNIQUE INDEX i1 ON test_data(description, id)`);
      await pool.query(`ALTER TABLE test_data REPLICA IDENTITY USING INDEX i1`);

      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);

      await pool.query(
        { statement: `ALTER TABLE test_data ALTER COLUMN description TYPE varchar` },
        { statement: `INSERT INTO test_data(id, description) VALUES('t3', 'test3')` }
      );

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2)).toMatchObject([
        // Initial snapshot
        PUT_T1,
        // Streamed
        PUT_T2
      ]);

      expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
        // Truncate - any order
        REMOVE_T1,
        REMOVE_T2
      ]);

      // Snapshot - order doesn't matter
      expect(data.slice(4, 7).sort(compareIds)).toMatchObject([PUT_T1, PUT_T2, PUT_T3]);

      expect(data.slice(7).sort(compareIds)).toMatchObject([
        // Replicated insert
        // We may eventually be able to de-duplicate this
        PUT_T3
      ]);
    })
  );

  test(
    'add to publication',
    walStreamTest(factory, async (context) => {
      // Add table to publication after initial replication
      const { pool } = context;

      await pool.query(`DROP PUBLICATION powersync`);
      await pool.query(`CREATE TABLE test_foo(id text primary key, description text)`);
      await pool.query(`CREATE PUBLICATION powersync FOR table test_foo`);

      const storage = await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);

      await pool.query(`ALTER PUBLICATION powersync ADD TABLE test_data`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t3', 'test3')`);

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 3).sort(compareIds)).toMatchObject([
        // Snapshot insert - any order
        PUT_T1,
        PUT_T2,
        PUT_T3
      ]);

      expect(data.slice(3)).toMatchObject([
        // Replicated insert
        // We may eventually be able to de-duplicate this
        PUT_T3
      ]);

      const metrics = await storage.factory.getStorageMetrics();
      expect(metrics.replication_size_bytes).toBeGreaterThan(0);
    })
  );

  test(
    'add to publication (not in sync rules)',
    walStreamTest(factory, async (context) => {
      // Add table to publication after initial replication
      // Since the table is not in sync rules, it should not be replicated.
      const { pool } = context;

      await pool.query(`DROP PUBLICATION powersync`);
      await pool.query(`CREATE TABLE test_foo(id text primary key, description text)`);
      await pool.query(`CREATE PUBLICATION powersync FOR table test_foo`);

      const storage = await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_other(id text primary key, description text)`);
      await pool.query(`INSERT INTO test_other(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(`INSERT INTO test_other(id, description) VALUES('t2', 'test2')`);

      await pool.query(`ALTER PUBLICATION powersync ADD TABLE test_other`);
      await pool.query(`INSERT INTO test_other(id, description) VALUES('t3', 'test3')`);

      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([]);

      const metrics = await storage.factory.getStorageMetrics();
      expect(metrics.replication_size_bytes).toEqual(0);
    })
  );

  test(
    'replica identity nothing',
    walStreamTest(factory, async (context) => {
      // Technically not a schema change, but fits here.
      // Replica ID works a little differently here - the table doesn't have
      // one defined, but we generate a unique one for each replicated row.

      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text primary key, description text)`);
      await pool.query(`ALTER TABLE test_data REPLICA IDENTITY NOTHING`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);

      // Just as an FYI - cannot update or delete here
      expect(pool.query(`UPDATE test_data SET description = 'test2b' WHERE id = 't2'`)).rejects.toThrow(
        'does not have a replica identity and publishes updates'
      );

      // Testing TRUNCATE is important here - this depends on current_data having unique
      // ids.
      await pool.query(`TRUNCATE TABLE test_data`);

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2)).toMatchObject([
        // Initial inserts
        PUT_T1,
        PUT_T2
      ]);

      expect(data.slice(2).sort(compareIds)).toMatchObject([
        // Truncate
        REMOVE_T1,
        REMOVE_T2
      ]);
    })
  );

  test(
    'replica identity default without PK',
    walStreamTest(factory, async (context) => {
      // Same as no replica identity
      const { pool } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await pool.query(`CREATE TABLE test_data(id text, description text)`);
      await pool.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);

      await context.replicateSnapshot();
      context.startStreaming();

      await pool.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);

      // Just as an FYI - cannot update or delete here
      expect(pool.query(`UPDATE test_data SET description = 'test2b' WHERE id = 't2'`)).rejects.toThrow(
        'does not have a replica identity and publishes updates'
      );

      // Testing TRUNCATE is important here - this depends on current_data having unique
      // ids.
      await pool.query(`TRUNCATE TABLE test_data`);

      const data = await context.getBucketData('global[]');

      expect(data.slice(0, 2)).toMatchObject([
        // Initial inserts
        PUT_T1,
        PUT_T2
      ]);

      expect(data.slice(2).sort(compareIds)).toMatchObject([
        // Truncate
        REMOVE_T1,
        REMOVE_T2
      ]);
    })
  );
}
