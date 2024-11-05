import { compareIds, putOp, removeOp } from '@core-tests/stream_utils.js';
import { describe, expect, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY, StorageFactory } from './util.js';
import { binlogStreamTest } from './BinlogStreamUtils.js';

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
  // TODO: Not supported in Zongji yet
  // test(
  //   'Re-create table',
  //   binlogStreamTest(factory, async (context) => {
  //     // Drop a table and re-create it.
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //     const { connectionManager } = context;
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);
  //     console.log('Inserted streaming row1');
  //
  //     await connectionManager.query(`DROP TABLE test_data`);
  //     console.log('Dropped table');
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     console.log('Recreating table');
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t3', 'test3')`);
  //     console.log('Inserted streaming row2');
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     // Initial inserts
  //     expect(data.slice(0, 2)).toMatchObject([PUT_T1, PUT_T2]);
  //
  //     // Truncate - order doesn't matter
  //     expect(data.slice(2, 4).sort(compareIds)).toMatchObject([REMOVE_T1, REMOVE_T2]);
  //
  //     expect(data.slice(4)).toMatchObject([
  //       // Snapshot insert
  //       PUT_T3,
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       PUT_T3
  //     ]);
  //   })
  // );
  // test(
  //   'Add table',
  //   binlogStreamTest(factory, async (context) => {
  //     // Add table after initial replication
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //     const { connectionManager } = context;
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //     while (!context.binlogStream.isReady) {
  //       await new Promise((resolve) => setTimeout(resolve, 1000));
  //     }
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     console.log('Created table');
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //     console.log('Inserted streaming row');
  //
  //     await new Promise((resolve) => setTimeout(resolve, 1000));
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data).toMatchObject([
  //       // Snapshot insert
  //       PUT_T1,
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       PUT_T1
  //     ]);
  //   })
  // );
  // test(
  //   'rename table (1)',
  //   binlogStreamTest(factory, async (context) => {
  //     const { connectionManager } = context;
  //
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     // Rename table not in sync rules -> in sync rules
  //     await connectionManager.query(`CREATE TABLE test_data_old(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data_old(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(
  //       { statement: `ALTER TABLE test_data_old RENAME TO test_data` },
  //       // We need an operation to detect the change
  //       { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
  //     );
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2).sort(compareIds)).toMatchObject([
  //       // Snapshot insert
  //       PUT_T1,
  //       PUT_T2
  //     ]);
  //     expect(data.slice(2)).toMatchObject([
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       PUT_T2
  //     ]);
  //   })
  // );
  //
  // test(
  //   'rename table (2)',
  //   binlogStreamTest(factory, async (context) => {
  //     // Rename table in sync rules -> in sync rules
  //     const { connectionManager } = context;
  //
  //     await context.updateSyncRules(`
  //   bucket_definitions:
  //     global:
  //       data:
  //         - SELECT id, * FROM "test_data%"
  //   `);
  //
  //     await connectionManager.query(`CREATE TABLE test_data1(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data1(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(
  //       { statement: `ALTER TABLE test_data1 RENAME TO test_data2` },
  //       // We need an operation to detect the change
  //       { statement: `INSERT INTO test_data2(id, description) VALUES('t2', 'test2')` }
  //     );
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial replication
  //       putOp('test_data1', { id: 't1', description: 'test1' }),
  //       // Initial truncate
  //       removeOp('test_data1', 't1')
  //     ]);
  //
  //     expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
  //       // Snapshot insert
  //       putOp('test_data2', { id: 't1', description: 'test1' }),
  //       putOp('test_data2', { id: 't2', description: 'test2' })
  //     ]);
  //     expect(data.slice(4)).toMatchObject([
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       putOp('test_data2', { id: 't2', description: 'test2' })
  //     ]);
  //   })
  // );
  //
  // test(
  //   'rename table (3)',
  //   binlogStreamTest(factory, async (context) => {
  //     // Rename table in sync rules -> not in sync rules
  //
  //     const { connectionManager } = context;
  //
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(
  //       { statement: `ALTER TABLE test_data RENAME TO test_data_na` },
  //       // We need an operation to detect the change
  //       { statement: `INSERT INTO test_data_na(id, description) VALUES('t2', 'test2')` }
  //     );
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data).toMatchObject([
  //       // Initial replication
  //       PUT_T1,
  //       // Truncate
  //       REMOVE_T1
  //     ]);
  //   })
  // );
  //
  // test(
  //   'change replica id',
  //   binlogStreamTest(factory, async (context) => {
  //     // Change replica id from default to full
  //     // Causes a re-import of the table.
  //
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(
  //       { statement: `ALTER TABLE test_data REPLICA IDENTITY FULL` },
  //       // We need an operation to detect the change
  //       { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
  //     );
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial inserts
  //       PUT_T1,
  //       // Truncate
  //       REMOVE_T1
  //     ]);
  //
  //     // Snapshot - order doesn't matter
  //     expect(data.slice(2, 4).sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);
  //
  //     expect(data.slice(4).sort(compareIds)).toMatchObject([
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       PUT_T2
  //     ]);
  //   })
  // );
  //
  // test(
  //   'change full replica id by adding column',
  //   binlogStreamTest(factory, async (context) => {
  //     // Change replica id from full by adding column
  //     // Causes a re-import of the table.
  //     // Other changes such as renaming column would have the same effect
  //
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`ALTER TABLE test_data REPLICA IDENTITY FULL`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(
  //       { statement: `ALTER TABLE test_data ADD COLUMN other TEXT` },
  //       { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
  //     );
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial inserts
  //       PUT_T1,
  //       // Truncate
  //       REMOVE_T1
  //     ]);
  //
  //     // Snapshot - order doesn't matter
  //     expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
  //       putOp('test_data', { id: 't1', description: 'test1', other: null }),
  //       putOp('test_data', { id: 't2', description: 'test2', other: null })
  //     ]);
  //
  //     expect(data.slice(4).sort(compareIds)).toMatchObject([
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       putOp('test_data', { id: 't2', description: 'test2', other: null })
  //     ]);
  //   })
  // );
  //
  // test(
  //   'change default replica id by changing column type',
  //   binlogStreamTest(factory, async (context) => {
  //     // Change default replica id by changing column type
  //     // Causes a re-import of the table.
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(
  //       { statement: `ALTER TABLE test_data ALTER COLUMN id TYPE varchar` },
  //       { statement: `INSERT INTO test_data(id, description) VALUES('t2', 'test2')` }
  //     );
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial inserts
  //       PUT_T1,
  //       // Truncate
  //       REMOVE_T1
  //     ]);
  //
  //     // Snapshot - order doesn't matter
  //     expect(data.slice(2, 4).sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);
  //
  //     expect(data.slice(4).sort(compareIds)).toMatchObject([
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       PUT_T2
  //     ]);
  //   })
  // );
  //
  // test(
  //   'change index id by changing column type',
  //   binlogStreamTest(factory, async (context) => {
  //     // Change index replica id by changing column type
  //     // Causes a re-import of the table.
  //     // Secondary functionality tested here is that replica id column order stays
  //     // the same between initial and incremental replication.
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT not null)`);
  //     await connectionManager.query(`CREATE UNIQUE INDEX i1 ON test_data(description, id)`);
  //     await connectionManager.query(`ALTER TABLE test_data REPLICA IDENTITY USING INDEX i1`);
  //
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);
  //
  //     await connectionManager.query(
  //       { statement: `ALTER TABLE test_data ALTER COLUMN description TYPE varchar` },
  //       { statement: `INSERT INTO test_data(id, description) VALUES('t3', 'test3')` }
  //     );
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial snapshot
  //       PUT_T1,
  //       // Streamed
  //       PUT_T2
  //     ]);
  //
  //     expect(data.slice(2, 4).sort(compareIds)).toMatchObject([
  //       // Truncate - any order
  //       REMOVE_T1,
  //       REMOVE_T2
  //     ]);
  //
  //     // Snapshot - order doesn't matter
  //     expect(data.slice(4, 7).sort(compareIds)).toMatchObject([PUT_T1, PUT_T2, PUT_T3]);
  //
  //     expect(data.slice(7).sort(compareIds)).toMatchObject([
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       PUT_T3
  //     ]);
  //   })
  // );
  //
  // test(
  //   'add to publication',
  //   binlogStreamTest(factory, async (context) => {
  //     // Add table to publication after initial replication
  //     const { connectionManager } = context;
  //
  //     await connectionManager.query(`DROP PUBLICATION powersync`);
  //     await connectionManager.query(`CREATE TABLE test_foo(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`CREATE PUBLICATION powersync FOR table test_foo`);
  //
  //     const storage = await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);
  //
  //     await connectionManager.query(`ALTER PUBLICATION powersync ADD TABLE test_data`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t3', 'test3')`);
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 3).sort(compareIds)).toMatchObject([
  //       // Snapshot insert - any order
  //       PUT_T1,
  //       PUT_T2,
  //       PUT_T3
  //     ]);
  //
  //     expect(data.slice(3)).toMatchObject([
  //       // Replicated insert
  //       // We may eventually be able to de-duplicate this
  //       PUT_T3
  //     ]);
  //
  //     const metrics = await storage.factory.getStorageMetrics();
  //     expect(metrics.replication_size_bytes).toBeGreaterThan(0);
  //   })
  // );
  //
  // test(
  //   'add to publication (not in sync rules)',
  //   binlogStreamTest(factory, async (context) => {
  //     // Add table to publication after initial replication
  //     // Since the table is not in sync rules, it should not be replicated.
  //     const { connectionManager } = context;
  //
  //     await connectionManager.query(`DROP PUBLICATION powersync`);
  //     await connectionManager.query(`CREATE TABLE test_foo(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`CREATE PUBLICATION powersync FOR table test_foo`);
  //
  //     const storage = await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_other(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_other(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(`INSERT INTO test_other(id, description) VALUES('t2', 'test2')`);
  //
  //     await connectionManager.query(`ALTER PUBLICATION powersync ADD TABLE test_other`);
  //     await connectionManager.query(`INSERT INTO test_other(id, description) VALUES('t3', 'test3')`);
  //
  //     const data = await context.getBucketData('global[]');
  //     expect(data).toMatchObject([]);
  //
  //     const metrics = await storage.factory.getStorageMetrics();
  //     expect(metrics.replication_size_bytes).toEqual(0);
  //   })
  // );
  //
  // test(
  //   'replica identity nothing',
  //   binlogStreamTest(factory, async (context) => {
  //     // Technically not a schema change, but fits here.
  //     // Replica ID works a little differently here - the table doesn't have
  //     // one defined, but we generate a unique one for each replicated row.
  //
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id CHAR(36) PRIMARY KEY, description TEXT)`);
  //     await connectionManager.query(`ALTER TABLE test_data REPLICA IDENTITY NOTHING`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);
  //
  //     // Just as an FYI - cannot update or delete here
  //     expect(connectionManager.query(`UPDATE test_data SET description = 'test2b' WHERE id = 't2'`)).rejects.toThrow(
  //       'does not have a replica identity and publishes updates'
  //     );
  //
  //     // Testing TRUNCATE is important here - this depends on current_data having unique
  //     // ids.
  //     await connectionManager.query(`TRUNCATE TABLE test_data`);
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial inserts
  //       PUT_T1,
  //       PUT_T2
  //     ]);
  //
  //     expect(data.slice(2).sort(compareIds)).toMatchObject([
  //       // Truncate
  //       REMOVE_T1,
  //       REMOVE_T2
  //     ]);
  //   })
  // );
  //
  // test(
  //   'replica identity default without PK',
  //   binlogStreamTest(factory, async (context) => {
  //     // Same as no replica identity
  //     const { connectionManager } = context;
  //     await context.updateSyncRules(BASIC_SYNC_RULES);
  //
  //     await connectionManager.query(`CREATE TABLE test_data(id TEXT, description TEXT)`);
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1', 'test1')`);
  //
  //     await context.replicateSnapshot();
  //     context.startStreaming();
  //
  //     await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2', 'test2')`);
  //
  //     // Just as an FYI - cannot update or delete here
  //     expect(connectionManager.query(`UPDATE test_data SET description = 'test2b' WHERE id = 't2'`)).rejects.toThrow(
  //       'does not have a replica identity and publishes updates'
  //     );
  //
  //     // Testing TRUNCATE is important here - this depends on current_data having unique
  //     // ids.
  //     await connectionManager.query(`TRUNCATE TABLE test_data`);
  //
  //     const data = await context.getBucketData('global[]');
  //
  //     expect(data.slice(0, 2)).toMatchObject([
  //       // Initial inserts
  //       PUT_T1,
  //       PUT_T2
  //     ]);
  //
  //     expect(data.slice(2).sort(compareIds)).toMatchObject([
  //       // Truncate
  //       REMOVE_T1,
  //       REMOVE_T2
  //     ]);
  //   })
  // );
}
