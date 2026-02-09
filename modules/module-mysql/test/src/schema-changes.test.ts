import { compareIds, putOp, removeOp, test_utils } from '@powersync/service-core-tests';
import { beforeAll, describe, expect, test } from 'vitest';

import { storage } from '@powersync/service-core';
import { createTestDb, describeWithStorage, TEST_CONNECTION_OPTIONS } from './util.js';
import { BinlogStreamTestContext } from './BinlogStreamUtils.js';
import timers from 'timers/promises';
import { MySQLConnectionManager } from '@module/replication/MySQLConnectionManager.js';
import { getMySQLVersion, qualifiedMySQLTable, satisfiesVersion } from '@module/utils/mysql-utils.js';

describe('MySQL Schema Changes', () => {
  describeWithStorage({ timeout: 20_000 }, defineTests);
});

const BASIC_SYNC_RULES = `
bucket_definitions:
  global:
    data:
      - SELECT id, * FROM "test_data"
`;

const PUT_T1 = test_utils.putOp('test_data', { id: 't1', description: 'test1' });
const PUT_T2 = test_utils.putOp('test_data', { id: 't2', description: 'test2' });
const PUT_T3 = test_utils.putOp('test_data', { id: 't3', description: 'test3' });

const REMOVE_T1 = test_utils.removeOp('test_data', 't1');
const REMOVE_T2 = test_utils.removeOp('test_data', 't2');

function defineTests(factory: storage.TestStorageFactory) {
  let isMySQL57: boolean = false;

  beforeAll(async () => {
    const connectionManager = new MySQLConnectionManager(TEST_CONNECTION_OPTIONS, {});
    const connection = await connectionManager.getConnection();
    const version = await getMySQLVersion(connection);
    isMySQL57 = satisfiesVersion(version, '5.7.x');
    connection.release();
    await connectionManager.end();
  });

  test('Re-create table', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Drop a table and re-create it.
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);

    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    // Dropping the table immediately leads to a rare race condition where Zongji tries to get the table information
    // for the previous write event, but the table is already gone. Without the table info the tablemap event can't be correctly
    // populated and replication will fail.
    await timers.setTimeout(50);
    await connectionManager.query(`DROP TABLE test_data`);
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t3','test3')`);

    const data = await context.getBucketData('global[]');

    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T3]);

    // Initial inserts
    expect(data.slice(0, 2)).toMatchObject([PUT_T1, PUT_T2]);

    // Truncate - order doesn't matter
    expect(data.slice(2, 4).sort(compareIds)).toMatchObject([REMOVE_T1, REMOVE_T2]);

    // Due to the async nature of this replication test,
    // the insert for t3 is picked up both in the snapshot and in the replication stream.
    expect(data.slice(4)).toMatchObject([
      PUT_T3, // Snapshot insert
      PUT_T3 // Insert from binlog replication stream
    ]);
  });

  test('Create table: New table in is in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();
    await context.startStreaming();

    // Add table after initial replication
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);

    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    const data = await context.getBucketData('global[]');

    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1]);

    expect(data).toMatchObject([PUT_T1, PUT_T1]);
  });

  test('Create table: New table is created from existing data', async () => {
    // Create table with select from is not allowed in MySQL 5.7 when enforce_gtid_consistency=ON
    if (!isMySQL57) {
      await using context = await BinlogStreamTestContext.open(factory);
      const { connectionManager } = context;
      await context.updateSyncRules(BASIC_SYNC_RULES);

      await connectionManager.query(`CREATE TABLE test_data_from
                                     (
                                       id          CHAR(36) PRIMARY KEY,
                                       description TEXT
                                     )`);
      await connectionManager.query(`INSERT INTO test_data_from(id, description)
                                     VALUES ('t1', 'test1')`);
      await connectionManager.query(`INSERT INTO test_data_from(id, description)
                                     VALUES ('t2', 'test2')`);
      await connectionManager.query(`INSERT INTO test_data_from(id, description)
                                     VALUES ('t3', 'test3')`);

      await context.replicateSnapshot();
      await context.startStreaming();

      // Add table after initial replication
      await connectionManager.query(`CREATE TABLE test_data SELECT * FROM test_data_from`);

      const data = await context.getBucketData('global[]');

      const reduced = test_utils.reduceBucket(data).slice(1);
      expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2, PUT_T3]);

      // Interestingly, the create with select triggers binlog row write events
      expect(data).toMatchObject([
        // From snapshot
        PUT_T1,
        PUT_T2,
        PUT_T3,
        // From replication stream
        PUT_T1,
        PUT_T2,
        PUT_T3
      ]);
    }
  });

  test('Create table: New table is not in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();
    await context.startStreaming();

    // Add table after initial replication
    await connectionManager.query(`CREATE TABLE test_data_ignored (id CHAR(36) PRIMARY KEY, description TEXT)`);

    await connectionManager.query(`INSERT INTO test_data_ignored(id, description) VALUES('t1','test ignored')`);

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([]);
  });

  test('Rename table: Table not in the sync rules to one in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    // Rename table not that is not in sync rules -> in sync rules
    await connectionManager.query(`CREATE TABLE test_data_old (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data_old(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`RENAME TABLE test_data_old TO test_data`);

    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');

    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

    expect(data).toMatchObject([
      // Snapshot insert
      PUT_T1,
      PUT_T2,
      // Replicated insert
      PUT_T2
    ]);
  });

  test('Rename table: Table in the sync rules to another table in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);

    await context.updateSyncRules(`
    bucket_definitions:
      global:
        data:
          - SELECT id, * FROM "test_data%"
    `);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data1 (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data1(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`RENAME TABLE test_data1 TO test_data2`);
    await connectionManager.query(`INSERT INTO test_data2(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');

    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([
      putOp('test_data2', { id: 't1', description: 'test1' }),
      putOp('test_data2', { id: 't2', description: 'test2' })
    ]);

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
  });

  test('Rename table: Table in the sync rules to not in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`RENAME TABLE test_data TO test_data_not_in_sync_rules`);
    await connectionManager.query(`INSERT INTO test_data_not_in_sync_rules(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');

    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([]);

    expect(data).toMatchObject([
      // Initial replication
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);
  });

  test('Change Replication Identity default to full by dropping the primary key', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Change replica id from default (PK) to full
    // Requires re-snapshotting the table.

    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`ALTER TABLE test_data DROP PRIMARY KEY;`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');

    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);

    expect(data.slice(2)).toMatchObject([
      // Snapshot inserts
      PUT_T1,
      PUT_T2,
      // Replicated insert
      PUT_T2
    ]);
  });

  test('Change Replication Identity full by adding a column', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Change replica id from full by adding column
    // Causes a re-import of the table.
    // Other changes such as renaming column would have the same effect

    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    // No primary key, no unique column, so full replication identity will be used
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36), description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`ALTER TABLE test_data ADD COLUMN new_column TEXT`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');

    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([
      putOp('test_data', { id: 't1', description: 'test1', new_column: null }),
      putOp('test_data', { id: 't2', description: 'test2', new_column: null })
    ]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);

    // Snapshot - order doesn't matter
    expect(data.slice(2)).toMatchObject([
      // Snapshot inserts
      putOp('test_data', { id: 't1', description: 'test1', new_column: null }),
      putOp('test_data', { id: 't2', description: 'test2', new_column: null }),
      // Replicated insert
      putOp('test_data', { id: 't2', description: 'test2', new_column: null })
    ]);
  });

  test('Change Replication Identity from full to index by adding a unique constraint', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Change replica id full by adding a unique index that can serve as the replication id

    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    // No primary key, no unique column, so full replication identity will be used
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36), description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`ALTER TABLE test_data ADD UNIQUE (id)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);

    // Snapshot - order doesn't matter
    expect(data.slice(2)).toMatchObject([
      // Snapshot inserts
      PUT_T1,
      PUT_T2,
      // Replicated insert
      PUT_T2
    ]);
  });

  test('Change Replication Identity from full to index by adding a unique index', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Change replica id full by adding a unique index that can serve as the replication id
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    // No primary key, no unique column, so full replication identity will be used
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36), description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`CREATE UNIQUE INDEX id_idx ON test_data (id)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);

    // Snapshot - order doesn't matter
    expect(data.slice(2)).toMatchObject([
      // Snapshot inserts
      PUT_T1,
      PUT_T2,
      // Replicated insert
      PUT_T2
    ]);
  });

  test('Change Replication Identity from index by dropping the unique constraint', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Change replica id full by adding a unique index that can serve as the replication id

    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    // Unique constraint on id
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36), description TEXT, UNIQUE (id))`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`ALTER TABLE test_data DROP INDEX id`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);

    // Snapshot - order doesn't matter
    expect(data.slice(2)).toMatchObject([
      // Snapshot inserts
      PUT_T1,
      PUT_T2,
      // Replicated insert
      PUT_T2
    ]);
  });

  test('Change Replication Identity default by modifying primary key column type', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`ALTER TABLE test_data MODIFY COLUMN id VARCHAR(36)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);

    expect(data.slice(2)).toMatchObject([
      // Snapshot inserts
      PUT_T1,
      PUT_T2,
      // Replicated insert
      PUT_T2
    ]);
  });

  test('Change Replication Identity by changing the type of a column in a compound unique index', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Change index replica id by changing column type
    // Causes a re-import of the table.
    // Secondary functionality tested here is that replica id column order stays
    // the same between initial and incremental replication.
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36), description CHAR(100))`);
    await connectionManager.query(`ALTER TABLE test_data ADD INDEX (id, description)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    await connectionManager.query(`ALTER TABLE test_data MODIFY COLUMN id VARCHAR(36)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t3','test3')`);

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2, PUT_T3]);

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
      PUT_T3
    ]);
  });

  test('Add column: New non replication identity column does not trigger re-sync', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Added column not in replication identity so it should not cause a re-import

    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`ALTER TABLE test_data ADD COLUMN new_column TEXT`);
    await connectionManager.query(
      `INSERT INTO test_data(id, description, new_column) VALUES('t2','test2', 'new_data')`
    );

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([
      PUT_T1,
      putOp('test_data', { id: 't2', description: 'test2', new_column: 'new_data' })
    ]);

    expect(data.slice(0, 1)).toMatchObject([PUT_T1]);

    expect(data.slice(1)).toMatchObject([
      putOp('test_data', { id: 't2', description: 'test2', new_column: 'new_data' })
    ]);
  });

  test('Modify non replication identity column', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Changing the type of a column that is not part of the replication identity does not cause a re-sync of the table.
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    await connectionManager.query(`ALTER TABLE test_data MODIFY COLUMN description VARCHAR(100)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t3','test3')`);

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([PUT_T1, PUT_T2, PUT_T3]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial snapshot
      PUT_T1,
      // Streamed
      PUT_T2
    ]);

    expect(data.slice(2)).toMatchObject([
      // Replicated insert
      PUT_T3
    ]);
  });

  test('Drop a table in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Technically not a schema change, but fits here.
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36), description CHAR(100))`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t2','test2')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`DROP TABLE test_data`);

    const data = await context.getBucketData('global[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      PUT_T2
    ]);

    expect(data.slice(2).sort(compareIds)).toMatchObject([
      // Drop
      REMOVE_T1,
      REMOVE_T2
    ]);
  });

  test('Schema changes for tables in other schemas in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    // Technically not a schema change, but fits here.
    await context.updateSyncRules(`
  bucket_definitions:
    multi_schema_test_data:
      data:
        - SELECT id, description, num FROM "multi_schema"."test_data"
        `);

    const { connectionManager } = context;
    await createTestDb(connectionManager, 'multi_schema');
    const testTable = qualifiedMySQLTable('test_data', 'multi_schema');
    await connectionManager.query(
      `CREATE TABLE IF NOT EXISTS ${testTable} (id CHAR(36) PRIMARY KEY,description TEXT);`
    );
    await connectionManager.query(`INSERT INTO ${testTable}(id, description) VALUES('t1','test1')`);
    await connectionManager.query(`INSERT INTO ${testTable}(id, description) VALUES('t2','test2')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`DROP TABLE ${testTable}`);

    const data = await context.getBucketData('multi_schema_test_data[]');
    const reduced = test_utils.reduceBucket(data).slice(1);
    expect(reduced.sort(compareIds)).toMatchObject([]);

    expect(data.slice(0, 2)).toMatchObject([
      // Initial inserts
      PUT_T1,
      PUT_T2
    ]);

    expect(data.slice(2).sort(compareIds)).toMatchObject([
      // Drop
      REMOVE_T1,
      REMOVE_T2
    ]);
  });

  test('Changes for tables in schemas not in the sync rules are ignored', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    await context.updateSyncRules(BASIC_SYNC_RULES);

    const { connectionManager } = context;
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36), description CHAR(100))`);

    await createTestDb(connectionManager, 'multi_schema');
    const testTable = qualifiedMySQLTable('test_data_ignored', 'multi_schema');
    await connectionManager.query(
      `CREATE TABLE IF NOT EXISTS ${testTable} (id CHAR(36) PRIMARY KEY,description TEXT);`
    );
    await connectionManager.query(`INSERT INTO ${testTable}(id, description) VALUES('t1','test1')`);
    await connectionManager.query(`INSERT INTO ${testTable}(id, description) VALUES('t2','test2')`);

    await context.replicateSnapshot();
    await context.startStreaming();

    await connectionManager.query(`INSERT INTO ${testTable}(id, description) VALUES('t3','test3')`);
    await connectionManager.query(`DROP TABLE ${testTable}`);

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([]);
  });
}
