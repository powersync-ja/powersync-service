import { compareIds, putOp, removeOp, test_utils } from '@powersync/service-core-tests';
import { describe, expect, test } from 'vitest';

import { storage } from '@powersync/service-core';
import { describeWithStorage } from './util.js';
import { BinlogStreamTestContext } from './BinlogStreamUtils.js';

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

    await connectionManager.query(`DROP TABLE test_data`);
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);
    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t3','test3')`);

    const data = await context.getBucketData('global[]');

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

  test('Add a table that is in the sync rules', async () => {
    await using context = await BinlogStreamTestContext.open(factory);
    const { connectionManager } = context;
    await context.updateSyncRules(BASIC_SYNC_RULES);

    await context.replicateSnapshot();
    await context.startStreaming();

    // Add table after initial replication
    await connectionManager.query(`CREATE TABLE test_data (id CHAR(36) PRIMARY KEY, description TEXT)`);

    await connectionManager.query(`INSERT INTO test_data(id, description) VALUES('t1','test1')`);

    const data = await context.getBucketData('global[]');

    expect(data).toMatchObject([PUT_T1, PUT_T1]);
  });

  test('(1) Rename a table not in the sync rules to one in the sync rules', async () => {
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

    expect(data).toMatchObject([
      // Snapshot insert
      PUT_T1,
      PUT_T2,
      // Replicated insert
      PUT_T2
    ]);
  });

  test('(2) Rename a table in the sync rules to another table also in the sync rules', async () => {
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

  test('(3) Rename table in the sync rules to one not in the sync rules', async () => {
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

    expect(data).toMatchObject([
      // Initial replication
      PUT_T1,
      // Truncate
      REMOVE_T1
    ]);
  });

  test('(1) Change Replication Identity default by dropping the primary key', async () => {
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

  test('(2) Change Replication Identity full by adding a column', async () => {
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

  test('(3) Change Replication Identity default by modifying primary key column type', async () => {
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

  test('(4) Change Replication Identity by changing the type of a column in a compound unique index', async () => {
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
}
