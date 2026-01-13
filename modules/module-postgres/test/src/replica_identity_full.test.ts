import { afterEach, describe, expect, test } from 'vitest';
import { WalStreamTestContext } from './wal_stream_utils.js';
import * as storage from './storage.js';
import * as stream_utils from '@powersync/service-core-tests';
import { describeWithStorage } from './util.js';
import { pgwireRows } from '@powersync/lib-service-postgres';

describeWithStorage({ timeout: 20_000 }, function (factory: storage.TestStorageFactory) {
  describe('REPLICA IDENTITY FULL optimization', () => {
    let context: WalStreamTestContext;

    afterEach(async () => {
      if (context) {
        await context[Symbol.asyncDispose]();
      }
    });

    test('table with REPLICA IDENTITY FULL sets storeCurrentData=false', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      // Create table with REPLICA IDENTITY FULL
      await pool.query(`
        CREATE TABLE test_full (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          description TEXT
        )
      `);
      await pool.query(`ALTER TABLE test_full REPLICA IDENTITY FULL`);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT * FROM test_full
      `);

      await context.initializeReplication();

      // Get the resolved table
      const tables = await context.getResolvedTables();
      const testTable = tables.find((t) => t.name === 'test_full');

      expect(testTable).toBeDefined();
      expect(testTable!.storeCurrentData).toBe(false);
    });

    test('table with REPLICA IDENTITY DEFAULT sets storeCurrentData=true', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      // Create table with default replica identity
      await pool.query(`
        CREATE TABLE test_default (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          description TEXT
        )
      `);
      // DEFAULT is the default, no need to set explicitly

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT * FROM test_default
      `);

      await context.initializeReplication();

      const tables = await context.getResolvedTables();
      const testTable = tables.find((t) => t.name === 'test_default');

      expect(testTable).toBeDefined();
      expect(testTable!.storeCurrentData).toBe(true);
    });

    test('replication with REPLICA IDENTITY FULL table', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      await pool.query(`
        CREATE TABLE test_full (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          description TEXT,
          value INT
        )
      `);
      await pool.query(`ALTER TABLE test_full REPLICA IDENTITY FULL`);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, description, value FROM test_full
      `);

      await context.initializeReplication();

      // Insert data
      const [{ id: id1 }] = pgwireRows(
        await pool.query(`INSERT INTO test_full (description, value) VALUES ('test1', 100) RETURNING id`)
      );

      // Wait for replication
      await context.replicateSnapshot();

      // Verify data is synced
      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([
        stream_utils.putOp('test_full', {
          id: id1,
          description: 'test1',
          value: 100
        })
      ]);
    });

    test('UPDATE operations with REPLICA IDENTITY FULL', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      await pool.query(`
        CREATE TABLE test_full (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          description TEXT,
          counter INT DEFAULT 0
        )
      `);
      await pool.query(`ALTER TABLE test_full REPLICA IDENTITY FULL`);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, description, counter FROM test_full
      `);

      await context.initializeReplication();

      // Insert initial data
      const [{ id: id1 }] = pgwireRows(
        await pool.query(`INSERT INTO test_full (description, counter) VALUES ('initial', 0) RETURNING id`)
      );

      await context.replicateSnapshot();

      // Update the record
      await pool.query(`UPDATE test_full SET description = 'updated', counter = counter + 1 WHERE id = $1`, [id1]);

      await context.replicateSnapshot();

      // Verify updated data
      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([
        stream_utils.putOp('test_full', {
          id: id1,
          description: 'updated',
          counter: 1
        })
      ]);
    });

    test('DELETE operations with REPLICA IDENTITY FULL', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      await pool.query(`
        CREATE TABLE test_full (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          description TEXT
        )
      `);
      await pool.query(`ALTER TABLE test_full REPLICA IDENTITY FULL`);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, description FROM test_full
      `);

      await context.initializeReplication();

      // Insert and then delete
      const [{ id: id1 }] = pgwireRows(
        await pool.query(`INSERT INTO test_full (description) VALUES ('to be deleted') RETURNING id`)
      );

      await context.replicateSnapshot();

      await pool.query(`DELETE FROM test_full WHERE id = $1`, [id1]);

      await context.replicateSnapshot();

      // Verify data is removed
      const data = await context.getBucketData('global[]');
      expect(data).toMatchObject([stream_utils.removeOp('test_full', id1)]);
    });

    test('mixed tables with FULL and DEFAULT replica identity', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      // Table with REPLICA IDENTITY FULL
      await pool.query(`
        CREATE TABLE table_full (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          data TEXT
        )
      `);
      await pool.query(`ALTER TABLE table_full REPLICA IDENTITY FULL`);

      // Table with DEFAULT replica identity
      await pool.query(`
        CREATE TABLE table_default (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          data TEXT
        )
      `);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, data FROM table_full
              - SELECT id, data FROM table_default
      `);

      await context.initializeReplication();

      // Verify table settings
      const tables = await context.getResolvedTables();
      const tableFull = tables.find((t) => t.name === 'table_full');
      const tableDefault = tables.find((t) => t.name === 'table_default');

      expect(tableFull?.storeCurrentData).toBe(false);
      expect(tableDefault?.storeCurrentData).toBe(true);

      // Insert into both tables
      const [{ id: id1 }] = pgwireRows(
        await pool.query(`INSERT INTO table_full (data) VALUES ('from full') RETURNING id`)
      );
      const [{ id: id2 }] = pgwireRows(
        await pool.query(`INSERT INTO table_default (data) VALUES ('from default') RETURNING id`)
      );

      await context.replicateSnapshot();

      // Verify both are synced
      const data = await context.getBucketData('global[]');
      expect(data).toHaveLength(2);

      const fullRecord = data.find((op: any) => op.object_id === id1);
      const defaultRecord = data.find((op: any) => op.object_id === id2);

      expect(fullRecord).toBeDefined();
      expect(defaultRecord).toBeDefined();
    });

    test('changing REPLICA IDENTITY from DEFAULT to FULL updates storeCurrentData', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      // Create table with DEFAULT replica identity
      await pool.query(`
        CREATE TABLE test_changeable (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          description TEXT
        )
      `);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, description FROM test_changeable
      `);

      await context.initializeReplication();

      let tables = await context.getResolvedTables();
      let testTable = tables.find((t) => t.name === 'test_changeable');
      expect(testTable?.storeCurrentData).toBe(true);

      // Stop replication
      await context[Symbol.asyncDispose]();

      // Change to REPLICA IDENTITY FULL
      await pool.query(`ALTER TABLE test_changeable REPLICA IDENTITY FULL`);

      // Restart context and replication
      context = await WalStreamTestContext.open(factory);
      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, description FROM test_changeable
      `);

      await context.initializeReplication();

      // Verify the setting changed
      tables = await context.getResolvedTables();
      testTable = tables.find((t) => t.name === 'test_changeable');
      expect(testTable?.storeCurrentData).toBe(false);
    });

    test('table with REPLICA IDENTITY INDEX sets storeCurrentData=true', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      await pool.query(`
        CREATE TABLE test_index (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          email TEXT UNIQUE NOT NULL,
          description TEXT
        )
      `);
      await pool.query(`ALTER TABLE test_index REPLICA IDENTITY USING INDEX test_index_email_key`);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, email, description FROM test_index
      `);

      await context.initializeReplication();

      const tables = await context.getResolvedTables();
      const testTable = tables.find((t) => t.name === 'test_index');

      expect(testTable).toBeDefined();
      // INDEX replica identity still needs storeCurrentData because only index columns are sent
      expect(testTable!.storeCurrentData).toBe(true);
    });

    test('table with REPLICA IDENTITY NOTHING sets storeCurrentData=true', async () => {
      context = await WalStreamTestContext.open(factory);
      const { pool } = context;

      await pool.query(`
        CREATE TABLE test_nothing (
          id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
          description TEXT
        )
      `);
      await pool.query(`ALTER TABLE test_nothing REPLICA IDENTITY NOTHING`);

      await context.updateSyncRules(`
        bucket_definitions:
          global:
            data:
              - SELECT id, description FROM test_nothing
      `);

      await context.initializeReplication();

      const tables = await context.getResolvedTables();
      const testTable = tables.find((t) => t.name === 'test_nothing');

      expect(testTable).toBeDefined();
      // NOTHING means no replica identity - we need storeCurrentData
      expect(testTable!.storeCurrentData).toBe(true);
    });
  });
});
