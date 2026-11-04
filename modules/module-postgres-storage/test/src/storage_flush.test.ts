import * as lib_postgres from '@powersync/lib-service-postgres';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { createPostgresTestSetup, POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

/**
 * Evaluates each row of "test" into one bucket per entry in its "tags" JSON array.
 */
const BY_TAG_SYNC_CONFIG = `
config:
  edition: 3

streams:
  by_tag:
    query: SELECT * FROM test WHERE subscription.parameter('tag') IN (SELECT value FROM json_each(test.tags))
`;

/**
 * Stores a parameter lookup row for each row of "users".
 */
const BY_OWNER_SYNC_CONFIG = `
config:
  edition: 3

streams:
  by_owner:
    query: SELECT * FROM test WHERE owner_id IN (SELECT id FROM users WHERE name = subscription.parameter('name'))
`;

type BucketDataRow = { bucket_name: string; data: string | null; source_key: string };
type ParameterDataRow = { lookup: string; bucket_parameters: string; source_key: string };

/**
 * A statement as passed to `query()`: either a script string or a statement with parameters.
 */
type QueryCall = string | { statement: string; params: { value: unknown }[] };

/**
 * Records the statements executed on any connection.
 */
function spyOnStatements() {
  const spy = vi.spyOn(lib_postgres.AbstractPostgresConnection.prototype, 'query');
  const calls = () => spy.mock.calls.map(([statement]) => statement as QueryCall);
  const inserts = (table: string) => {
    const pattern = new RegExp(`INSERT INTO\\s+${table}\\b`);
    return calls().filter((call): call is Exclude<QueryCall, string> => {
      return typeof call == 'object' && pattern.test(call.statement);
    });
  };
  return {
    insertCount(table: string) {
      return inserts(table).length;
    },
    /**
     * The rows passed to each INSERT statement on the table, in execution order.
     */
    insertedRows<T>(table: string): T[][] {
      return inserts(table).map((call) => call.params[0].value as T[]);
    },
    /**
     * The number of replication transactions started.
     */
    transactionCount() {
      return calls().filter((call) => {
        return typeof call == 'string' && call.includes('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
      }).length;
    }
  };
}

/**
 * The estimates used by PostgresPersistedBatch for the size of a statement.
 */
function estimateBucketDataSize(rows: BucketDataRow[]) {
  return rows.reduce((total, row) => {
    return total + row.bucket_name.length + (row.data?.length ?? 0) + row.source_key.length + 100;
  }, 0);
}

function estimateParameterDataSize(rows: ParameterDataRow[]) {
  return rows.reduce((total, row) => {
    return total + row.lookup.length + row.bucket_parameters.length + row.source_key.length + 100;
  }, 0);
}

async function readBucketData(db: lib_postgres.DatabaseClient, groupId: number) {
  return await db.sql`
    SELECT
      bucket_name,
      op_id,
      op,
      data
    FROM
      bucket_data
    WHERE
      group_id = ${{ type: 'int4', value: groupId }}
    ORDER BY
      op_id
  `.rows<{ bucket_name: string; op_id: bigint; op: string; data: string | null }>();
}

for (const storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Postgres Sync Bucket Storage - flush statements - v${storageVersion}`, () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    test('inserts bucket_data in statements bounded by max_statement_size', async () => {
      const setup = createPostgresTestSetup({ batchLimits: { max_statement_size: 2_000 } });
      await using factory = await setup.factory();
      const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(BY_TAG_SYNC_CONFIG, { storageVersion }));
      const bucketStorage = factory.getInstance(syncRules);

      const tags = Array.from({ length: 50 }, (_, i) => `tag${i}`);
      const description = 'x'.repeat(500);

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
      const statements = spyOnStatements();
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'test1',
          tags: JSON.stringify(tags),
          description
        },
        afterReplicaId: test_utils.rid('test1')
      });
      await writer.flush();

      // Each row is roughly 700 bytes, so this needs multiple statements, all in one transaction.
      const inserted = statements.insertedRows<BucketDataRow>('bucket_data');
      expect(inserted.length).toBeGreaterThan(1);
      expect(statements.transactionCount()).toEqual(1);
      for (const statementRows of inserted) {
        expect(estimateBucketDataSize(statementRows)).toBeLessThanOrEqual(2_000);
      }

      const rows = await readBucketData(factory.db, bucketStorage.replicationStreamId);
      expect(rows.length).toEqual(tags.length);
      // One PUT operation per bucket, with op_ids in insertion order.
      expect(new Set(rows.map((row) => row.bucket_name)).size).toEqual(tags.length);
      for (let i = 1; i < rows.length; i++) {
        expect(rows[i].op_id).toBeGreaterThan(rows[i - 1].op_id);
      }
      for (const row of rows) {
        expect(JSON.parse(row.data)).toEqual({ id: 'test1', tags: JSON.stringify(tags), description });
      }
    });

    test('inserts bucket_parameters in statements bounded by max_statement_size', async () => {
      const setup = createPostgresTestSetup({ batchLimits: { max_statement_size: 2_000 } });
      await using factory = await setup.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(BY_OWNER_SYNC_CONFIG, { storageVersion })
      );
      const bucketStorage = factory.getInstance(syncRules);

      const userCount = 30;

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'users', ['id'], POSTGRES_STORAGE_FACTORY, 2);
      const statements = spyOnStatements();
      for (let i = 0; i < userCount; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: `user${i}`,
            name: `name${i}`
          },
          afterReplicaId: test_utils.rid(`user${i}`)
        });
      }
      await writer.flush();

      const inserted = statements.insertedRows<ParameterDataRow>('bucket_parameters');
      expect(inserted.length).toBeGreaterThan(1);
      expect(statements.transactionCount()).toEqual(1);
      for (const statementRows of inserted) {
        expect(estimateParameterDataSize(statementRows)).toBeLessThanOrEqual(2_000);
      }

      const rows = await factory.db.sql`
        SELECT
          bucket_parameters
        FROM
          bucket_parameters
        WHERE
          group_id = ${{ type: 'int4', value: bucketStorage.replicationStreamId }}
        ORDER BY
          id
      `.rows<{ bucket_parameters: string }>();
      // The looked-up "id" column is stored by position, in insertion order.
      expect(rows.map((row) => JSON.parse(row.bucket_parameters))).toEqual(
        Array.from({ length: userCount }, (_, i) => [{ 0: `user${i}` }])
      );
    });

    test('flushes a single row evaluated into many buckets with large data', { timeout: 30_000 }, async () => {
      // Regression test: a single row with large data evaluated into many buckets could
      // previously exceed the maximum string length when serializing the INSERT parameter.
      await using factory = await POSTGRES_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(BY_TAG_SYNC_CONFIG, { storageVersion }));
      const bucketStorage = factory.getInstance(syncRules);

      const tags = Array.from({ length: 300 }, (_, i) => `tag${i}`);
      const description = '0123456789'.repeat(30_000);

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
      const statements = spyOnStatements();
      await writer.save({
        sourceTable,
        tag: storage.SaveOperationTag.INSERT,
        after: {
          id: 'large1',
          tags: JSON.stringify(tags),
          description
        },
        afterReplicaId: test_utils.rid('large1')
      });
      await writer.flush();

      // 300 buckets x 300kB exceeds the default max_estimated_size (5MB), so the rows are inserted
      // in multiple statements, all in the transaction of this batch.
      expect(statements.insertCount('bucket_data')).toBeGreaterThan(1);
      expect(statements.transactionCount()).toEqual(1);

      const rows = await readBucketData(factory.db, bucketStorage.replicationStreamId);
      expect(rows.length).toEqual(tags.length);
      expect(new Set(rows.map((row) => row.bucket_name)).size).toEqual(tags.length);
      for (const row of rows) {
        expect(JSON.parse(row.data).description).toEqual(description);
      }
    });

    test('keeps the transaction size limit while inserting rows early', async () => {
      // Rows inserted before the end of a batch still count towards max_estimated_size,
      // so inserting early must not grow the transactions.
      const setup = createPostgresTestSetup({ batchLimits: { max_estimated_size: 5_000, max_statement_size: 2_000 } });
      await using factory = await setup.factory();
      const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(BY_TAG_SYNC_CONFIG, { storageVersion }));
      const bucketStorage = factory.getInstance(syncRules);

      const rowCount = 30;
      const description = 'x'.repeat(500);

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
      const statements = spyOnStatements();
      for (let i = 0; i < rowCount; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: `test${i}`,
            tags: JSON.stringify([`tag${i}`]),
            description
          },
          afterReplicaId: test_utils.rid(`test${i}`)
        });
      }
      await writer.flush();

      // Roughly 700 bytes per row against a 5000 byte transaction limit.
      expect(statements.transactionCount()).toBeGreaterThanOrEqual(3);

      const rows = await readBucketData(factory.db, bucketStorage.replicationStreamId);
      expect(rows.length).toEqual(rowCount);
      for (let i = 1; i < rows.length; i++) {
        expect(rows[i].op_id).toBeGreaterThan(rows[i - 1].op_id);
      }
    });

    test('inserts the REMOVE operations of a truncate in statements bounded by max_statement_size', async () => {
      // The REMOVE operations are inserted on the truncate transaction while its current_data rows
      // are processed, on the connection those rows were read from.
      const setup = createPostgresTestSetup({ batchLimits: { max_statement_size: 2_000 } });
      await using factory = await setup.factory();
      const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(BY_TAG_SYNC_CONFIG, { storageVersion }));
      const bucketStorage = factory.getInstance(syncRules);

      const rowCount = 20;
      const tags = Array.from({ length: 20 }, (_, i) => `tag${i}`);

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
      for (let i = 0; i < rowCount; i++) {
        await writer.save({
          sourceTable,
          tag: storage.SaveOperationTag.INSERT,
          after: {
            id: `test${i}`,
            tags: JSON.stringify(tags)
          },
          afterReplicaId: test_utils.rid(`test${i}`)
        });
      }
      await writer.flush();

      const statements = spyOnStatements();
      await writer.truncate([sourceTable]);

      // 400 REMOVE operations of roughly 150 bytes each, all in the transaction of the truncate.
      const inserted = statements.insertedRows<BucketDataRow>('bucket_data');
      expect(inserted.length).toBeGreaterThan(1);
      expect(statements.transactionCount()).toEqual(1);
      for (const statementRows of inserted) {
        expect(estimateBucketDataSize(statementRows)).toBeLessThanOrEqual(2_000);
      }

      const rows = await readBucketData(factory.db, bucketStorage.replicationStreamId);
      const operationCount = rowCount * tags.length;
      expect(rows.length).toEqual(operationCount * 2);
      // The REMOVE operations follow the PUT operations, with op_ids in insertion order.
      expect(rows.slice(0, operationCount).every((row) => row.op == 'PUT')).toBe(true);
      expect(rows.slice(operationCount).every((row) => row.op == 'REMOVE')).toBe(true);
      for (let i = 1; i < rows.length; i++) {
        expect(rows[i].op_id).toBeGreaterThan(rows[i - 1].op_id);
      }
    });
  });
}
