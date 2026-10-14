import * as lib_postgres from '@powersync/lib-service-postgres';
import { storage, updateSyncRulesFromYaml } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import { afterEach, describe, expect, test, vi } from 'vitest';
import { chunkRowsBySize } from '../../src/storage/batch/PostgresPersistedBatch.js';
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

/**
 * Records the INSERT statements executed on any connection, by target table.
 */
function spyOnInserts() {
  const spy = vi.spyOn(lib_postgres.AbstractPostgresConnection.prototype, 'query');
  return {
    count(table: string) {
      const pattern = new RegExp(`INSERT INTO\\s+${table}\\b`);
      return spy.mock.calls.filter(([statement]) => {
        return typeof statement == 'object' && pattern.test(statement.statement);
      }).length;
    }
  };
}

async function readBucketData(db: lib_postgres.DatabaseClient, groupId: number) {
  return await db.sql`
    SELECT
      bucket_name,
      op_id,
      data
    FROM
      bucket_data
    WHERE
      group_id = ${{ type: 'int4', value: groupId }}
    ORDER BY
      op_id
  `.rows<{ bucket_name: string; op_id: bigint; data: string }>();
}

describe('chunkRowsBySize', () => {
  const size = (row: string) => row.length;

  test('splits rows into consecutive chunks by estimated size', () => {
    expect(chunkRowsBySize(['aa', 'bb', 'cc', 'dd', 'e'], size, 4)).toEqual([['aa', 'bb'], ['cc', 'dd'], ['e']]);
  });

  test('keeps all rows in one chunk within the limit', () => {
    expect(chunkRowsBySize(['aa', 'bb'], size, 4)).toEqual([['aa', 'bb']]);
  });

  test('places a single row larger than the limit in its own chunk', () => {
    expect(chunkRowsBySize(['aa', 'bbbbbbbb', 'cc', 'dd'], size, 4)).toEqual([['aa'], ['bbbbbbbb'], ['cc', 'dd']]);
  });

  test('returns no chunks for no rows', () => {
    expect(chunkRowsBySize([], size, 4)).toEqual([]);
  });
});

for (const storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Postgres Sync Bucket Storage - flush chunking - v${storageVersion}`, () => {
    afterEach(() => {
      vi.restoreAllMocks();
    });

    test('splits bucket_data inserts by max_statement_size', async () => {
      const setup = createPostgresTestSetup({ batchLimits: { max_statement_size: 2_000 } });
      await using factory = await setup.factory();
      const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(BY_TAG_SYNC_CONFIG, { storageVersion }));
      const bucketStorage = factory.getInstance(syncRules);

      const tags = Array.from({ length: 50 }, (_, i) => `tag${i}`);
      const description = 'x'.repeat(500);
      const inserts = spyOnInserts();

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
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

      // Each row is roughly 600 bytes, so this needs multiple statements.
      expect(inserts.count('bucket_data')).toBeGreaterThan(1);

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

    test('splits bucket_parameters inserts by max_statement_size', async () => {
      const setup = createPostgresTestSetup({ batchLimits: { max_statement_size: 2_000 } });
      await using factory = await setup.factory();
      const syncRules = await factory.updateSyncRules(
        updateSyncRulesFromYaml(BY_OWNER_SYNC_CONFIG, { storageVersion })
      );
      const bucketStorage = factory.getInstance(syncRules);

      const userCount = 30;
      const inserts = spyOnInserts();

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'users', ['id'], POSTGRES_STORAGE_FACTORY, 2);
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

      expect(inserts.count('bucket_parameters')).toBeGreaterThan(1);

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

    test('flushes a single row evaluated into many buckets with large data', async () => {
      // Regression test: a single row with large data evaluated into many buckets could
      // previously exceed the maximum string length when serializing the INSERT parameter.
      await using factory = await POSTGRES_STORAGE_FACTORY.factory();
      const syncRules = await factory.updateSyncRules(updateSyncRulesFromYaml(BY_TAG_SYNC_CONFIG, { storageVersion }));
      const bucketStorage = factory.getInstance(syncRules);

      const tags = Array.from({ length: 300 }, (_, i) => `tag${i}`);
      const description = '0123456789'.repeat(30_000);
      const inserts = spyOnInserts();

      await using writer = await bucketStorage.createWriter(test_utils.BATCH_OPTIONS);
      const sourceTable = await test_utils.resolveTestTable(writer, 'test', ['id'], POSTGRES_STORAGE_FACTORY);
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

      // 300 buckets x 300kB exceeds the default max_statement_size of 64MB.
      expect(inserts.count('bucket_data')).toBeGreaterThan(1);

      const rows = await readBucketData(factory.db, bucketStorage.replicationStreamId);
      expect(rows.length).toEqual(tags.length);
      expect(new Set(rows.map((row) => row.bucket_name)).size).toEqual(tags.length);
      for (const row of rows) {
        expect(JSON.parse(row.data).description).toEqual(description);
      }
    });
  });
}
