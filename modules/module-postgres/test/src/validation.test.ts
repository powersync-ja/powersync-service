import { MONGO_STORAGE_FACTORY } from '@core-tests/util.js';
import { expect, test } from 'vitest';
import { walStreamTest } from './wal_stream_utils.js';
import { getDebugTablesInfo } from '@module/replication/replication-utils.js';

// Not quite a walStreamTest, but it helps to manage the connection
test(
  'validate tables',
  walStreamTest(MONGO_STORAGE_FACTORY, async (context) => {
    const { pool } = context;

    await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

    const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
      - SELECT * FROM "other"
      - SELECT * FROM "other%"
`;

    const syncRules = await context.factory.updateSyncRules({ content: syncRuleContent });

    const tablePatterns = syncRules.parsed({ defaultSchema: 'public' }).sync_rules.getSourceTables();
    const tableInfo = await getDebugTablesInfo({
      db: pool,
      publicationName: context.publicationName,
      connectionTag: context.connectionTag,
      tablePatterns: tablePatterns,
      syncRules: syncRules.parsed({ defaultSchema: 'public' }).sync_rules
    });
    expect(tableInfo).toEqual([
      {
        schema: 'public',
        pattern: 'test_data',
        wildcard: false,
        table: {
          schema: 'public',
          name: 'test_data',
          replication_id: ['id'],
          pattern: undefined,
          data_queries: true,
          parameter_queries: false,
          errors: []
        }
      },
      {
        schema: 'public',
        pattern: 'other',
        wildcard: false,
        table: {
          schema: 'public',
          name: 'other',
          replication_id: [],
          data_queries: true,
          parameter_queries: false,
          errors: [{ level: 'warning', message: 'Table "public"."other" not found.' }]
        }
      },
      { schema: 'public', pattern: 'other%', wildcard: true, tables: [] }
    ]);
  })
);
