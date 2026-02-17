import { getDebugTablesInfo } from '@module/replication/replication-utils.js';
import { expect, test } from 'vitest';

import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

test('validate tables', async () => {
  await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory);
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

  const tablePatterns = syncRules.parsed({ defaultSchema: 'public' }).sync_rules.config.getSourceTables();
  const tableInfo = await getDebugTablesInfo({
    db: pool,
    publicationName: context.publicationName,
    connectionTag: context.connectionTag,
    tablePatterns: tablePatterns,
    syncRules: syncRules.parsed({ defaultSchema: 'public' }).sync_rules.config
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
});
