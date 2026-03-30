import { getDebugTablesInfoBatched } from '@module/replication/replication-utils.js';
import { expect, test } from 'vitest';

import { updateSyncRulesFromYaml } from '@powersync/service-core';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

test('validate tables', async () => {
  await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory);
  const { pool, connectionManager } = context;

  await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

  const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
      - SELECT * FROM "other"
      - SELECT * FROM "other%"
`;

  const syncRules = await context.factory.updateSyncRules(updateSyncRulesFromYaml(syncRuleContent));

  const tablePatterns = syncRules.parsed({ defaultSchema: 'public' }).sync_rules.config.getSourceTables();
  const connection = await connectionManager.snapshotConnection();
  await using _ = { [Symbol.asyncDispose]: () => connection.end() };
  const tableInfo = await getDebugTablesInfoBatched({
    db: connection,
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
