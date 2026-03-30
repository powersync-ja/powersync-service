import { getDebugTablesInfoBatched } from '@module/replication/replication-utils.js';
import * as pgwire from '@powersync/service-jpgwire';
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { expect, test } from 'vitest';

import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_CONNECTION_OPTIONS } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

function parseRules(syncRuleContent: string) {
  return SqlSyncRules.fromYaml(syncRuleContent, { defaultSchema: 'public' }).config;
}

async function validateSyncRules(context: WalStreamTestContext, syncRuleContent: string) {
  const connection = await context.connectionManager.snapshotConnection();
  await using _ = { [Symbol.asyncDispose]: () => connection.end() };
  return await validateSyncRulesWithConnection(context, connection, syncRuleContent);
}

async function validateSyncRulesWithConnection(
  context: WalStreamTestContext,
  connection: pgwire.PgConnection,
  syncRuleContent: string
) {
  const syncRules = parseRules(syncRuleContent);
  return getDebugTablesInfoBatched({
    db: connection,
    publicationName: context.publicationName,
    connectionTag: context.connectionTag,
    tablePatterns: syncRules.getSourceTables(),
    syncRules
  });
}

function getPatternResult(results: Awaited<ReturnType<typeof getDebugTablesInfoBatched>>, pattern: string) {
  const result = results.find((entry) => entry.pattern == pattern);
  expect(result).toBeDefined();
  return result!;
}

function getExactTable(results: Awaited<ReturnType<typeof getDebugTablesInfoBatched>>, pattern: string) {
  const result = getPatternResult(results, pattern);
  expect(result.wildcard).toBe(false);
  expect(result.table).toBeDefined();
  return result.table!;
}

function getWildcardTables(results: Awaited<ReturnType<typeof getDebugTablesInfoBatched>>, pattern: string) {
  const result = getPatternResult(results, pattern);
  expect(result.wildcard).toBe(true);
  return [...(result.tables ?? [])].sort((a, b) => a.name.localeCompare(b.name));
}

function errorMessages(table: { errors: { message: string }[] }) {
  return table.errors.map((error) => error.message);
}

test('validate tables covers exact, missing, wildcard and parameter-query sources', async () => {
  await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory);
  const { pool } = context;

  await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);
  await pool.query(`CREATE TABLE test_param_data(id uuid primary key default uuid_generate_v4(), description text)`);
  await pool.query(`CREATE TABLE test_wild_alpha(id uuid primary key default uuid_generate_v4(), description text)`);
  await pool.query(`CREATE TABLE test_wild_beta(id uuid primary key default uuid_generate_v4(), description text)`);

  const results = await validateSyncRules(
    context,
    `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
      - SELECT * FROM "test_missing_exact"
      - SELECT * FROM "test_wild_%"
      - SELECT * FROM "test_missing_%"
  by_param:
    parameters: SELECT id FROM test_param_data WHERE id = token_parameters.user_id
    data: []
`
  );

  expect(getExactTable(results, 'test_data')).toEqual({
    schema: 'public',
    name: 'test_data',
    replication_id: ['id'],
    pattern: undefined,
    data_queries: true,
    parameter_queries: false,
    errors: []
  });

  expect(getExactTable(results, 'test_missing_exact')).toEqual({
    schema: 'public',
    name: 'test_missing_exact',
    replication_id: [],
    pattern: undefined,
    data_queries: true,
    parameter_queries: false,
    errors: [{ level: 'warning', message: 'Table "public"."test_missing_exact" not found.' }]
  });

  expect(getExactTable(results, 'test_param_data')).toEqual({
    schema: 'public',
    name: 'test_param_data',
    replication_id: ['id'],
    pattern: undefined,
    data_queries: false,
    parameter_queries: true,
    errors: []
  });

  expect(getWildcardTables(results, 'test_wild_%')).toEqual([
    {
      schema: 'public',
      name: 'test_wild_alpha',
      pattern: 'test_wild_%',
      replication_id: ['id'],
      data_queries: true,
      parameter_queries: false,
      errors: []
    },
    {
      schema: 'public',
      name: 'test_wild_beta',
      pattern: 'test_wild_%',
      replication_id: ['id'],
      data_queries: true,
      parameter_queries: false,
      errors: []
    }
  ]);

  expect(getWildcardTables(results, 'test_missing_%')).toEqual([]);
});

test('validate tables covers replica identity modes and publication membership', async () => {
  await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory);
  const { pool } = context;

  await pool.query(`CREATE TABLE test_default_pk(id text primary key, description text)`);
  await pool.query(`CREATE TABLE test_no_pk(id text, description text)`);
  await pool.query(`CREATE TABLE test_full_identity(id text, description text)`);
  await pool.query(`ALTER TABLE test_full_identity REPLICA IDENTITY FULL`);
  await pool.query(`CREATE TABLE test_index_identity(id text not null, description text not null)`);
  await pool.query(`CREATE UNIQUE INDEX test_index_identity_repl_idx ON test_index_identity(description, id)`);
  await pool.query(`ALTER TABLE test_index_identity REPLICA IDENTITY USING INDEX test_index_identity_repl_idx`);
  await pool.query(`CREATE TABLE test_nothing_identity(id text primary key, description text)`);
  await pool.query(`ALTER TABLE test_nothing_identity REPLICA IDENTITY NOTHING`);
  await pool.query(`CREATE TABLE test_missing_publication(id text primary key, description text)`);

  await pool.query(`DROP PUBLICATION powersync`);
  await pool.query(`
    CREATE PUBLICATION powersync FOR TABLE
      test_default_pk,
      test_no_pk,
      test_full_identity,
      test_index_identity,
      test_nothing_identity
  `);

  const results = await validateSyncRules(
    context,
    `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test_default_pk
      - SELECT * FROM test_no_pk
      - SELECT * FROM test_full_identity
      - SELECT * FROM test_index_identity
      - SELECT * FROM test_nothing_identity
      - SELECT * FROM test_missing_publication
`
  );

  expect(getExactTable(results, 'test_default_pk')).toEqual({
    schema: 'public',
    name: 'test_default_pk',
    replication_id: ['id'],
    pattern: undefined,
    data_queries: true,
    parameter_queries: false,
    errors: []
  });

  const noPk = getExactTable(results, 'test_no_pk');
  expect(noPk.replication_id).toEqual([]);
  expect(noPk.data_queries).toBe(true);
  expect(noPk.parameter_queries).toBe(false);
  expect(errorMessages(noPk)).toEqual([
    'No replication id found for "public"."test_no_pk". Replica identity: default. Configure a primary key on the table.'
  ]);

  expect(getExactTable(results, 'test_full_identity')).toEqual({
    schema: 'public',
    name: 'test_full_identity',
    replication_id: ['id', 'description'],
    pattern: undefined,
    data_queries: true,
    parameter_queries: false,
    errors: []
  });

  expect(getExactTable(results, 'test_index_identity')).toEqual({
    schema: 'public',
    name: 'test_index_identity',
    replication_id: ['id', 'description'],
    pattern: undefined,
    data_queries: true,
    parameter_queries: false,
    errors: []
  });

  expect(getExactTable(results, 'test_nothing_identity')).toEqual({
    schema: 'public',
    name: 'test_nothing_identity',
    replication_id: [],
    pattern: undefined,
    data_queries: true,
    parameter_queries: false,
    errors: []
  });

  const missingPublication = getExactTable(results, 'test_missing_publication');
  expect(missingPublication.replication_id).toEqual(['id']);
  expect(missingPublication.data_queries).toBe(true);
  expect(missingPublication.parameter_queries).toBe(false);
  expect(errorMessages(missingPublication)).toEqual([
    `Table "public"."test_missing_publication" is not part of publication 'powersync'. Run: \`ALTER PUBLICATION powersync ADD TABLE "public"."test_missing_publication"\`.`
  ]);
});

test('validate tables covers rls warnings and select permission failures', async () => {
  await using context = await WalStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory);
  const { pool } = context;
  const roleName = `validate_reader_${Date.now().toString(36)}`;
  const password = 'validate_reader_password';

  await pool.query(`CREATE TABLE test_rls(id text primary key, description text)`);
  await pool.query(`ALTER TABLE test_rls ENABLE ROW LEVEL SECURITY`);
  await pool.query(`CREATE TABLE test_no_select(id text primary key, description text)`);
  await pool.query(`DROP PUBLICATION powersync`);
  await pool.query(`CREATE PUBLICATION powersync FOR TABLE test_rls, test_no_select`);

  await pool.query(`CREATE ROLE ${roleName} LOGIN PASSWORD '${password}'`);
  await pool.query(`GRANT USAGE ON SCHEMA public TO ${roleName}`);
  await pool.query(`GRANT SELECT ON test_rls TO ${roleName}`);

  const connection = await pgwire.connectPgWire(
    {
      ...TEST_CONNECTION_OPTIONS,
      username: roleName,
      password
    },
    { type: 'standard', applicationName: 'powersync-tests' }
  );

  try {
    const results = await validateSyncRulesWithConnection(
      context,
      connection,
      `
bucket_definitions:
  global:
    data:
      - SELECT * FROM test_rls
      - SELECT * FROM test_no_select
`
    );

    const rlsTable = getExactTable(results, 'test_rls');
    expect(rlsTable.replication_id).toEqual(['id']);
    expect(rlsTable.data_queries).toBe(true);
    expect(rlsTable.parameter_queries).toBe(false);
    expect(errorMessages(rlsTable)).toEqual([
      `[PSYNC_S1145] Row Level Security is enabled on table "test_rls". To make sure that ${roleName} can read the table, run: 'ALTER ROLE ${roleName} BYPASSRLS'.`
    ]);

    const noSelectTable = getExactTable(results, 'test_no_select');
    expect(noSelectTable.replication_id).toEqual(['id']);
    expect(noSelectTable.data_queries).toBe(true);
    expect(noSelectTable.parameter_queries).toBe(false);
    expect(noSelectTable.errors).toHaveLength(1);
    expect(noSelectTable.errors[0]).toMatchObject({
      level: 'fatal'
    });
    expect(noSelectTable.errors[0]!.message).toContain('permission denied for table test_no_select');
  } finally {
    await connection.end();
    await pool.query(`DROP OWNED BY ${roleName}`);
    await pool.query(`DROP ROLE ${roleName}`);
  }
});
