import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import { SqlSyncRules, TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '@powersync/service-sync-rules';
import { describe, expect, test } from 'vitest';
import { clearTestDb, connectPgPool, TEST_CONNECTION_OPTIONS } from './util.js';

describe('PostgresRouteAPIAdapter tests', () => {
  test('infers connection schema', async () => {
    const db = await connectPgPool();
    try {
      await clearTestDb(db);
      const api = new PostgresRouteAPIAdapter(db);

      await db.query(`CREATE DOMAIN rating_value AS FLOAT CHECK (VALUE BETWEEN 0 AND 5)`);
      await db.query(`
        CREATE TABLE test_users (
            id TEXT NOT NULL PRIMARY KEY,
            is_admin BOOLEAN,
            rating RATING_VALUE
        );
      `);

      const schema = await api.getConnectionSchema();
      // Ignore any other potential schemas in the test database, for example the 'powersync' schema.
      const filtered = schema.filter((s) => s.name == 'public');
      expect(filtered).toStrictEqual([
        {
          name: 'public',
          tables: [
            {
              name: 'test_users',
              columns: [
                {
                  internal_type: 'text',
                  name: 'id',
                  pg_type: 'text',
                  sqlite_type: TYPE_TEXT,
                  type: 'text'
                },
                {
                  internal_type: 'boolean',
                  name: 'is_admin',
                  pg_type: 'bool',
                  sqlite_type: TYPE_INTEGER,
                  type: 'boolean'
                },
                {
                  internal_type: 'rating_value',
                  name: 'rating',
                  pg_type: 'rating_value',
                  sqlite_type: TYPE_REAL,
                  type: 'rating_value'
                }
              ]
            }
          ]
        }
      ]);
    } finally {
      await db.end();
    }
  });

  test('batches debug table info through a refcursor-backed connection', async () => {
    const db = await connectPgPool();
    try {
      await clearTestDb(db);
      await db.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

      const parsed = SqlSyncRules.fromYaml(
        `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
      - SELECT * FROM "other"
      - SELECT * FROM "other%"
`,
        { defaultSchema: 'public' }
      );

      const api = new PostgresRouteAPIAdapter(db, undefined, TEST_CONNECTION_OPTIONS as any);
      const tableInfo = await api.getDebugTablesInfo(parsed.config.getSourceTables(), parsed.config);

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
    } finally {
      await db.end();
    }
  });
});
