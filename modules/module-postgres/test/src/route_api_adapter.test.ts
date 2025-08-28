import { describe, expect, test } from 'vitest';
import { clearTestDb, connectPgPool } from './util.js';
import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import { TYPE_INTEGER, TYPE_REAL, TYPE_TEXT } from '@powersync/service-sync-rules';

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
      expect(schema).toStrictEqual([
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
});
