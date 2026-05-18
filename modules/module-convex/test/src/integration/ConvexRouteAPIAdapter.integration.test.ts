import { ConvexRouteAPIAdapter } from '@module/api/ConvexRouteAPIAdapter.js';
import { normalizeConnectionConfig } from '@module/types/types.js';
import type { DatabaseSchema } from '@powersync/service-types';
import { randomUUID } from 'crypto';
import { describe, expect, test } from 'vitest';
import { env } from '../env.js';
import { ConvexStreamTestContext } from '../test-utils/ConvexStreamTestContext.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from '../test-utils/util.js';

const TEST_CONNECTION_OPTIONS = normalizeConnectionConfig({
  type: 'convex',
  deploy_key: env.CONVEX_DEPLOY_KEY,
  deployment_url: env.CONVEX_URL
});

function normalizeSchemaForSnapshot(schema: DatabaseSchema[]): DatabaseSchema[] {
  const snapshottedTables = new Set(['lists', 'todos']);

  return schema.map((database) => ({
    ...database,
    tables: [...database.tables]
      .filter((table) => snapshottedTables.has(table.name))
      .sort((a, b) => a.name.localeCompare(b.name))
      .map((table) => ({
        columns: [...table.columns]
          .sort((a, b) => a.name.localeCompare(b.name))
          .map((column) => ({
            internal_type: column.internal_type,
            name: column.name,
            pg_type: column.pg_type,
            sqlite_type: column.sqlite_type,
            type: column.type
          })),
        name: table.name
      }))
  }));
}

describe.skipIf(!env.CONVEX_DEPLOY_KEY)('ConvexStream ConvexRouteAPIAdapter tests', function () {
  test('json_schemas lists schema-defined tables without documents', async () => {
    /**
     * The Convex stream uses json_schemas for initial wildcard table expansion.
     * If json_schemas only listed tables that already contain documents, then a
     * wildcard sync rule could miss an empty-but-schema-defined table at the
     * initial snapshot boundary and would need a later inline snapshot when the
     * first delta appears. This verifies that table names are available even
     * before any documents exist, so new table deltas can be applied directly.
     */
    await using context = await ConvexStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {});

    const schemas = await context.connectionManager.client.getJsonSchemas();
    const tableNames = schemas.tables.map((table) => table.tableName);

    expect(tableNames).toContain('schema_only_probe');

    const page = await context.connectionManager.client.listSnapshot({ tableName: 'schema_only_probe' });
    expect(page.values).toHaveLength(0);
  });

  test('retrieves the testing Convex schema in the expected service schema format', async () => {
    /**
     * It seems like Convex requires the table to contain populated columns in order to report
     * valuable column information.
     */
    await using context = await ConvexStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY.factory, {});
    // Create an item
    await context.backend.client.mutation(context.backend.api.lists.createBatch, {
      lists: [
        {
          name: 'a string name',
          uuid: randomUUID(),
          archived: 1,
          attributes: { color: 'red' },
          created_at: new Date().toISOString(),
          owner: 'an owner',
          owner_id: randomUUID(),
          settings: {
            color: 'red',
            is_public: true,
            theme: 'theme'
          },
          tags: ['one', 'two']
        }
      ]
    });
    await using adapter = new ConvexRouteAPIAdapter(TEST_CONNECTION_OPTIONS);
    const schema = await adapter.getConnectionSchema();
    expect(schema).toMatchObject(
      expect.arrayContaining([
        {
          name: 'convex',
          tables: expect.arrayContaining([
            {
              name: 'lists',
              columns: expect.arrayContaining([
                {
                  internal_type: 'string',
                  name: '_id',
                  pg_type: 'string',
                  sqlite_type: 2,
                  type: 'id'
                },
                {
                  internal_type: 'float64',
                  name: 'archived',
                  pg_type: 'float64',
                  sqlite_type: 8,
                  type: 'number'
                },
                {
                  internal_type: 'object',
                  name: 'attributes',
                  pg_type: 'object',
                  sqlite_type: 2,
                  type: 'object'
                },
                {
                  internal_type: 'string',
                  name: 'created_at',
                  pg_type: 'string',
                  sqlite_type: 2,
                  type: 'string'
                },
                {
                  internal_type: 'string',
                  name: 'name',
                  pg_type: 'string',
                  sqlite_type: 2,
                  type: 'string'
                },
                {
                  internal_type: 'string',
                  name: 'owner',
                  pg_type: 'string',
                  sqlite_type: 2,
                  type: 'string'
                },
                {
                  internal_type: 'string',
                  name: 'owner_id',
                  pg_type: 'string',
                  sqlite_type: 2,
                  type: 'string'
                },
                {
                  internal_type: 'object',
                  name: 'settings',
                  pg_type: 'object',
                  sqlite_type: 2,
                  type: 'object'
                },
                {
                  internal_type: 'array',
                  name: 'tags',
                  pg_type: 'array',
                  sqlite_type: 2,
                  type: 'array'
                },
                {
                  internal_type: 'string',
                  name: 'uuid',
                  pg_type: 'string',
                  sqlite_type: 2,
                  type: 'string'
                }
              ])
            }
          ])
        }
      ])
    );
  });
});
