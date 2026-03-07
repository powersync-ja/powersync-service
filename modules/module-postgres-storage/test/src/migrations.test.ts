import { beforeEach, describe, expect, it } from 'vitest';

import { Direction } from '@powersync/lib-services-framework';
import { register } from '@powersync/service-core-tests';
import { dropTables, PostgresBucketStorageFactory } from '../../src/index.js';
import { PostgresMigrationAgent } from '../../src/migrations/PostgresMigrationAgent.js';
import { env } from './env.js';
import { POSTGRES_STORAGE_FACTORY, POSTGRES_STORAGE_SETUP, TEST_CONNECTION_OPTIONS } from './util.js';

const MIGRATION_AGENT_FACTORY = () => {
  return new PostgresMigrationAgent({ type: 'postgresql', uri: env.PG_STORAGE_TEST_URL, sslmode: 'disable' });
};

describe('Migrations', () => {
  beforeEach(async () => {
    // The migration tests clear the migration store, without running the down migrations.
    // This ensures all the down migrations have been run before.
    const setup = POSTGRES_STORAGE_SETUP;
    await using factory = new PostgresBucketStorageFactory({
      config: TEST_CONNECTION_OPTIONS,
      slot_name_prefix: 'test_'
    });

    await dropTables(factory.db);
    await setup.migrate(Direction.Down);
  });

  register.registerMigrationTests(MIGRATION_AGENT_FACTORY);

  it('Should have tables declared', async () => {
    const { db } = await POSTGRES_STORAGE_FACTORY.factory();

    const tables = await db.sql`
      SELECT
        table_schema,
        table_name
      FROM
        information_schema.tables
      WHERE
        table_type = 'BASE TABLE'
        AND table_schema NOT IN ('pg_catalog', 'information_schema')
      ORDER BY
        table_schema,
        table_name;
    `.rows<{ table_schema: string; table_name: string }>();

    expect(tables.find((t) => t.table_name == 'sync_rules')).exist;
  });
});
