import { describe, expect, it } from 'vitest';

import { register } from '@powersync/service-core-tests';
import { PostgresMigrationAgent } from '../../src/migrations/PostgresMigrationAgent.js';
import { env } from './env.js';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

const MIGRATION_AGENT_FACTORY = () => {
  return new PostgresMigrationAgent({ type: 'postgresql', uri: env.PG_STORAGE_TEST_URL, sslmode: 'disable' });
};

describe('Migrations', () => {
  register.registerMigrationTests(MIGRATION_AGENT_FACTORY);

  it('Should have tables declared', async () => {
    const { db } = await POSTGRES_STORAGE_FACTORY();

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
