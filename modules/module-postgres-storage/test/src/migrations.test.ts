import { describe, expect, it } from 'vitest';
import { POSTGRES_STORAGE_FACTORY } from './util.js';

describe('Migrations', () => {
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
