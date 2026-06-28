import { putOp } from '@powersync/service-core-tests';
import { pgwireRows } from '@powersync/service-jpgwire';
import { describe, expect, test } from 'vitest';
import { describeWithStorage, StorageVersionTestContext } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

// A wildcard schema (`%`) lets one stream span many schemas, exposing the per-row schema as `_schema`
// and resolving it per client from a JWT claim. A single slot and `FOR ALL TABLES` publication cover all.
const TENANT_STREAM = `
streams:
  stream:
    query: SELECT * FROM "%".test_data WHERE _schema = auth.parameters() ->> 'tenant_schema'

config:
  edition: 2
`;

describe('schema-per-tenant streams', () => {
  describeWithStorage({ timeout: 30_000 }, defineTests);
});

function defineTests({ factory, storageVersion }: StorageVersionTestContext) {
  const openContext = (options?: Parameters<typeof WalStreamTestContext.open>[1]) => {
    return WalStreamTestContext.open(factory, { ...options, storageVersion });
  };

  const setupTenants = async (pool: WalStreamTestContext['pool'], schemas: string[]) => {
    for (const schema of schemas) {
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
      await pool.query(`CREATE SCHEMA ${schema}`);
      await pool.query(
        `CREATE TABLE ${schema}.test_data(id uuid primary key default uuid_generate_v4(), description text)`
      );
    }
  };

  test('discovers tables across all tenant schemas (single slot)', async () => {
    await using context = await openContext();
    const { pool } = context;
    await context.updateSyncRules(TENANT_STREAM);
    await setupTenants(pool, ['tenant_a', 'tenant_b']);

    const resolved = await context.getResolvedTables();
    const qualified = resolved.map((t) => `${t.schema}.${t.name}`);

    expect(qualified).toContain('tenant_a.test_data');
    expect(qualified).toContain('tenant_b.test_data');
  });

  test('routes each tenant row to a per-schema bucket, without syncing _schema', async () => {
    await using context = await openContext();
    const { pool } = context;
    await context.updateSyncRules(TENANT_STREAM);
    await setupTenants(pool, ['tenant_a', 'tenant_b']);

    await context.initializeReplication();

    const [{ a_id }] = pgwireRows(
      await pool.query(`INSERT INTO tenant_a.test_data(description) VALUES('a') RETURNING id AS a_id`)
    );
    const [{ b_id }] = pgwireRows(
      await pool.query(`INSERT INTO tenant_b.test_data(description) VALUES('b') RETURNING id AS b_id`)
    );

    expect(await context.getBucketData('stream|0["tenant_a"]')).toMatchObject([
      putOp('test_data', { id: a_id, description: 'a' })
    ]);
    expect(await context.getBucketData('stream|0["tenant_b"]')).toMatchObject([
      putOp('test_data', { id: b_id, description: 'b' })
    ]);
  });
}
