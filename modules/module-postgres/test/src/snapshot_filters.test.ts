import { reduceBucket } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';
import { describeWithStorage, StorageVersionTestContext } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

describe('initial snapshot filters', () => {
  describeWithStorage({ timeout: 120_000 }, defineSnapshotFilterTests);
});

function defineSnapshotFilterTests({ factory, storageVersion }: StorageVersionTestContext) {
  test('applies filter during initial snapshot', async () => {
    // A small chunk length so the filter is also exercised on the resume path
    // (WHERE key > $1 AND (filter)) of the chunked snapshot query.
    await using context = await WalStreamTestContext.open(factory, {
      storageVersion,
      walStreamOptions: { snapshotChunkLength: 100 }
    });

    await context.updateSyncRules(`
initial_snapshot_filters:
  test_data:
    sql: "archived = false"

bucket_definitions:
  global:
    data:
      - SELECT id, description FROM test_data WHERE archived = false`);
    const { pool } = context;

    await pool.query(`
      CREATE TABLE test_data(
        id int4 primary key,
        description text,
        archived boolean not null default false
      )`);
    // 1000 rows, of which only 150 match the filter.
    await pool.query(`
      INSERT INTO test_data(id, description, archived)
      SELECT i, 'row ' || i, i % 10 >= 5 OR i > 300 FROM generate_series(1, 1000) i`);

    await context.replicateSnapshot();

    const data = await context.getBucketData('global[]', undefined, {});
    const reduced = reduceBucket(data).filter((row) => row.object_type == 'test_data');
    expect(reduced.length).toEqual(150);
  });

  test('filter with a plain table name matches the table in any schema', async () => {
    await using context = await WalStreamTestContext.open(factory, { storageVersion });

    await context.updateSyncRules(`
initial_snapshot_filters:
  test_data:
    sql: "amount > 10"

bucket_definitions:
  global:
    data:
      - SELECT id, amount FROM "%".test_data WHERE amount > 10`);
    const { pool } = context;

    for (const schema of ['tenant_a', 'tenant_b']) {
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
      await pool.query(`CREATE SCHEMA ${schema}`);
      await pool.query(`CREATE TABLE ${schema}.test_data(id int4 primary key, amount int4)`);
    }
    await pool.query(`INSERT INTO tenant_a.test_data SELECT i, i FROM generate_series(1, 20) i`);
    await pool.query(`INSERT INTO tenant_b.test_data SELECT 100 + i, i FROM generate_series(1, 30) i`);

    await context.replicateSnapshot();

    const data = await context.getBucketData('global[]', undefined, {});
    const reduced = reduceBucket(data).filter((row) => row.object_type == 'test_data');
    // tenant_a: amounts 11-20 (10 rows), tenant_b: amounts 11-30 (20 rows).
    expect(reduced.length).toEqual(30);
  });
}
