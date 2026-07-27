import { reduceBucket } from '@powersync/service-core';
import { describe, expect, test } from 'vitest';
import { describeWithStorage, StorageVersionTestContext } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

describe('parallel snapshots', () => {
  describeWithStorage({ timeout: 120_000 }, defineParallelSnapshotTests);
});

function defineParallelSnapshotTests({ factory, storageVersion }: StorageVersionTestContext) {
  test('initial snapshot with 2 workers', async () => {
    // Multiple tables of different sizes, snapshotted by 2 concurrent workers.
    // A small chunk length forces multiple chunks per table, exercising the
    // pipelined flushes (two alternating writers per worker) as well.
    await using context = await WalStreamTestContext.open(factory, {
      storageVersion,
      walStreamOptions: { snapshotConcurrency: 2, snapshotChunkLength: 100 }
    });

    await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      - SELECT * FROM test_a
      - SELECT * FROM test_b
      - SELECT * FROM test_c
      - SELECT * FROM test_d`);
    const { pool } = context;

    // Sizes chosen to span multiple chunks, a single partial chunk, and an empty table.
    await pool.query(`CREATE TABLE test_a(id int4 primary key, description text)`);
    await pool.query(`CREATE TABLE test_b(id int4 primary key, description text)`);
    await pool.query(`CREATE TABLE test_c(id int4 primary key, description text)`);
    await pool.query(`CREATE TABLE test_d(id int4 primary key, description text)`);
    await pool.query(`INSERT INTO test_a(id, description) SELECT i, 'a' FROM generate_series(1, 350) i`);
    await pool.query(`INSERT INTO test_b(id, description) SELECT i, 'b' FROM generate_series(1, 240) i`);
    await pool.query(`INSERT INTO test_c(id, description) SELECT i, 'c' FROM generate_series(1, 1) i`);

    await context.replicateSnapshot();

    const data = await context.getBucketData('global[]', undefined, {});
    const reduced = reduceBucket(data);

    const countByTable = new Map<string, number>();
    for (const row of reduced) {
      if (row.object_type != null) {
        countByTable.set(row.object_type, (countByTable.get(row.object_type) ?? 0) + 1);
      }
    }
    expect(countByTable.get('test_a')).toEqual(350);
    expect(countByTable.get('test_b')).toEqual(240);
    expect(countByTable.get('test_c')).toEqual(1);
    expect(countByTable.get('test_d')).toBeUndefined();
  });

  test('snapshot concurrency larger than table count', async () => {
    // More workers than tables must not hang or duplicate data.
    await using context = await WalStreamTestContext.open(factory, {
      storageVersion,
      walStreamOptions: { snapshotConcurrency: 4, snapshotChunkLength: 100 }
    });

    await context.updateSyncRules(`bucket_definitions:
  global:
    data:
      - SELECT * FROM test_data`);
    const { pool } = context;

    await pool.query(`CREATE TABLE test_data(id int4 primary key, description text)`);
    await pool.query(`INSERT INTO test_data(id, description) SELECT i, 'foo' FROM generate_series(1, 250) i`);

    await context.replicateSnapshot();

    const data = await context.getBucketData('global[]', undefined, {});
    const reduced = reduceBucket(data);
    const rows = reduced.filter((row) => row.object_type == 'test_data');
    expect(rows.length).toEqual(250);
  });
}
