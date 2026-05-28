import { putOp, removeOp } from '@powersync/service-core-tests';
import { pgwireRows } from '@powersync/service-jpgwire';
import * as crypto from 'crypto';
import { describe, expect, test } from 'vitest';
import { describeWithStorage, StorageVersionTestContext } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

/**
 * End-to-end tests for the per-table storeCurrentData optimization driven by the source's
 * REPLICA IDENTITY. With REPLICA IDENTITY FULL, Postgres always sends the complete row, so
 * PowerSync does not need to keep its own copy in current_data; for every other replica identity
 * it does. The flag is resolved from `pg_class.relreplident` during table resolution.
 */
describe('replica identity full', () => {
  describeWithStorage({ timeout: 30_000 }, defineReplicaIdentityTests);
});

function defineReplicaIdentityTests({ factory, storageVersion }: StorageVersionTestContext) {
  const openContext = (options?: Parameters<typeof WalStreamTestContext.open>[1]) => {
    return WalStreamTestContext.open(factory, { ...options, storageVersion });
  };

  async function resolvedTable(context: WalStreamTestContext, name: string) {
    const tables = await context.getResolvedTables();
    return tables.find((t) => t.name === name);
  }

  test('REPLICA IDENTITY FULL resolves storeCurrentData=false', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(`CREATE TABLE test_full (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), description TEXT)`);
    await pool.query(`ALTER TABLE test_full REPLICA IDENTITY FULL`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM test_full
`);
    await context.initializeReplication();

    expect((await resolvedTable(context, 'test_full'))?.storeCurrentData).toBe(false);
  });

  test('REPLICA IDENTITY DEFAULT resolves storeCurrentData=true', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(`CREATE TABLE test_default (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), description TEXT)`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM test_default
`);
    await context.initializeReplication();

    expect((await resolvedTable(context, 'test_default'))?.storeCurrentData).toBe(true);
  });

  test('REPLICA IDENTITY USING INDEX resolves storeCurrentData=true', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(
      `CREATE TABLE test_index (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), email TEXT NOT NULL UNIQUE, description TEXT)`
    );
    await pool.query(`ALTER TABLE test_index REPLICA IDENTITY USING INDEX test_index_email_key`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, email, description FROM test_index
`);
    await context.initializeReplication();

    // Only the index columns are sent, so the rest of the row may still be missing - keep current_data.
    expect((await resolvedTable(context, 'test_index'))?.storeCurrentData).toBe(true);
  });

  test('REPLICA IDENTITY NOTHING resolves storeCurrentData=true', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(`CREATE TABLE test_nothing (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), description TEXT)`);
    await pool.query(`ALTER TABLE test_nothing REPLICA IDENTITY NOTHING`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM test_nothing
`);
    await context.initializeReplication();

    expect((await resolvedTable(context, 'test_nothing'))?.storeCurrentData).toBe(true);
  });

  test('replicates a REPLICA IDENTITY FULL table', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(
      `CREATE TABLE test_full (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), description TEXT, value INT)`
    );
    await pool.query(`ALTER TABLE test_full REPLICA IDENTITY FULL`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description, value FROM test_full
`);
    await context.initializeReplication();

    const [{ id }] = pgwireRows(
      await pool.query(`INSERT INTO test_full (description, value) VALUES ('test1', 100) RETURNING id`)
    );

    const data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_full', { id, description: 'test1', value: 100n })]);
  });

  test('streams UPDATE and DELETE for a REPLICA IDENTITY FULL table', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(
      `CREATE TABLE test_full (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), description TEXT, counter INT DEFAULT 0)`
    );
    await pool.query(`ALTER TABLE test_full REPLICA IDENTITY FULL`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description, counter FROM test_full
`);
    await context.initializeReplication();

    const [{ id }] = pgwireRows(
      await pool.query(`INSERT INTO test_full (description, counter) VALUES ('initial', 0) RETURNING id`)
    );
    await pool.query(`UPDATE test_full SET description = 'updated', counter = counter + 1 WHERE id = '${id}'`);

    let data = await context.getBucketData('global[]');
    // The streamed UPDATE is the most recent op, with the new values - even though no copy of the
    // previous row was stored.
    expect(data.at(-1)).toMatchObject(putOp('test_full', { id, description: 'updated', counter: 1n }));

    await pool.query(`DELETE FROM test_full WHERE id = '${id}'`);
    data = await context.getBucketData('global[]');
    expect(data.at(-1)).toMatchObject(removeOp('test_full', id));
  });

  test('UPDATE of an unrelated column preserves an unchanged TOAST value on a FULL table', async () => {
    // Core safety property: on a FULL table no current_data copy is kept and the TOAST merge is skipped,
    // so an unchanged TOAST value survives an UPDATE only because pgoutput sends the full old tuple and
    // the decoder backfills it into the new tuple. Fails if that backfill regresses.
    await using context = await openContext();
    const { pool } = context;
    await pool.query(
      `CREATE TABLE test_full_toast (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT, description TEXT)`
    );
    await pool.query(`ALTER TABLE test_full_toast REPLICA IDENTITY FULL`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, name, description FROM test_full_toast
`);
    await context.initializeReplication();

    // Only meaningful if this table takes the storeCurrentData=false path.
    expect((await resolvedTable(context, 'test_full_toast'))?.storeCurrentData).toBe(false);

    // Must be > 8kb after compression to be stored out-of-line (TOASTed).
    const largeDescription = crypto.randomBytes(20_000).toString('hex');
    const [{ id }] = pgwireRows(
      await pool.query({
        statement: `INSERT INTO test_full_toast(name, description) VALUES('test1', $1) RETURNING id`,
        params: [{ type: 'varchar', value: largeDescription }]
      })
    );

    // Update only `name`; `description` is unchanged, so its TOAST value is omitted from the new tuple.
    await pool.query(`UPDATE test_full_toast SET name = 'test2' WHERE id = '${id}'`);

    const data = await context.getBucketData('global[]');
    // The replica identity is the whole row, so the UPDATE re-keys it (PUT, then REMOVE old + PUT new).
    // Both PUTs must carry the complete row including the unchanged TOAST `description`.
    const puts = data.filter((op) => op.op === 'PUT');
    expect(puts).toMatchObject([
      putOp('test_full_toast', { id, name: 'test1', description: largeDescription }),
      putOp('test_full_toast', { id, name: 'test2', description: largeDescription })
    ]);
  });

  test('mixed FULL and DEFAULT tables resolve independently and both replicate', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(`CREATE TABLE test_mixed_full (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), data TEXT)`);
    await pool.query(`ALTER TABLE test_mixed_full REPLICA IDENTITY FULL`);
    await pool.query(`CREATE TABLE test_mixed_default (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), data TEXT)`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, data FROM test_mixed_full
      - SELECT id, data FROM test_mixed_default
`);
    await context.initializeReplication();

    expect((await resolvedTable(context, 'test_mixed_full'))?.storeCurrentData).toBe(false);
    expect((await resolvedTable(context, 'test_mixed_default'))?.storeCurrentData).toBe(true);

    const [{ id: fullId }] = pgwireRows(
      await pool.query(`INSERT INTO test_mixed_full (data) VALUES ('from full') RETURNING id`)
    );
    const [{ id: defaultId }] = pgwireRows(
      await pool.query(`INSERT INTO test_mixed_default (data) VALUES ('from default') RETURNING id`)
    );

    const data = await context.getBucketData('global[]');
    expect(data).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ object_id: fullId }),
        expect.objectContaining({ object_id: defaultId })
      ])
    );
  });

  test('changing REPLICA IDENTITY DEFAULT->FULL updates storeCurrentData (catalog-scan path)', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(`CREATE TABLE test_changeable (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), description TEXT)`);

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM test_changeable
`);
    await context.initializeReplication();
    expect((await resolvedTable(context, 'test_changeable'))?.storeCurrentData).toBe(true);

    // resolvedTable runs the catalog-scan path; the streaming path is covered by the test below.
    await pool.query(`ALTER TABLE test_changeable REPLICA IDENTITY FULL`);
    expect((await resolvedTable(context, 'test_changeable'))?.storeCurrentData).toBe(false);
  });

  test('mid-stream ALTER REPLICA IDENTITY DEFAULT->FULL is handled via Relation message', async () => {
    await using context = await openContext();
    const { pool } = context;
    await pool.query(
      `CREATE TABLE test_mid_stream (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), description TEXT, value INT)`
    );

    await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT id, description, value FROM test_mid_stream
`);
    await context.initializeReplication();
    // DEFAULT replica identity -> storeCurrentData=true after the initial catalog scan.
    const initial = await resolvedTable(context, 'test_mid_stream');
    expect(initial?.storeCurrentData).toBe(true);
    expect(initial?.snapshotComplete).toBe(true);

    // First INSERT triggers the Relation message (replicaIdentity='default') on the streaming path.
    const [{ id: id1 }] = pgwireRows(
      await pool.query(`INSERT INTO test_mid_stream (description, value) VALUES ('before', 1) RETURNING id`)
    );
    let data = await context.getBucketData('global[]');
    expect(data).toMatchObject([putOp('test_mid_stream', { id: id1, description: 'before', value: 1n })]);

    // pgoutput sends the new Relation message lazily, on the next DML rather than at ALTER time.
    await pool.query(`ALTER TABLE test_mid_stream REPLICA IDENTITY FULL`);

    // Next DML carries the Relation message (replicaIdentity='full'); handleRelation ->
    // resolveTables must update store_current_data without re-snapshotting the table.
    const [{ id: id2 }] = pgwireRows(
      await pool.query(`INSERT INTO test_mid_stream (description, value) VALUES ('after', 2) RETURNING id`)
    );
    await pool.query(`UPDATE test_mid_stream SET value = 20 WHERE id = '${id2}'`);
    await pool.query(`DELETE FROM test_mid_stream WHERE id = '${id1}'`);

    data = await context.getBucketData('global[]');
    // Assert the final reduced state per row, not the op sequence: a subkey-changing UPDATE emits
    // both a REMOVE and a PUT, ordered differently across storage layers.
    const lastForId1 = data.findLast((op) => op.object_id === id1);
    const lastForId2 = data.findLast((op) => op.object_id === id2);
    expect(lastForId1).toMatchObject({ op: 'REMOVE', object_type: 'test_mid_stream' });
    expect(lastForId2).toMatchObject(putOp('test_mid_stream', { id: id2, description: 'after', value: 20n }));
    // The pre-ALTER INSERT for id1 must still appear in the stream.
    expect(data).toEqual(
      expect.arrayContaining([
        expect.objectContaining(putOp('test_mid_stream', { id: id1, description: 'before', value: 1n }))
      ])
    );

    // store_current_data is now false, and snapshotComplete stays true - no re-snapshot.
    const after = await resolvedTable(context, 'test_mid_stream');
    expect(after?.storeCurrentData).toBe(false);
    expect(after?.snapshotComplete).toBe(true);
  });
}
