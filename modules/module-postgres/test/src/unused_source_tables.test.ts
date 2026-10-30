import { reduceBucket } from '@powersync/service-core-tests';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { describe, expect, test, vi } from 'vitest';
import { describeWithStorage } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

const SYNC_CONFIG = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    synced:
      query: SELECT * FROM test_unused_synced
`;

async function storedTableNames(context: WalStreamTestContext) {
  const factory = context.factory;
  const streamId = context.storage!.replicationStreamId;
  if (factory instanceof postgres_storage.PostgresBucketStorageFactory) {
    const rows = await factory.db.sql`
      SELECT table_name FROM source_tables WHERE group_id = ${{ type: 'int4', value: streamId }}
    `.rows<{ table_name: string }>();
    return rows.map((row) => row.table_name).sort();
  }
  if (factory instanceof mongo_storage.MongoBucketStorage) {
    const rows = await factory.db.source_tables.find({ group_id: streamId }).toArray();
    return rows.map((row) => row.table_name).sort();
  }
  throw new Error('Unexpected storage backend');
}

describe('Postgres unused publication tables', { timeout: 30_000 }, () => {
  describeWithStorage({ storageVersions: [1, 2] }, ({ factory, storageVersion }) => {
    const open = (options: Parameters<typeof WalStreamTestContext.open>[1] = {}) =>
      WalStreamTestContext.open(factory, { ...options, storageVersion });

    test('caches ignored relations and reconciles renames into and out of scope', async () => {
      await using context = await open();
      await context.updateSyncRules(SYNC_CONFIG);
      await context.pool.query(`
        CREATE TABLE test_unused_ignored (id TEXT PRIMARY KEY);
        INSERT INTO test_unused_ignored VALUES ('existing');
      `);
      await context.initializeReplication();
      const handle = vi.spyOn(context.walStream, 'handleRelation');
      await context.pool.query("INSERT INTO test_unused_ignored VALUES ('ignored-first')");
      await context.getCheckpoint();
      const relationCount = handle.mock.calls.length;
      expect(relationCount).toBeGreaterThan(0);
      await context.pool.query("INSERT INTO test_unused_ignored VALUES ('ignored-second')");
      await context.getCheckpoint();
      expect(handle.mock.calls.length).toBe(relationCount);
      expect(await storedTableNames(context)).toEqual([]);

      await context.pool.query(`
        ALTER TABLE test_unused_ignored RENAME TO test_unused_synced;
        INSERT INTO test_unused_synced VALUES ('after-rename');
      `);
      const data = reduceBucket(await context.getBucketData('synced|0[]')).filter((op) => op.op === 'PUT');
      expect(data.map((op) => op.object_id).sort()).toEqual([
        'after-rename',
        'existing',
        'ignored-first',
        'ignored-second'
      ]);
      expect(await storedTableNames(context)).toEqual(['test_unused_synced']);

      await context.pool.query(`
        ALTER TABLE test_unused_synced RENAME TO test_unused_ignored;
        INSERT INTO test_unused_ignored VALUES ('after-removal');
      `);
      const remaining = reduceBucket(await context.getBucketData('synced|0[]')).filter((op) => op.op === 'PUT');
      expect(remaining).toEqual([]);
      expect(await storedTableNames(context)).toEqual([]);
    });
  });
});
