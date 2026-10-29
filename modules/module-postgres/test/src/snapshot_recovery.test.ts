import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { describe, expect, test, vi } from 'vitest';
import { env } from './env.js';
import { INITIALIZED_POSTGRES_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

const SYNC_CONFIG = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    synced:
      query: SELECT * FROM test_snapshot_recovery_synced
`;

describe.skipIf(!env.TEST_POSTGRES_STORAGE)('Postgres snapshot recovery with unused publication tables', () => {
  test.each([1, 2])('v%i recovers without resolving an unused table during the snapshot', async (storageVersion) => {
    {
      let unusedWritten = false;
      await using context: WalStreamTestContext = await WalStreamTestContext.open(
        INITIALIZED_POSTGRES_STORAGE_FACTORY.factory,
        {
          storageVersion,
          walStreamOptions: {
            onSnapshotChunkFlushed: async () => {
              if (!unusedWritten) {
                await context.pool.query("INSERT INTO test_snapshot_recovery_unused VALUES ('during-snapshot')");
                unusedWritten = true;
              }
            }
          }
        }
      );
      await context.updateSyncRules(SYNC_CONFIG);
      await context.pool.query(`
        CREATE TABLE test_snapshot_recovery_synced (id TEXT PRIMARY KEY);
        CREATE TABLE test_snapshot_recovery_unused (id TEXT PRIMARY KEY);
        INSERT INTO test_snapshot_recovery_synced VALUES ('before-restart');
      `);

      // Process a real WAL insert, then stop before committing the first checkpoint.
      const interruption = new Error('Stop after processing the unused insert');
      const writeChange = context.walStream.writeChange.bind(context.walStream);
      vi.spyOn(context.walStream, 'writeChange').mockImplementation(async (batch, msg) => {
        const result = await writeChange(batch, msg);
        if (msg.tag === 'insert' && msg.relation.name === 'test_snapshot_recovery_unused') {
          throw interruption;
        }
        return result;
      });

      await expect(async () => {
        await context.replicateSnapshot();
        await context.getCheckpoint();
      }).rejects.toBe(interruption);
      expect((await context.storage!.getStatus()).snapshotDone).toBe(false);
    }

    await using context = await WalStreamTestContext.open(INITIALIZED_POSTGRES_STORAGE_FACTORY.factory, {
      doNotClear: true,
      storageVersion
    });
    await context.loadNextSyncRules();
    const relations = vi.spyOn(context.walStream, 'handleRelation');
    await context.initializeReplication();
    expect(
      relations.mock.calls.filter(([options]) => !options.snapshot).map(([options]) => options.descriptor.name)
    ).toEqual(['test_snapshot_recovery_synced']);
    expect((await context.storage!.getStatus()).snapshotDone).toBe(true);
    expect((await context.factory.getActiveSyncConfig())?.replicationStream.replicationStreamId).toBe(
      context.storage!.replicationStreamId
    );

    const factory = context.factory as postgres_storage.PostgresBucketStorageFactory;
    const pending = await factory.db.sql`
      SELECT snapshot_done FROM source_tables
      WHERE group_id = ${{ type: 'int4', value: context.storage!.replicationStreamId }}
        AND table_name = 'test_snapshot_recovery_unused'
    `.first<{ snapshot_done: boolean }>();
    expect(pending!.snapshot_done).toBe(false);

    await context.pool.query("INSERT INTO test_snapshot_recovery_synced VALUES ('after-restart')");
    await context.getCheckpoint();
  });
});
