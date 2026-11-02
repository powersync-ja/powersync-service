import { describe, expect, test, vi } from 'vitest';
import { describeWithStorage } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

const SYNC_CONFIG = /* yaml */ `
  # sync config fixture.
  config:
    edition: 3
  streams:
    synced:
      query: SELECT * FROM test_snapshot_recovery_synced
`;

describe('Postgres snapshot restart with unused publication tables', () => {
  describeWithStorage({ storageVersions: [1, 2] }, ({ factory, storageVersion }) => {
    test('ignores unused relations without leaving snapshot blockers after restart', async () => {
      {
        let unusedWritten = false;
        await using context: WalStreamTestContext = await WalStreamTestContext.open(factory, {
          storageVersion,
          walStreamOptions: {
            onSnapshotChunkFlushed: async () => {
              if (!unusedWritten) {
                await context.pool.query("INSERT INTO test_snapshot_recovery_unused VALUES ('during-snapshot')");
                unusedWritten = true;
              }
            }
          }
        });
        await context.updateSyncRules(SYNC_CONFIG);
        await context.pool.query(`
        CREATE TABLE test_snapshot_recovery_synced (id TEXT PRIMARY KEY);
        CREATE TABLE test_snapshot_recovery_unused (id TEXT PRIMARY KEY);
        INSERT INTO test_snapshot_recovery_synced VALUES ('before-restart');
      `);

        let unusedResolved = false;
        const handleRelation = context.walStream.handleRelation.bind(context.walStream);
        vi.spyOn(context.walStream, 'handleRelation').mockImplementation(async (options) => {
          const tables = await handleRelation(options);
          if (options.descriptor.name === 'test_snapshot_recovery_unused') {
            expect(tables).toEqual([]);
            unusedResolved = true;
          }
          return tables;
        });

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
        expect(unusedResolved).toBe(true);
        expect((await context.storage!.getStatus()).snapshotDone).toBe(false);
      }

      await using context = await WalStreamTestContext.open(factory, {
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

      await context.pool.query("INSERT INTO test_snapshot_recovery_synced VALUES ('after-restart')");
      await context.getCheckpoint();
    });
  });
});
