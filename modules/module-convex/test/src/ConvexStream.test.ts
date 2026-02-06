import { SaveOperationTag, SourceTable } from '@powersync/service-core';
import { TablePattern } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { ConvexLSN } from '@module/common/ConvexLSN.js';
import { ConvexStream } from '@module/replication/ConvexStream.js';

function createFakeStorage(options?: {
  snapshotDone?: boolean;
  snapshotLsn?: string | null;
  resumeFromLsn?: string | null;
}) {
  const saves: any[] = [];
  const commits: string[] = [];
  const keepalives: string[] = [];
  const resumeLsnUpdates: string[] = [];

  const table = new SourceTable({
    id: 1,
    connectionTag: 'default',
    objectId: 'users',
    schema: 'convex',
    name: 'users',
    replicaIdColumns: [{ name: '_id' }],
    snapshotComplete: false
  });

  const batch: any = {
    lastCheckpointLsn: null,
    resumeFromLsn: options?.resumeFromLsn ?? null,
    noCheckpointBeforeLsn: ConvexLSN.ZERO.comparable,
    async save(record: any) {
      saves.push(record);
      return null;
    },
    async truncate(_tables: SourceTable[]) {
      return null;
    },
    async drop(_tables: SourceTable[]) {
      return null;
    },
    async flush() {
      return null;
    },
    async commit(lsn: string) {
      commits.push(lsn);
      this.lastCheckpointLsn = lsn;
      return true;
    },
    async keepalive(lsn: string) {
      keepalives.push(lsn);
      this.lastCheckpointLsn = lsn;
      return true;
    },
    async setResumeLsn(lsn: string) {
      resumeLsnUpdates.push(lsn);
      this.resumeFromLsn = lsn;
    },
    async markSnapshotDone(tables: SourceTable[], _lsn: string) {
      for (const sourceTable of tables) {
        sourceTable.snapshotComplete = true;
      }
      return tables;
    },
    async updateTableProgress(sourceTable: SourceTable, progress: any) {
      sourceTable.snapshotStatus = {
        totalEstimatedCount: progress.totalEstimatedCount ?? sourceTable.snapshotStatus?.totalEstimatedCount ?? -1,
        replicatedCount: progress.replicatedCount ?? sourceTable.snapshotStatus?.replicatedCount ?? 0,
        lastKey: progress.lastKey ?? sourceTable.snapshotStatus?.lastKey ?? null
      };
      return sourceTable;
    }
  };

  const storage = {
    group_id: 1,
    getParsedSyncRules: () => ({
      getSourceTables: () => [new TablePattern('convex', 'users')],
      applyRowContext: (row: Record<string, unknown>) => row
    }),
    async getStatus() {
      return {
        active: true,
        snapshot_done: options?.snapshotDone ?? false,
        checkpoint_lsn: options?.snapshotDone ? ConvexLSN.fromCursor('100').comparable : null,
        snapshot_lsn: options?.snapshotLsn ?? null
      };
    },
    clear: vi.fn(async () => undefined),
    populatePersistentChecksumCache: vi.fn(async () => ({ buckets: 0 })),
    resolveTable: vi.fn(async () => ({
      table,
      dropTables: []
    })),
    startBatch: vi.fn(async (_options: any, callback: (batch: any) => Promise<void>) => {
      await callback(batch);
      return { flushed_op: 1n };
    }),
    reportError: vi.fn(async () => undefined)
  };

  return {
    storage,
    batch,
    table,
    saves,
    commits,
    keepalives,
    resumeLsnUpdates
  };
}

describe('ConvexStream', () => {
  it('runs initial snapshot and stores resume LSN', async () => {
    const context = createFakeStorage();
    const abortController = new AbortController();

    const stream = new ConvexStream({
      abortSignal: abortController.signal,
      storage: context.storage as any,
      metrics: {
        getCounter: () => ({ add: () => {} })
      } as any,
      connections: {
        schema: 'convex',
        connectionTag: 'default',
        connectionId: '1',
        config: { pollingIntervalMs: 1 },
        client: {
          getJsonSchemas: async () => ({
            tables: [{ tableName: 'users', schema: {} }],
            raw: {}
          }),
          listSnapshot: async () => ({
            snapshot: '100',
            cursor: null,
            hasMore: false,
            values: [{ _table: 'users', _id: 'u1', name: 'Alice' }]
          })
        }
      } as any
    });

    await stream.initReplication();

    expect(context.saves.length).toBe(1);
    expect(context.saves[0]?.tag).toBe(SaveOperationTag.INSERT);
    expect(context.resumeLsnUpdates.length).toBe(1);
    expect(context.commits.at(-1)).toBe(ConvexLSN.fromCursor('100').comparable);
  });

  it('streams deltas and commits checkpoint', async () => {
    const context = createFakeStorage({
      snapshotDone: true,
      resumeFromLsn: ConvexLSN.fromCursor('100').comparable
    });
    const abortController = new AbortController();

    let calls = 0;

    const stream = new ConvexStream({
      abortSignal: abortController.signal,
      storage: context.storage as any,
      metrics: {
        getCounter: () => ({ add: () => {} })
      } as any,
      connections: {
        schema: 'convex',
        connectionTag: 'default',
        connectionId: '1',
        config: { pollingIntervalMs: 1 },
        client: {
          getJsonSchemas: async () => ({
            tables: [{ tableName: 'users', schema: {} }],
            raw: {}
          }),
          documentDeltas: async () => {
            calls += 1;
            setTimeout(() => abortController.abort(), 0);
            return {
              cursor: '101',
              hasMore: false,
              values: [
                { _table: 'users', _id: 'u1', name: 'Updated' },
                { _table: 'users', _id: 'u2', _deleted: true }
              ]
            };
          }
        }
      } as any
    });

    await stream.streamChanges();

    expect(calls).toBeGreaterThan(0);
    expect(context.saves.length).toBe(2);
    expect(context.saves[0]?.tag).toBe(SaveOperationTag.UPDATE);
    expect(context.saves[1]?.tag).toBe(SaveOperationTag.DELETE);
    expect(context.commits.at(-1)).toBe(ConvexLSN.fromCursor('101').comparable);
  });
});
