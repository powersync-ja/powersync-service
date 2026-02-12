import { SaveOperationTag, SourceTable } from '@powersync/service-core';
import { TablePattern } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { ConvexLSN } from '@module/common/ConvexLSN.js';
import { ConvexStream } from '@module/replication/ConvexStream.js';

function createFakeStorage(options?: {
  snapshotDone?: boolean;
  snapshotLsn?: string | null;
  resumeFromLsn?: string | null;
  tableSnapshotStatus?: {
    totalEstimatedCount?: number;
    replicatedCount?: number;
    lastKey?: Uint8Array | null;
  };
}) {
  const saves: any[] = [];
  const commits: string[] = [];
  const keepalives: string[] = [];
  const resumeLsnUpdates: string[] = [];
  const tableProgressUpdates: any[] = [];

  const table = new SourceTable({
    id: 1,
    connectionTag: 'default',
    objectId: 'users',
    schema: 'convex',
    name: 'users',
    replicaIdColumns: [{ name: '_id' }],
    snapshotComplete: false
  });
  if (options?.tableSnapshotStatus) {
    table.snapshotStatus = {
      totalEstimatedCount: options.tableSnapshotStatus.totalEstimatedCount ?? -1,
      replicatedCount: options.tableSnapshotStatus.replicatedCount ?? 0,
      lastKey: options.tableSnapshotStatus.lastKey ?? null
    };
  }

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
      tableProgressUpdates.push(progress);
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
    resumeLsnUpdates,
    tableProgressUpdates
  };
}

describe('ConvexStream', () => {
  it('pins a global snapshot boundary before table hydration', async () => {
    const context = createFakeStorage();
    const abortController = new AbortController();
    const snapshotCalls: any[] = [];
    const listSnapshot = vi.fn(async (options: any) => {
      snapshotCalls.push(options ?? {});
      if (options?.tableName == null) {
        return {
          snapshot: '100',
          cursor: null,
          hasMore: false,
          values: []
        };
      }

      return {
        snapshot: '100',
        cursor: null,
        hasMore: false,
        values: [{ _table: 'users', _id: 'u1', name: 'Alice' }]
      };
    });

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
          listSnapshot,
          getGlobalSnapshotCursor: async (options?: any) => (await listSnapshot(options)).snapshot
        }
      } as any
    });

    await stream.initReplication();

    expect(snapshotCalls.length).toBe(2);
    expect(snapshotCalls[0]?.tableName).toBeUndefined();
    expect(snapshotCalls[0]?.cursor).toBeUndefined();
    expect(snapshotCalls[1]?.tableName).toBe('users');
    expect(snapshotCalls[1]?.cursor).toBeUndefined();
    expect(snapshotCalls[1]?.snapshot).toBe('100');
    expect(context.saves.length).toBe(1);
    expect(context.saves[0]?.tag).toBe(SaveOperationTag.INSERT);
    expect(context.resumeLsnUpdates.length).toBe(1);
    expect(context.commits.at(-1)).toBe(ConvexLSN.fromCursor('100').comparable);
  });

  it('starts each table snapshot from first page, then paginates within the run', async () => {
    const context = createFakeStorage({
      snapshotLsn: ConvexLSN.fromCursor('200').comparable,
      tableSnapshotStatus: {
        replicatedCount: 99,
        totalEstimatedCount: -1,
        lastKey: Buffer.from('stale-cursor', 'utf8')
      }
    });
    const abortController = new AbortController();

    const snapshotCalls: any[] = [];
    const listSnapshot = vi.fn(async (options: any) => {
      snapshotCalls.push(options ?? {});
      if (snapshotCalls.length == 1) {
        return {
          snapshot: '200',
          cursor: 'page-2',
          hasMore: true,
          values: [{ _table: 'users', _id: 'u1', name: 'Alice' }]
        };
      }
      return {
        snapshot: '200',
        cursor: null,
        hasMore: false,
        values: [{ _table: 'users', _id: 'u2', name: 'Bob' }]
      };
    });

    const getGlobalSnapshotCursor = vi.fn(async () => 'should-not-be-called');

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
          listSnapshot,
          getGlobalSnapshotCursor
        }
      } as any
    });

    await stream.initReplication();

    expect(getGlobalSnapshotCursor).not.toHaveBeenCalled();
    expect(snapshotCalls.length).toBe(2);
    expect(snapshotCalls[0]?.snapshot).toBe('200');
    expect(snapshotCalls[0]?.cursor).toBeUndefined();
    expect(snapshotCalls[1]?.cursor).toBe('page-2');
    expect(context.tableProgressUpdates[0]).toMatchObject({
      replicatedCount: 0,
      lastKey: null,
      totalEstimatedCount: -1
    });
  });

  it('fails when table snapshots return a different snapshot boundary', async () => {
    const context = createFakeStorage({
      snapshotLsn: ConvexLSN.fromCursor('300').comparable
    });
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
          getGlobalSnapshotCursor: async () => 'should-not-be-called',
          listSnapshot: async () => ({
            snapshot: '301',
            cursor: null,
            hasMore: false,
            values: [{ _table: 'users', _id: 'u1', name: 'Alice' }]
          })
        }
      } as any
    });

    await expect(stream.initReplication()).rejects.toThrow(/snapshot cursor changed while snapshotting/);
  });

  it('streams deltas and commits checkpoint', async () => {
    const context = createFakeStorage({
      snapshotDone: true,
      resumeFromLsn: ConvexLSN.fromCursor('100').comparable
    });
    const abortController = new AbortController();

    let calls = 0;
    const deltaCalls: any[] = [];

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
          documentDeltas: async (options: any) => {
            calls += 1;
            deltaCalls.push(options ?? {});
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
    expect(deltaCalls[0]?.tableName).toBeUndefined();
  });

  it('keeps alive immediately when only checkpoint marker rows are streamed', async () => {
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
            if (calls == 1) {
              return {
                cursor: '101',
                hasMore: true,
                values: []
              };
            }

            setTimeout(() => abortController.abort(), 0);
            return {
              cursor: '102',
              hasMore: false,
              values: [{ _table: 'source_powersync_checkpoints', _id: 'cp1' }]
            };
          }
        }
      } as any
    });

    await stream.streamChanges();

    expect(context.saves.length).toBe(0);
    expect(context.commits.length).toBe(0);
    expect(context.keepalives).toEqual([
      ConvexLSN.fromCursor('101').comparable,
      ConvexLSN.fromCursor('102').comparable
    ]);
  });
});
