import { SaveOperationTag, SourceTable } from '@powersync/service-core';
import { TablePattern } from '@powersync/service-sync-rules';
import { describe, expect, it, vi } from 'vitest';
import { toConvexLsn, ZERO_LSN } from '@module/common/ConvexLSN.js';
import { ConvexStream } from '@module/replication/ConvexStream.js';

function createFakeStorage(options?: {
  snapshotDone?: boolean;
  snapshotLsn?: string | null;
  resumeFromLsn?: string | null;
  sourcePatterns?: TablePattern[];
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

  const tables = new Map<string, SourceTable>();
  let nextTableId = 1;
  const getOrCreateTable = (
    name: string,
    tableOptions?: {
      snapshotStatus?: {
        totalEstimatedCount?: number;
        replicatedCount?: number;
        lastKey?: Uint8Array | null;
      };
      snapshotComplete?: boolean;
    }
  ) => {
    const existing = tables.get(name);
    if (existing != null) {
      return existing;
    }

    const table = new SourceTable({
      id: nextTableId++,
      connectionTag: 'default',
      objectId: name,
      schema: 'convex',
      name,
      replicaIdColumns: [{ name: '_id' }],
      snapshotComplete: tableOptions?.snapshotComplete ?? false
    });
    if (tableOptions?.snapshotStatus) {
      table.snapshotStatus = {
        totalEstimatedCount: tableOptions.snapshotStatus.totalEstimatedCount ?? -1,
        replicatedCount: tableOptions.snapshotStatus.replicatedCount ?? 0,
        lastKey: tableOptions.snapshotStatus.lastKey ?? null
      };
    }

    tables.set(name, table);
    return table;
  };

  const table = getOrCreateTable('users', {
    snapshotStatus: options?.tableSnapshotStatus
  });

  const batch: any = {
    lastCheckpointLsn: null,
    resumeFromLsn: options?.resumeFromLsn ?? null,
    noCheckpointBeforeLsn: ZERO_LSN,
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
      tableProgressUpdates.push({
        tableName: sourceTable.name,
        ...progress
      });
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
      getSourceTables: () => options?.sourcePatterns ?? [new TablePattern('convex', 'users')],
      applyRowContext: (row: Record<string, unknown>) => row
    }),
    async getStatus() {
      return {
        active: true,
        snapshot_done: options?.snapshotDone ?? false,
        checkpoint_lsn: options?.snapshotDone ? toConvexLsn('100') : null,
        snapshot_lsn: options?.snapshotLsn ?? null
      };
    },
    clear: vi.fn(async () => undefined),
    populatePersistentChecksumCache: vi.fn(async () => ({ buckets: 0 })),
    resolveTable: vi.fn(async ({ entity_descriptor }: any) => ({
      table: getOrCreateTable(entity_descriptor.name),
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
    tables,
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
    expect(context.commits.at(-1)).toBe(toConvexLsn('100'));
  });

  it('decodes bytes fields to Uint8Array during snapshot hydration', async () => {
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
            tables: [
              {
                tableName: 'users',
                schema: {
                  properties: {
                    avatar: { type: 'bytes' }
                  }
                }
              }
            ],
            raw: {}
          }),
          listSnapshot: async (options: any) => {
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
              values: [{ _table: 'users', _id: 'u1', avatar: 'AQID' }]
            };
          },
          getGlobalSnapshotCursor: async () => '100'
        }
      } as any
    });

    await stream.initReplication();

    expect(context.saves).toHaveLength(1);
    expect(context.saves[0]?.after.avatar).toEqual(Uint8Array.of(1, 2, 3));
  });

  it('resumes table snapshots from the persisted page cursor', async () => {
    const context = createFakeStorage({
      snapshotLsn: toConvexLsn('200'),
      tableSnapshotStatus: {
        replicatedCount: 1,
        totalEstimatedCount: -1,
        lastKey: Buffer.from('page-2', 'utf8')
      }
    });
    const abortController = new AbortController();

    const snapshotCalls: any[] = [];
    const listSnapshot = vi.fn(async (options: any) => {
      snapshotCalls.push(options ?? {});
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
    expect(snapshotCalls.length).toBe(1);
    expect(snapshotCalls[0]?.snapshot).toBe('200');
    expect(snapshotCalls[0]?.cursor).toBe('page-2');
    expect(context.saves.length).toBe(1);
    expect(context.tableProgressUpdates).toHaveLength(1);
    expect(context.tableProgressUpdates[0]?.replicatedCount).toBe(2);
  });

  it('marks snapshot done without re-reading rows when the final page was already flushed', async () => {
    const context = createFakeStorage({
      snapshotLsn: toConvexLsn('200'),
      tableSnapshotStatus: {
        replicatedCount: 2,
        totalEstimatedCount: -1,
        lastKey: Buffer.from('convex-snapshot-progress:{"cursor":null,"finished":true}', 'utf8')
      }
    });
    const abortController = new AbortController();
    const listSnapshot = vi.fn(async () => ({
      snapshot: '200',
      cursor: null,
      hasMore: false,
      values: []
    }));

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
          getGlobalSnapshotCursor: async () => 'should-not-be-called'
        }
      } as any
    });

    await stream.initReplication();

    expect(listSnapshot).not.toHaveBeenCalled();
    expect(context.saves.length).toBe(0);
    expect(context.table.snapshotComplete).toBe(true);
  });

  it('fails when table snapshots return a different snapshot boundary', async () => {
    const context = createFakeStorage({
      snapshotLsn: toConvexLsn('300')
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
      resumeFromLsn: toConvexLsn('100')
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
    expect(context.commits.at(-1)).toBe(toConvexLsn('101'));
    expect(deltaCalls[0]?.tableName).toBeUndefined();
  });

  it('refreshes metadata before snapshotting a newly discovered wildcard-matched table inline', async () => {
    const context = createFakeStorage({
      snapshotDone: true,
      resumeFromLsn: toConvexLsn('100'),
      sourcePatterns: [new TablePattern('convex', 'projects%')]
    });
    const abortController = new AbortController();

    let calls = 0;
    const getJsonSchemas = vi.fn(async () => ({
      tables: [{ tableName: 'users', schema: {} }],
      raw: {}
    }));
    const listSnapshot = vi.fn(async (options: any) => ({
      snapshot: '101',
      cursor: null,
      hasMore: false,
      values: [{ _table: 'projects_archive', _id: 'p1', name: 'From snapshot' }]
    }));

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
          getJsonSchemas,
          listSnapshot,
          documentDeltas: async () => {
            calls += 1;
            setTimeout(() => abortController.abort(), 0);
            return {
              cursor: '101',
              hasMore: false,
              values: [{ _table: 'projects_archive', _id: 'p1', name: 'From delta' }]
            };
          }
        }
      } as any
    });

    await stream.streamChanges();

    expect(calls).toBeGreaterThan(0);
    expect(getJsonSchemas).toHaveBeenCalledTimes(2);
    expect(listSnapshot).toHaveBeenCalledTimes(1);
    expect(listSnapshot).toHaveBeenCalledWith({
      tableName: 'projects_archive',
      snapshot: '101',
      cursor: undefined,
      signal: abortController.signal
    });
    expect(context.saves.length).toBe(1);
    expect(context.saves[0]?.tag).toBe(SaveOperationTag.INSERT);
    expect(context.saves[0]?.sourceTable.name).toBe('projects_archive');
    expect(context.tables.get('projects_archive')?.snapshotComplete).toBe(true);
  });

  it('keeps alive immediately when only checkpoint marker rows are streamed', async () => {
    const context = createFakeStorage({
      snapshotDone: true,
      resumeFromLsn: toConvexLsn('100')
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
              values: [{ _table: 'powersync_checkpoints', _id: 'cp1' }]
            };
          }
        }
      } as any
    });

    await stream.streamChanges();

    expect(context.saves.length).toBe(0);
    expect(context.commits.length).toBe(0);
    expect(context.keepalives).toEqual([
      toConvexLsn('101'),
      toConvexLsn('102')
    ]);
  });
});
