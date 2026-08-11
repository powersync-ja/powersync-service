import { CaptureInstance } from '@module/common/CaptureInstance.js';
import { LSN } from '@module/common/LSN.js';
import { MSSQLSourceTable } from '@module/common/MSSQLSourceTable.js';
import { CDCPoller } from '@module/replication/CDCPoller.js';
import {
  createCaptureReconciler,
  MSSQLSourceMetadata,
  readCaptureMetadata
} from '@module/replication/CaptureReconciler.js';
import type { MSSQLTableReconciliationContext } from '@module/replication/MSSQLTableReconciliationContext.js';
import { MSSQLTableReconciliationState } from '@module/replication/MSSQLTableReconciliationContext.js';
import { SourceEntityDescriptor, SourceTable } from '@powersync/service-core';
import { describe, expect, it, vi } from 'vitest';

/**
 * Build a source descriptor with optional identity overrides.
 */
function source(overrides: Partial<SourceEntityDescriptor> = {}): SourceEntityDescriptor {
  return {
    connectionTag: 'default',
    schema: 'dbo',
    name: 'users',
    objectId: 100,
    replicaIdColumns: [{ name: 'id', type: 'int', typeId: 56 }],
    ...overrides
  };
}

/**
 * Build a persisted source-table candidate with optional capture metadata.
 */
function candidate(
  id: string,
  metadata?: MSSQLSourceMetadata,
  overrides: Partial<ConstructorParameters<typeof SourceTable>[0]> = {}
): SourceTable {
  return new SourceTable({
    id,
    ref: { connectionTag: 'default', schema: 'dbo', name: 'users' },
    objectId: 100,
    replicaIdColumns: [{ name: 'id', type: 'int', typeId: 56 }],
    snapshotComplete: true,
    bucketDataSources: [],
    parameterLookupSources: [],
    sourceMetadata: metadata,
    ...overrides
  });
}

/**
 * Build a capture instance for the given capture-table object id.
 */
function instance(objectId: number): CaptureInstance {
  return {
    name: `dbo_users_${objectId}`,
    objectId,
    minLSN: LSN.fromString(LSN.ZERO),
    createDate: new Date(),
    pendingSchemaChanges: []
  };
}

function readyContext(
  captureInstances: CaptureInstance[],
  sourceDescriptor: SourceEntityDescriptor = source()
): MSSQLTableReconciliationContext {
  return {
    state: MSSQLTableReconciliationState.READY,
    source: sourceDescriptor,
    captureInstances
  };
}

function unavailableContext(
  state: MSSQLTableReconciliationState.TABLE_MISSING | MSSQLTableReconciliationState.CDC_DISABLED,
  sourceDescriptor: SourceEntityDescriptor = source()
): MSSQLTableReconciliationContext {
  return { state, source: sourceDescriptor };
}

function reconcile(context: MSSQLTableReconciliationContext, candidates: SourceTable[]) {
  return createCaptureReconciler(context)({ source: context.source, candidates });
}

describe('readCaptureMetadata', () => {
  it('parses a valid metadata object', () => {
    expect(readCaptureMetadata({ captureTableObjectId: 7 })).toEqual({ captureTableObjectId: 7 });
  });

  it('returns null for legacy metadata', () => {
    expect(readCaptureMetadata(null)).toBeNull();
  });

  it('rejects malformed metadata', () => {
    expect(() => readCaptureMetadata([1, 2] as any)).toThrow();
    expect(() => readCaptureMetadata({ foo: 1 } as any)).toThrow();
  });
});

describe('createCaptureReconciler', () => {
  it('pins a new binding to the newest available capture instance', () => {
    const resolution = reconcile(readyContext([instance(50), instance(40)]), []);
    expect(resolution.compatibleTables).toEqual([]);
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 50 } });
  });

  it('reports a missing table with no persisted binding as not ready', () => {
    expect(() =>
      reconcile(
        unavailableContext(
          MSSQLTableReconciliationState.TABLE_MISSING,
          source({ objectId: undefined, replicaIdColumns: [] })
        ),
        []
      )
    ).toThrow(/does not exist/);
  });

  it('reports a missing table with a persisted binding as unavailable', () => {
    expect(() =>
      reconcile(
        unavailableContext(
          MSSQLTableReconciliationState.TABLE_MISSING,
          source({ objectId: undefined, replicaIdColumns: [] })
        ),
        [candidate('old', { captureTableObjectId: 40 })]
      )
    ).toThrow(/no longer matches the source table binding/);
  });

  it('fails a new binding when CDC is disabled', () => {
    expect(() => reconcile(unavailableContext(MSSQLTableReconciliationState.CDC_DISABLED), [])).toThrow(
      /CDC is not enabled/
    );
  });

  it('updates legacy metadata-free candidates to the newest capture instance', () => {
    const resolution = reconcile(readyContext([instance(50)]), [candidate('a'), candidate('b')]);
    expect(
      resolution.compatibleTables.map((table) => ({ id: table.id, sourceMetadata: table.sourceMetadata }))
    ).toEqual([
      { id: 'a', sourceMetadata: { captureTableObjectId: 50 } },
      { id: 'b', sourceMetadata: { captureTableObjectId: 50 } }
    ]);
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 50 } });
  });

  it('fails an existing binding when CDC is disabled', () => {
    expect(() => reconcile(unavailableContext(MSSQLTableReconciliationState.CDC_DISABLED), [candidate('a')])).toThrow(
      /CDC is no longer enabled/
    );
  });

  it('reports a changed source identity as unavailable when CDC is disabled', () => {
    const changedSource = source({ objectId: 200 });
    expect(() =>
      reconcile(unavailableContext(MSSQLTableReconciliationState.CDC_DISABLED, changedSource), [candidate('old')])
    ).toThrow(/no longer matches the source table binding/);
  });

  it('preserves a pinned capture identity that is still available', () => {
    const resolution = reconcile(readyContext([instance(50), instance(40)]), [
      candidate('a', { captureTableObjectId: 40 })
    ]);
    expect(resolution.compatibleTables.map((table) => table.id)).toEqual(['a']);
    expect(resolution.compatibleTables[0].sourceMetadata).toEqual({ captureTableObjectId: 40 });
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 40 } });
  });

  it('fails when the pinned capture instance was dropped, even with a replacement available', () => {
    expect(() => reconcile(readyContext([instance(50)]), [candidate('a', { captureTableObjectId: 40 })])).toThrow(
      /no longer available/
    );
  });

  it('fails on a mixture of metadata-free and pinned candidates', () => {
    expect(() =>
      reconcile(readyContext([instance(40)]), [candidate('a'), candidate('b', { captureTableObjectId: 40 })])
    ).toThrow(/mixture/);
  });

  it('fails on multiple distinct pinned identities', () => {
    expect(() =>
      reconcile(readyContext([instance(40), instance(41)]), [
        candidate('a', { captureTableObjectId: 40 }),
        candidate('b', { captureTableObjectId: 41 })
      ])
    ).toThrow(/multiple persisted capture identities/);
  });

  it('does not replace an existing binding when the source identity changed', () => {
    const changedSource = source({ objectId: 200 });
    expect(() =>
      reconcile(readyContext([instance(50)], changedSource), [candidate('old', { captureTableObjectId: 40 })])
    ).toThrow(
      /Table \[dbo\]\.\[users\] no longer matches the source table binding.*already-replicated data is retained/
    );
  });

  it('drops stale incompatible candidates when a compatible candidate anchors the binding', () => {
    const resolution = reconcile(readyContext([instance(50)]), [
      candidate('a'),
      candidate('mismatch', undefined, {
        replicaIdColumns: [{ name: 'id', type: 'bigint', typeId: 127 }]
      })
    ]);
    expect(
      resolution.compatibleTables.map((table) => ({ id: table.id, sourceMetadata: table.sourceMetadata }))
    ).toEqual([{ id: 'a', sourceMetadata: { captureTableObjectId: 50 } }]);
    expect(resolution.incompatibleTables.map((table) => table.id)).toEqual(['mismatch']);
  });
});

describe('MSSQLSourceTable.setCaptureInstance', () => {
  it('sets the instance matching the persisted capture-table object id', () => {
    const table = new MSSQLSourceTable(source(), [candidate('a', { captureTableObjectId: 40 })]);
    const expected = instance(40);

    table.setCaptureInstance([instance(50), expected]);

    expect(table.captureInstance).toBe(expected);
  });

  it('sets null when the binding is legacy or the pinned instance is unavailable', () => {
    const legacy = new MSSQLSourceTable(source(), [candidate('legacy')]);
    const pinned = new MSSQLSourceTable(source(), [candidate('pinned', { captureTableObjectId: 40 })]);

    legacy.setCaptureInstance([instance(40)]);
    pinned.setCaptureInstance([instance(40)]);
    pinned.setCaptureInstance([instance(50)]);

    expect(legacy.captureInstance).toBeNull();
    expect(pinned.captureInstance).toBeNull();
  });
});

describe('CDCPoller capture-instance metadata', () => {
  it('refreshes the bound instance to use its latest metadata', async () => {
    const persisted = candidate('a', { captureTableObjectId: 40 });
    const table = new MSSQLSourceTable(source(), [persisted]);
    const startupInstance = instance(40);
    table.setCaptureInstance([startupInstance]);

    const refreshed = instance(40);
    refreshed.minLSN = LSN.fromString('00000000:00000002:0000');
    const query = vi.fn().mockResolvedValue({ recordset: [] });
    const poller = new CDCPoller({
      connectionManager: { query } as any,
      eventHandler: {} as any,
      getReplicatedTables: () => [table],
      startLSN: LSN.fromString(LSN.ZERO),
      additionalConfig: { pollingIntervalMs: 1, pollingBatchSize: 1, trustServerCertificate: false }
    });
    (poller as any).captureInstances = new Map([
      [
        100,
        {
          sourceTable: { schema: 'dbo', name: 'users', objectId: 100 },
          instances: [refreshed]
        }
      ]
    ]);

    const bounds = {
      startLSN: LSN.fromString('00000000:00000001:0000'),
      endLSN: LSN.fromString('00000000:00000003:0000')
    };
    await (poller as any).pollTable(table, bounds);

    expect(bounds.startLSN).toBe(refreshed.minLSN);
    expect(table.captureInstance).toBe(refreshed);
    expect(table.pinnedCaptureObjectId).toBe(40);
  });
});
