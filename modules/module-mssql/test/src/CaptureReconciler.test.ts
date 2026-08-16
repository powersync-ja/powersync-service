import { CaptureInstance } from '@module/common/CaptureInstance.js';
import { createCaptureReconciler, readCaptureMetadata } from '@module/replication/CaptureReconciler.js';
import type { MSSQLTableReconciliationContext } from '@module/replication/MSSQLTableReconciliationContext.js';
import { MSSQLTableReconciliationState } from '@module/replication/MSSQLTableReconciliationContext.js';
import { SourceEntityDescriptor, SourceTable } from '@powersync/service-core';
import { describe, expect, it } from 'vitest';
import { createCaptureInstance, createSourceDescriptor, createSourceTableCandidate } from './util.js';

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
    const resolution = reconcile(readyContext([createCaptureInstance(50), createCaptureInstance(40)]), []);
    expect(resolution.compatibleTables).toEqual([]);
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 50 } });
  });

  it('reports a missing table with no persisted binding as not ready', () => {
    expect(() =>
      reconcile(
        unavailableContext(
          MSSQLTableReconciliationState.TABLE_MISSING,
          createSourceDescriptor({ objectId: undefined, replicaIdColumns: [] })
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
          createSourceDescriptor({ objectId: undefined, replicaIdColumns: [] })
        ),
        [createSourceTableCandidate('old', { captureTableObjectId: 40 })]
      )
    ).toThrow(/no longer matches the source table binding/);
  });

  it('fails a new binding when CDC is disabled', () => {
    expect(() => reconcile(unavailableContext(MSSQLTableReconciliationState.CDC_DISABLED), [])).toThrow(
      /CDC is not enabled/
    );
  });

  it('updates legacy metadata-free candidates to the newest capture instance', () => {
    const resolution = reconcile(readyContext([createCaptureInstance(50)]), [
      createSourceTableCandidate('a'),
      createSourceTableCandidate('b')
    ]);
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
    expect(() =>
      reconcile(unavailableContext(MSSQLTableReconciliationState.CDC_DISABLED), [createSourceTableCandidate('a')])
    ).toThrow(/CDC is no longer enabled/);
  });

  it('reports a changed source identity as unavailable when CDC is disabled', () => {
    const changedSource = createSourceDescriptor({ objectId: 200 });
    expect(() =>
      reconcile(unavailableContext(MSSQLTableReconciliationState.CDC_DISABLED, changedSource), [
        createSourceTableCandidate('old')
      ])
    ).toThrow(/no longer matches the source table binding/);
  });

  it('preserves a pinned capture identity that is still available', () => {
    const resolution = reconcile(readyContext([createCaptureInstance(50), createCaptureInstance(40)]), [
      createSourceTableCandidate('a', { captureTableObjectId: 40 })
    ]);
    expect(resolution.compatibleTables.map((table) => table.id)).toEqual(['a']);
    expect(resolution.compatibleTables[0].sourceMetadata).toEqual({ captureTableObjectId: 40 });
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 40 } });
  });

  it('fails when the pinned capture instance was dropped, even with a replacement available', () => {
    expect(() =>
      reconcile(readyContext([createCaptureInstance(50)]), [
        createSourceTableCandidate('a', { captureTableObjectId: 40 })
      ])
    ).toThrow(/no longer available/);
  });

  it('fails on a mixture of metadata-free and pinned candidates', () => {
    expect(() =>
      reconcile(readyContext([createCaptureInstance(40)]), [
        createSourceTableCandidate('a'),
        createSourceTableCandidate('b', { captureTableObjectId: 40 })
      ])
    ).toThrow(/mixture/);
  });

  it('fails on multiple distinct pinned identities', () => {
    expect(() =>
      reconcile(readyContext([createCaptureInstance(40), createCaptureInstance(41)]), [
        createSourceTableCandidate('a', { captureTableObjectId: 40 }),
        createSourceTableCandidate('b', { captureTableObjectId: 41 })
      ])
    ).toThrow(/multiple persisted capture identities/);
  });

  it('does not replace an existing binding when the source identity changed', () => {
    const changedSource = createSourceDescriptor({ objectId: 200 });
    expect(() =>
      reconcile(readyContext([createCaptureInstance(50)], changedSource), [
        createSourceTableCandidate('old', { captureTableObjectId: 40 })
      ])
    ).toThrow(/Table \[dbo]\.\[users] no longer matches the source table binding.*already-replicated data is retained/);
  });

  it('drops stale incompatible candidates when a compatible candidate anchors the binding', () => {
    const resolution = reconcile(readyContext([createCaptureInstance(50)]), [
      createSourceTableCandidate('a'),
      createSourceTableCandidate('mismatch', undefined, {
        replicaIdColumns: [{ name: 'id', type: 'bigint', typeId: 127 }]
      })
    ]);
    expect(
      resolution.compatibleTables.map((table) => ({ id: table.id, sourceMetadata: table.sourceMetadata }))
    ).toEqual([{ id: 'a', sourceMetadata: { captureTableObjectId: 50 } }]);
    expect(resolution.incompatibleTables.map((table) => table.id)).toEqual(['mismatch']);
  });
});

function readyContext(
  captureInstances: CaptureInstance[],
  sourceDescriptor: SourceEntityDescriptor = createSourceDescriptor()
): MSSQLTableReconciliationContext {
  return {
    state: MSSQLTableReconciliationState.READY,
    source: sourceDescriptor,
    captureInstances
  };
}

function unavailableContext(
  state: MSSQLTableReconciliationState.TABLE_MISSING | MSSQLTableReconciliationState.CDC_DISABLED,
  sourceDescriptor: SourceEntityDescriptor = createSourceDescriptor()
): MSSQLTableReconciliationContext {
  return { state, source: sourceDescriptor };
}
