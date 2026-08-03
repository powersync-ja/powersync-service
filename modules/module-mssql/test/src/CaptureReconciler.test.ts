import { CaptureInstance } from '@module/common/CaptureInstance.js';
import { LSN } from '@module/common/LSN.js';
import {
  createCaptureReconciler,
  MSSQLSourceMetadata,
  readCaptureMetadata
} from '@module/replication/CaptureReconciler.js';
import { SourceEntityDescriptor, SourceTable } from '@powersync/service-core';
import { describe, expect, it } from 'vitest';

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

function instance(objectId: number): CaptureInstance {
  return {
    name: `dbo_users_${objectId}`,
    objectId,
    minLSN: LSN.fromString(LSN.ZERO),
    createDate: new Date(),
    pendingSchemaChanges: []
  };
}

describe('readCaptureMetadata', () => {
  it('parses a valid metadata object', () => {
    expect(readCaptureMetadata({ captureTableObjectId: 7 })).toEqual({ captureTableObjectId: 7 });
  });

  it('returns null for legacy metadata', () => {
    expect(readCaptureMetadata(undefined)).toBeNull();
    expect(readCaptureMetadata(null)).toBeNull();
  });

  it('rejects malformed metadata', () => {
    expect(() => readCaptureMetadata([1, 2] as any)).toThrow();
    expect(() => readCaptureMetadata({ foo: 1 } as any)).toThrow();
  });
});

describe('createCaptureReconciler', () => {
  it('pins a new binding to the newest available capture instance', () => {
    const resolution = createCaptureReconciler([instance(50), instance(40)])({
      source: source(),
      candidates: []
    });
    expect(resolution.compatibleTables).toEqual([]);
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 50 } });
  });

  it('fails a new binding when no capture instance is available', () => {
    expect(() => createCaptureReconciler([])({ source: source(), candidates: [] })).toThrow(
      /No CDC capture instance is available/
    );
  });

  it('updates legacy metadata-free candidates to the newest capture instance', () => {
    const resolution = createCaptureReconciler([instance(50)])({
      source: source(),
      candidates: [candidate('a'), candidate('b')]
    });
    expect(
      resolution.compatibleTables.map((table) => ({ id: table.id, sourceMetadata: table.sourceMetadata }))
    ).toEqual([
      { id: 'a', sourceMetadata: { captureTableObjectId: 50 } },
      { id: 'b', sourceMetadata: { captureTableObjectId: 50 } }
    ]);
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 50 } });
  });

  it('fails a legacy binding when no capture instance is available', () => {
    expect(() =>
      createCaptureReconciler([])({
        source: source(),
        candidates: [candidate('a')]
      })
    ).toThrow(/No CDC capture instance is available/);
  });

  it('preserves a pinned capture identity that is still available', () => {
    const resolution = createCaptureReconciler([instance(50), instance(40)])({
      source: source(),
      candidates: [candidate('a', { captureTableObjectId: 40 })]
    });
    expect(resolution.compatibleTables.map((table) => table.id)).toEqual(['a']);
    expect(resolution.compatibleTables[0].sourceMetadata).toEqual({ captureTableObjectId: 40 });
    expect(resolution.incompatibleTables).toEqual([]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: { captureTableObjectId: 40 } });
  });

  it('fails when the pinned capture instance was dropped, even with a replacement available', () => {
    expect(() =>
      createCaptureReconciler([instance(50)])({
        source: source(),
        candidates: [candidate('a', { captureTableObjectId: 40 })]
      })
    ).toThrow(/no longer available/);
  });

  it('fails on a mixture of metadata-free and pinned candidates', () => {
    expect(() =>
      createCaptureReconciler([instance(40)])({
        source: source(),
        candidates: [candidate('a'), candidate('b', { captureTableObjectId: 40 })]
      })
    ).toThrow(/mixture/);
  });

  it('fails on multiple distinct pinned identities', () => {
    expect(() =>
      createCaptureReconciler([instance(40), instance(41)])({
        source: source(),
        candidates: [candidate('a', { captureTableObjectId: 40 }), candidate('b', { captureTableObjectId: 41 })]
      })
    ).toThrow(/multiple persisted capture identities/);
  });

  it('drops candidates that do not match the generic identity', () => {
    const resolution = createCaptureReconciler([instance(50)])({
      source: source(),
      candidates: [
        candidate('a'),
        candidate('mismatch', undefined, {
          replicaIdColumns: [{ name: 'id', type: 'bigint', typeId: 127 }]
        })
      ]
    });
    expect(
      resolution.compatibleTables.map((table) => ({ id: table.id, sourceMetadata: table.sourceMetadata }))
    ).toEqual([{ id: 'a', sourceMetadata: { captureTableObjectId: 50 } }]);
    expect(resolution.incompatibleTables.map((table) => table.id)).toEqual(['mismatch']);
  });
});
