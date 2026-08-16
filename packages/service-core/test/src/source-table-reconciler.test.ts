import { SourceEntityDescriptor } from '@/storage/SourceEntity.js';
import { SourceTable, sourceTableIdEquals } from '@/storage/SourceTable.js';
import {
  defaultSourceTableReconciler,
  diffSourceTableUpdates,
  materializeSourceTableResolution,
  sourceIdentityCompatible,
  validateSourceTableCandidateResolution
} from '@/storage/SourceTableReconciler.js';
import * as bson from 'bson';
import { describe, expect, it } from 'vitest';

function descriptor(overrides: Partial<SourceEntityDescriptor> = {}): SourceEntityDescriptor {
  return {
    connectionTag: 'default',
    schema: 'public',
    name: 'users',
    objectId: 100,
    replicaIdColumns: [{ name: 'id', type: 'int', typeId: 23 }],
    ...overrides
  };
}

function candidate(overrides: Partial<ConstructorParameters<typeof SourceTable>[0]> = {}): SourceTable {
  return new SourceTable({
    id: overrides.id ?? 'table-1',
    ref: overrides.ref ?? { connectionTag: 'default', schema: 'public', name: 'users' },
    objectId: 'objectId' in overrides ? overrides.objectId! : 100,
    replicaIdColumns: overrides.replicaIdColumns ?? [{ name: 'id', type: 'int', typeId: 23 }],
    snapshotComplete: overrides.snapshotComplete ?? true,
    bucketDataSources: overrides.bucketDataSources ?? [],
    parameterLookupSources: overrides.parameterLookupSources ?? [],
    sourceMetadata: overrides.sourceMetadata
  });
}

describe('sourceIdentityCompatible', () => {
  it('matches identical identity', () => {
    expect(sourceIdentityCompatible(descriptor(), candidate())).toBe(true);
  });

  it('rejects a different object id', () => {
    expect(sourceIdentityCompatible(descriptor({ objectId: 200 }), candidate({ objectId: 100 }))).toBe(false);
  });

  it('rejects a different schema/name', () => {
    expect(sourceIdentityCompatible(descriptor({ name: 'accounts' }), candidate())).toBe(false);
  });

  it('rejects changed replica-id columns', () => {
    expect(
      sourceIdentityCompatible(
        descriptor(),
        candidate({ replicaIdColumns: [{ name: 'id', type: 'bigint', typeId: 20 }] })
      )
    ).toBe(false);
  });

  it('treats an undefined descriptor object id as a wildcard on object id', () => {
    expect(sourceIdentityCompatible(descriptor({ objectId: undefined }), candidate({ objectId: 999 }))).toBe(true);
  });
});

describe('defaultSourceTableReconciler', () => {
  it('returns all identity-compatible candidates and no metadata', async () => {
    const a = candidate({ id: 'a' });
    const b = candidate({ id: 'b', ref: { connectionTag: 'default', schema: 'public', name: 'accounts' } });
    const resolution = await defaultSourceTableReconciler({ source: descriptor(), candidates: [a, b] });
    expect(resolution.compatibleTables).toEqual([a]);
    expect(resolution.incompatibleTables).toEqual([b]);
    expect(resolution.newTableValues).toEqual({ sourceMetadata: null });
  });

  it('returns an empty set when nothing matches', async () => {
    const resolution = await defaultSourceTableReconciler({
      source: descriptor({ objectId: 200 }),
      candidates: [candidate({ objectId: 100 })]
    });
    expect(resolution.compatibleTables).toHaveLength(0);
    expect(resolution.incompatibleTables.map((table) => table.id)).toEqual(['table-1']);
  });
});

describe('validateSourceTableCandidateResolution', () => {
  it('accepts an explicit compatible/incompatible partition', () => {
    const a = candidate({ id: 'a' });
    const b = candidate({ id: 'b' });
    expect(() =>
      validateSourceTableCandidateResolution([a, b], {
        compatibleTables: [a],
        incompatibleTables: [b],
        newTableValues: { sourceMetadata: null }
      })
    ).not.toThrow();
  });

  it('rejects omitted, duplicate, and unknown candidates', () => {
    const a = candidate({ id: 'a' });
    expect(() =>
      validateSourceTableCandidateResolution([a], {
        compatibleTables: [],
        incompatibleTables: [],
        newTableValues: { sourceMetadata: null }
      })
    ).toThrow(/exactly once/);
    expect(() =>
      validateSourceTableCandidateResolution([a], {
        compatibleTables: [a],
        incompatibleTables: [a],
        newTableValues: { sourceMetadata: null }
      })
    ).toThrow(/exactly once/);
    expect(() =>
      validateSourceTableCandidateResolution([a], {
        compatibleTables: [a],
        incompatibleTables: [candidate({ id: 'unknown' })],
        newTableValues: { sourceMetadata: null }
      })
    ).toThrow(/unknown candidate/);
  });
});

describe('diffSourceTableUpdates', () => {
  function resolution(compatibleTables: SourceTable[]) {
    return { compatibleTables, incompatibleTables: [], newTableValues: { sourceMetadata: null } };
  }

  it('returns nothing when the reconciler returned the candidates untouched', () => {
    const a = candidate({ id: 'a', sourceMetadata: { captureTableObjectId: 7 } });
    expect(diffSourceTableUpdates([a], resolution([a]))).toEqual([]);
  });

  it('compares by value, not by reference', () => {
    // A new object with equal metadata should not cause a write.
    const a = candidate({ id: 'a', sourceMetadata: { captureTableObjectId: 7 } });
    const rebuilt = a.withSourceMetadata({ captureTableObjectId: 7 });

    expect(rebuilt.sourceMetadata).not.toBe(a.sourceMetadata);
    expect(diffSourceTableUpdates([a], resolution([rebuilt]))).toEqual([]);
  });

  it('returns changed metadata', () => {
    const a = candidate({ id: 'a', sourceMetadata: { captureTableObjectId: 7 } });
    expect(diffSourceTableUpdates([a], resolution([a.withSourceMetadata({ captureTableObjectId: 8 })]))).toEqual([
      { id: 'a', sourceMetadata: { captureTableObjectId: 8 } }
    ]);
  });

  it('returns metadata added to a legacy record', () => {
    const legacy = candidate({ id: 'a' });
    expect(legacy.sourceMetadata).toBeNull();
    expect(
      diffSourceTableUpdates([legacy], resolution([legacy.withSourceMetadata({ captureTableObjectId: 7 })]))
    ).toEqual([{ id: 'a', sourceMetadata: { captureTableObjectId: 7 } }]);
  });

  it('returns cleared metadata as null', () => {
    const a = candidate({ id: 'a', sourceMetadata: { captureTableObjectId: 7 } });
    expect(diffSourceTableUpdates([a], resolution([a.withSourceMetadata(null)]))).toEqual([
      { id: 'a', sourceMetadata: null }
    ]);
  });

  it('materializes only source metadata from reconciler results', () => {
    const persisted = candidate({ id: 'a', snapshotComplete: false });
    const modified = persisted.withSourceMetadata({ captureTableObjectId: 8 });
    modified.snapshotComplete = true;
    modified.syncData = false;

    const [materialized] = materializeSourceTableResolution([persisted], resolution([modified])).compatibleTables;

    expect(materialized.sourceMetadata).toEqual({ captureTableObjectId: 8 });
    expect(materialized.snapshotComplete).toBe(false);
    expect(materialized.syncData).toBe(persisted.syncData);
  });

  it('does not trust incompatible candidate mutations made through a cast', () => {
    const persisted = candidate({ id: 'a', snapshotComplete: false });
    const exposed = persisted.clone();
    // Simulate callback code bypassing the read-only type boundary.
    (exposed as any).snapshotComplete = true;

    const materialized = materializeSourceTableResolution([persisted], {
      compatibleTables: [],
      incompatibleTables: [exposed],
      newTableValues: { sourceMetadata: null }
    }).incompatibleTables[0];

    expect(exposed.snapshotComplete).toBe(true);
    expect(materialized).toBe(persisted);
    expect(materialized.snapshotComplete).toBe(false);
    expect(materialized.syncData).toBe(persisted.syncData);
  });

  it('only reports the candidates that changed', () => {
    const a = candidate({ id: 'a', sourceMetadata: { captureTableObjectId: 7 } });
    const b = candidate({ id: 'b', sourceMetadata: { captureTableObjectId: 7 } });
    const updates = diffSourceTableUpdates([a, b], resolution([a, b.withSourceMetadata({ captureTableObjectId: 9 })]));
    expect(updates).toEqual([{ id: 'b', sourceMetadata: { captureTableObjectId: 9 } }]);
  });

  it('rejects a compatible table that was never a candidate', () => {
    expect(() => diffSourceTableUpdates([candidate({ id: 'a' })], resolution([candidate({ id: 'ghost' })]))).toThrow(
      /unknown candidate/
    );
  });
});

describe('SourceTable.clone', () => {
  it('isolates mutable identity and metadata values', () => {
    const persisted = candidate({ id: 'a', sourceMetadata: { captureTableObjectId: 7 } });
    const exposed = persisted.clone();

    (exposed.ref as any).name = 'changed';
    exposed.replicaIdColumns[0].name = 'changed';
    (exposed.sourceMetadata as any).captureTableObjectId = 8;

    expect(persisted.name).toBe('users');
    expect(persisted.replicaIdColumns[0].name).toBe('id');
    expect(persisted.sourceMetadata).toEqual({ captureTableObjectId: 7 });
  });
});

describe('sourceTableIdEquals', () => {
  it('compares string ids by value', () => {
    expect(sourceTableIdEquals('table-1', 'table-1')).toBe(true);
    expect(sourceTableIdEquals('table-1', 'table-2')).toBe(false);
  });

  it('compares BSON ObjectIds by value', () => {
    const id = new bson.ObjectId();
    const copy = new bson.ObjectId(id.toHexString());

    expect(id).not.toBe(copy);
    expect(sourceTableIdEquals(id, copy)).toBe(true);
  });

  it('does not mix string and BSON ObjectId representations', () => {
    const id = new bson.ObjectId();

    expect(sourceTableIdEquals(id.toHexString(), id)).toBe(false);
    expect(sourceTableIdEquals(id, id.toHexString())).toBe(false);
  });
});
