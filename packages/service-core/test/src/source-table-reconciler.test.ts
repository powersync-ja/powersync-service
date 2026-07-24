import { SourceEntityDescriptor } from '@/storage/SourceEntity.js';
import { SourceTable, sourceTableIdEquals } from '@/storage/SourceTable.js';
import { defaultSourceTableReconciler, sourceIdentityCompatible } from '@/storage/SourceTableReconciler.js';
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
  it('returns all identity-compatible candidates and no metadata', () => {
    const a = candidate({ id: 'a' });
    const b = candidate({ id: 'b', ref: { connectionTag: 'default', schema: 'public', name: 'accounts' } });
    const resolution = defaultSourceTableReconciler({ source: descriptor(), candidates: [a, b] });
    expect([...resolution.sourceCompatibleCandidateIds]).toEqual(['a']);
    expect(resolution.sourceMetadata).toBeUndefined();
  });

  it('returns an empty set when nothing matches', () => {
    const resolution = defaultSourceTableReconciler({
      source: descriptor({ objectId: 200 }),
      candidates: [candidate({ objectId: 100 })]
    });
    expect(resolution.sourceCompatibleCandidateIds.size).toBe(0);
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
