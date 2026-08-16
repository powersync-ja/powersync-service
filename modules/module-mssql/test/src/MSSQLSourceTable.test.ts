import { MSSQLSourceTable } from '@module/common/MSSQLSourceTable.js';
import { describe, expect, it } from 'vitest';
import { createCaptureInstance, createSourceDescriptor, createSourceTableCandidate } from './util.js';

describe('MSSQLSourceTable.setCaptureInstance', () => {
  it('sets the instance matching the persisted capture-table object id', () => {
    const table = new MSSQLSourceTable(createSourceDescriptor(), [
      createSourceTableCandidate('a', { captureTableObjectId: 40 })
    ]);
    const expected = createCaptureInstance(40);

    table.setCaptureInstance([createCaptureInstance(50), expected]);

    expect(table.captureInstance).toBe(expected);
  });

  it('sets null when the binding is legacy or the pinned instance is unavailable', () => {
    const legacy = new MSSQLSourceTable(createSourceDescriptor(), [createSourceTableCandidate('legacy')]);
    const pinned = new MSSQLSourceTable(createSourceDescriptor(), [
      createSourceTableCandidate('pinned', { captureTableObjectId: 40 })
    ]);

    legacy.setCaptureInstance([createCaptureInstance(40)]);
    pinned.setCaptureInstance([createCaptureInstance(40)]);
    pinned.setCaptureInstance([createCaptureInstance(50)]);

    expect(legacy.captureInstance).toBeNull();
    expect(pinned.captureInstance).toBeNull();
  });
});
