import { describe, expect, test } from 'vitest';
import * as storage from '../../../src/storage/storage-index.js';

/**
 * Build a SourceTable with the current `ref`-based options shape.
 * Data/parameter sources are not relevant to the storeCurrentData behaviour, so they default to empty.
 */
function makeTable(options: Partial<storage.SourceTableOptions> = {}): storage.SourceTable {
  return new storage.SourceTable({
    id: 'test-id',
    ref: { connectionTag: 'test', schema: 'public', name: 'test_table' },
    objectId: 123,
    replicaIdColumns: [{ name: 'id' }],
    snapshotComplete: true,
    bucketDataSources: [],
    parameterLookupSources: [],
    ...options
  });
}

describe('SourceTable', () => {
  describe('storeCurrentData property', () => {
    test('defaults to true', () => {
      const table = makeTable();
      expect(table.storeCurrentData).toBe(true);
    });

    test('can be set to false', () => {
      const table = makeTable();
      table.storeCurrentData = false;
      expect(table.storeCurrentData).toBe(false);
    });

    test('is preserved when cloning', () => {
      const table = makeTable();
      table.storeCurrentData = false;
      const cloned = table.clone();

      expect(cloned.storeCurrentData).toBe(false);
      expect(cloned).not.toBe(table);
    });

    test('clone preserves all properties including storeCurrentData', () => {
      const table = makeTable({
        replicaIdColumns: [{ name: 'id', type: 'int4' }],
        snapshotComplete: false
      });

      table.syncData = false;
      table.syncParameters = false;
      table.syncEvent = false;
      table.storeCurrentData = false;
      table.snapshotStatus = {
        totalEstimatedCount: 100,
        replicatedCount: 50,
        lastKey: Buffer.from('test')
      };

      const cloned = table.clone();

      expect(cloned.syncData).toBe(false);
      expect(cloned.syncParameters).toBe(false);
      expect(cloned.syncEvent).toBe(false);
      expect(cloned.storeCurrentData).toBe(false);
      expect(cloned.snapshotStatus).toEqual(table.snapshotStatus);
    });
  });

  describe('integration with other properties', () => {
    test('storeCurrentData does not affect syncAny', () => {
      const table = makeTable();

      table.storeCurrentData = false;
      table.syncData = true;
      table.syncParameters = false;
      table.syncEvent = false;

      expect(table.syncAny).toBe(true); // Should still be true
    });

    test('storeCurrentData is independent of snapshot status', () => {
      const table = makeTable({ snapshotComplete: false });

      table.storeCurrentData = false;
      expect(table.snapshotComplete).toBe(false);
      expect(table.storeCurrentData).toBe(false);
    });
  });
});
