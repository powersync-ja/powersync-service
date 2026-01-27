import { describe, expect, test } from 'vitest';
import * as storage from '../../../src/storage/storage-index.js';

describe('SourceTable', () => {
  describe('storeCurrentData property', () => {
    test('defaults to true', () => {
      const table = new storage.SourceTable({
        id: 'test-id',
        connectionTag: 'test',
        objectId: 123,
        schema: 'public',
        name: 'test_table',
        replicaIdColumns: [{ name: 'id' }],
        snapshotComplete: true
      });

      expect(table.storeCurrentData).toBe(true);
    });

    test('can be set to false', () => {
      const table = new storage.SourceTable({
        id: 'test-id',
        connectionTag: 'test',
        objectId: 123,
        schema: 'public',
        name: 'test_table',
        replicaIdColumns: [{ name: 'id' }],
        snapshotComplete: true
      });

      table.storeCurrentData = false;
      expect(table.storeCurrentData).toBe(false);
    });

    test('is preserved when cloning', () => {
      const table = new storage.SourceTable({
        id: 'test-id',
        connectionTag: 'test',
        objectId: 123,
        schema: 'public',
        name: 'test_table',
        replicaIdColumns: [{ name: 'id' }],
        snapshotComplete: true
      });

      table.storeCurrentData = false;
      const cloned = table.clone();

      expect(cloned.storeCurrentData).toBe(false);
      expect(cloned).not.toBe(table); // Ensure it's a different instance
    });

    test('clone preserves all properties including storeCurrentData', () => {
      const table = new storage.SourceTable({
        id: 'test-id',
        connectionTag: 'test',
        objectId: 123,
        schema: 'public',
        name: 'test_table',
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
      const table = new storage.SourceTable({
        id: 'test-id',
        connectionTag: 'test',
        objectId: 123,
        schema: 'public',
        name: 'test_table',
        replicaIdColumns: [{ name: 'id' }],
        snapshotComplete: true
      });

      table.storeCurrentData = false;
      table.syncData = true;
      table.syncParameters = false;
      table.syncEvent = false;

      expect(table.syncAny).toBe(true); // Should still be true
    });

    test('storeCurrentData is independent of snapshot status', () => {
      const table = new storage.SourceTable({
        id: 'test-id',
        connectionTag: 'test',
        objectId: 123,
        schema: 'public',
        name: 'test_table',
        replicaIdColumns: [{ name: 'id' }],
        snapshotComplete: false
      });

      table.storeCurrentData = false;
      expect(table.snapshotComplete).toBe(false);
      expect(table.storeCurrentData).toBe(false);
    });
  });
});
