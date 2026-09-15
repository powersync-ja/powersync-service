import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { describe, expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { describeWithStorage } from './util.js';

describe.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)('MongoDB snapshot query provider', () => {
  describeWithStorage({ timeout: 30_000 }, ({ factory, storageVersion }) => {
    test.each([false, true])('pages snapshots with an expression filter: %s', async (filtered) => {
      const getSnapshotFilter = vi.fn(() =>
        filtered
          ? {
              $expr: {
                $and: [
                  { $gt: ['$_id', 2] },
                  { $lt: ['$_id', 12] },
                  { $eq: [{ $mod: ['$_id', 2] }, 0] },
                  { $eq: ['$label', 'live'] }
                ]
              }
            }
          : null
      );
      await using context = await openChangeStreamTestContext(factory, {
        storageVersion,
        streamOptions: {
          snapshotChunkLength: 2,
          createReplicationQueryProvider: ({ connectionTag, defaultSchema, syncConfig }) => {
            expect(connectionTag).toBe(context.connectionTag);
            expect(defaultSchema).toBe(context.db.databaseName);
            expect(syncConfig.connectionConfig).toEqual({});
            return { ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER, getSnapshotFilter };
          }
        }
      });
      await context.updateSyncRules(/* yaml */ ` bucket_definitions:
          global:
            data:
              - SELECT _id AS id, label FROM documents `);
      // A case-insensitive collection default must not widen the provider's simple-collation predicate.
      const collection = await context.db.createCollection<{ _id: number; label: string }>('documents', {
        collation: { locale: 'en', strength: 2 }
      });
      await collection.insertMany(
        Array.from({ length: 14 }, (_, i) => ({ _id: i + 1, label: (i + 1) % 3 == 0 ? 'LIVE' : 'live' }))
      );
      await context.replicateSnapshot();
      const rows = (await context.getBucketData('global[]')).filter((op) => op.op == 'PUT');
      // More matches than the two-row page size exercise continuation without replacing the filter's $expr.
      expect(rows.map((op) => op.object_id).sort()).toEqual(
        (filtered ? [4, 8, 10] : Array.from({ length: 14 }, (_, i) => i + 1)).map(String).sort()
      );
      expect(getSnapshotFilter).toHaveBeenCalledWith(
        expect.objectContaining({ schema: context.db.databaseName, name: 'documents' })
      );
    });

    test('rejects unsupported sources before opening snapshot or streaming queries', async () => {
      const validateSource = vi.fn(async () => {
        throw new Error('unsupported test source');
      });
      const openChangeStream = vi.fn(DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.openChangeStream);
      const getSnapshotFilter = vi.fn(DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER.getSnapshotFilter);
      await using context = await openChangeStreamTestContext(factory, {
        storageVersion,
        streamOptions: {
          createReplicationQueryProvider: () => ({ validateSource, openChangeStream, getSnapshotFilter })
        }
      });
      await context.updateSyncRules(/* yaml */ ` bucket_definitions:
          global:
            data:
              - SELECT _id AS id FROM documents `);
      await expect(context.replicateSnapshot()).rejects.toThrow('unsupported test source');
      expect(validateSource).toHaveBeenCalledWith({
        connectionManager: context.connectionManager,
        isDocumentDb: false
      });
      expect(openChangeStream).not.toHaveBeenCalled();
      expect(getSnapshotFilter).not.toHaveBeenCalled();
    });
  });
});
