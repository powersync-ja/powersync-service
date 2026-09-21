import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { describe, expect, test, vi } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { describeWithStorage } from './util.js';

describe.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)('MongoDB snapshot query provider', () => {
  describeWithStorage({ timeout: 30_000 }, ({ factory, storageVersion }) => {
    test.each([false, true])('pages snapshots with an expression filter: %s', async (filtered) => {
      // Run the same snapshot with and without a provider predicate. The filtered case selects even
      // ids between 2 and 12 whose label is exactly 'live'; null must preserve the full snapshot.
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
            // Providers need the actual source namespace and parsed connection options to build their
            // predicates. This sync config has no connection-specific options, so those are empty.
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
      // The collection treats 'LIVE' and 'live' as equal by default. Provider filters use simple
      // collation, so id 6 must be excluded for its 'LIVE' label even though it passes the id conditions.
      const collection = await context.db.createCollection<{ _id: number; label: string }>('documents', {
        collation: { locale: 'en', strength: 2 }
      });
      await collection.insertMany(
        Array.from({ length: 14 }, (_, i) => ({ _id: i + 1, label: (i + 1) % 3 == 0 ? 'LIVE' : 'live' }))
      );
      await context.replicateSnapshot();
      const rows = (await context.getBucketData('global[]')).filter((op) => op.op == 'PUT');
      // Matching ids 4, 8 and 10 require two pages at a page size of two. After the first page ends
      // at id 8, pagination adds its own $expr requiring _id > 8. It must combine that condition
      // with the provider's $expr: id 10 is included, while ids 9 and 11 through 14 remain excluded.
      // Replacing the provider predicate with the continuation predicate would admit those extra rows.
      // With no provider filter, pagination must return all 14 documents instead.
      expect(rows.map((op) => op.object_id).sort()).toEqual(
        (filtered ? [4, 8, 10] : Array.from({ length: 14 }, (_, i) => i + 1)).map(String).sort()
      );
      expect(getSnapshotFilter).toHaveBeenCalledWith(
        expect.objectContaining({ schema: context.db.databaseName, name: 'documents' })
      );
    });

    test('rejects unsupported sources before opening snapshot or streaming queries', async () => {
      // Simulate a provider requiring a capability the source does not support. Replication must
      // surface that validation error before asking the provider to open a stream or select snapshot rows.
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
      // Spies delegate to the real default hooks if invoked, but validation should prevent either call.
      expect(openChangeStream).not.toHaveBeenCalled();
      expect(getSnapshotFilter).not.toHaveBeenCalled();
    });
  });
});
