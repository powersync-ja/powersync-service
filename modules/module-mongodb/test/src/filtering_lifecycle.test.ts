import {
  filterMongoReplicationStream,
  MongoReplicationFilterResult,
  MongoReplicationQueryProvider,
  PostImagesOption
} from '@module/index.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { describe, expect, test } from 'vitest';
import { openChangeStreamTestContext } from './change_stream_test_setup.js';
import { DATABASE_TYPE, DatabaseType } from './DatabaseType.js';
import { describeWithStorage } from './util.js';

const CONFIG = /* yaml */ `
  # Sync config fixture.
  config:
    edition: 3
  streams:
    global:
      query: SELECT _id AS id, active FROM documents
`;

/**
 * Filter membership is decided by the provider, not by the SQL query, so bucket assertions detect leaks.
 * Required pre/post-images let the provider distinguish entries, exits and source deletes precisely.
 */
const PROVIDER: MongoReplicationQueryProvider = {
  getSnapshotFilter: () => ({ active: true }),
  openChangeStream: ({ open }) =>
    filterMongoReplicationStream({
      batches: open({ imageOptions: { fullDocumentBeforeChange: 'required' } }),
      filter: (event) => {
        const after = 'fullDocument' in event ? event.fullDocument : undefined;
        const before = 'fullDocumentBeforeChange' in event ? event.fullDocumentBeforeChange : undefined;
        if (after != null && mongo.BSON.deserialize(after).active === true) {
          return MongoReplicationFilterResult.KEEP;
        }
        if (before != null && mongo.BSON.deserialize(before).active === true) {
          return MongoReplicationFilterResult.DELETE;
        }
        return MongoReplicationFilterResult.EXCLUDE;
      }
    })
};

describe.skipIf(DATABASE_TYPE == DatabaseType.DOCUMENTDB)('MongoDB filtering lifecycle', () => {
  describeWithStorage({ timeout: 30_000 }, ({ factory, storageVersion }) => {
    test('keeps snapshot and streaming membership consistent across updates, deletes and restart', async () => {
      const options = {
        storageVersion,
        mongoOptions: { postImages: PostImagesOption.READ_ONLY },
        streamOptions: { createReplicationQueryProvider: () => PROVIDER }
      };
      let resumeAfter: bigint;
      {
        await using context = await openChangeStreamTestContext(factory, options);
        await context.updateSyncRules(CONFIG);
        const collection = await context.db.createCollection<{ _id: string; active: boolean }>('documents', {
          changeStreamPreAndPostImages: { enabled: true }
        });
        await collection.insertMany([
          { _id: 'one', active: true },
          { _id: 'two', active: false },
          { _id: 'three', active: false }
        ]);
        await context.replicateSnapshot();
        let checkpoint = await context.getCheckpoint();
        expect(await context.getBucketDataAtCheckpoint('global|0[]', checkpoint)).toMatchObject([
          { op: 'PUT', object_id: 'one' }
        ]);

        // Enter the selected set through a real update, using the server's write-time post-image.
        await collection.updateOne({ _id: 'two' }, { $set: { active: true } });
        expect(await context.getBucketData('global|0[]', checkpoint.checkpoint)).toMatchObject([
          { op: 'PUT', object_id: 'two' }
        ]);
        checkpoint = await context.getCheckpoint();

        // Leaving the set must emit a synthetic delete rather than merely suppressing the update.
        await collection.updateOne({ _id: 'one' }, { $set: { active: false } });
        await collection.insertOne({ _id: 'excluded', active: false });
        expect(await context.getBucketData('global|0[]', checkpoint.checkpoint)).toMatchObject([
          { op: 'REMOVE', object_id: 'one' }
        ]);
        resumeAfter = (await context.getCheckpoint()).checkpoint;
        await context.stop();

        // Queue a source delete and another entry while replication is stopped.
        await collection.deleteOne({ _id: 'two' });
        await collection.updateOne({ _id: 'three' }, { $set: { active: true } });
      }

      await using resumed = await openChangeStreamTestContext(factory, { ...options, doNotClear: true });
      await resumed.loadActiveSyncRules();
      resumed.startStreaming();
      expect(await resumed.getBucketData('global|0[]', resumeAfter)).toMatchObject([
        { op: 'REMOVE', object_id: 'two' },
        { op: 'PUT', object_id: 'three' }
      ]);
    });
  });
});
