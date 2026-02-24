import { ChangeStreamInvalidatedError } from '@module/replication/ChangeStream.js';
import { MongoManager } from '@module/replication/MongoManager.js';
import { normalizeConnectionConfig } from '@module/types/types.js';
import { describe, expect, test } from 'vitest';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { env } from './env.js';
import { describeWithStorage, StorageVersionTestContext } from './util.js';

describe('mongodb resuming replication', () => {
  describeWithStorage({}, defineResumeTest);
});

function defineResumeTest({ factory: factoryGenerator, storageVersion }: StorageVersionTestContext) {
  const openContext = (options?: Parameters<typeof ChangeStreamTestContext.open>[1]) => {
    return ChangeStreamTestContext.open(factoryGenerator, { ...options, storageVersion });
  };

  test('resuming with a different source database', async () => {
    await using context = await openContext();
    const { db } = context;

    await context.updateSyncRules(/* yaml */
    ` bucket_definitions:
        global:
          data:
            - SELECT _id as id, description, num FROM "test_data"`);

    await context.replicateSnapshot();

    context.startStreaming();

    const collection = db.collection('test_data');
    await collection.insertOne({ description: 'test1', num: 1152921504606846976n });

    // Wait for the item above to be replicated. The commit should store a resume token.
    await context.getCheckpoint();

    // Done with this context for now
    await context.dispose();

    // Use the provided MongoDB url to connect to a different source database
    const originalUrl = env.MONGO_TEST_URL;
    // Change this to a different database
    const url = new URL(originalUrl);
    const parts = url.pathname.split('/');
    parts[1] = 'differentDB'; // Replace the database name
    url.pathname = parts.join('/');

    // Point to a new source DB
    const connectionManager = new MongoManager(
      normalizeConnectionConfig({
        type: 'mongodb',
        uri: url.toString()
      })
    );
    const factory = await factoryGenerator({ doNotClear: true });

    // Create a new context without updating the sync rules
    await using context2 = new ChangeStreamTestContext(factory, connectionManager, {}, storageVersion);
    const activeContent = await factory.getActiveSyncRulesContent();
    context2.storage = factory.getInstance(activeContent!);

    // If this test times out, it likely didn't throw the expected error here.
    const error = await context2.startStreaming().catch((ex) => ex);
    // The ChangeStreamReplicationJob will detect this and throw a ChangeStreamInvalidatedError
    expect(error).toBeInstanceOf(ChangeStreamInvalidatedError);
  });
}
