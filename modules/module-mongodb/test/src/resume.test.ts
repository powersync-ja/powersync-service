import { MongoLSN, ZERO_LSN } from '@module/common/MongoLSN.js';

import { MongoManager } from '@module/replication/MongoManager.js';
import { normalizeConnectionConfig } from '@module/types/types.js';
import { isMongoServerError, mongo } from '@powersync/lib-service-mongodb';
import { BucketStorageFactory, TestStorageOptions } from '@powersync/service-core';
import { describe, expect, test, vi } from 'vitest';
import { ChangeStreamTestContext } from './change_stream_utils.js';
import { env } from './env.js';
import { INITIALIZED_MONGO_STORAGE_FACTORY, INITIALIZED_POSTGRES_STORAGE_FACTORY } from './util.js';

describe('mongo lsn', () => {
  test('LSN with resume tokens should be comparable', () => {
    // Values without a resume token should be comparable
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1)
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10)
        }).comparable
    ).true;

    // Values with resume tokens should correctly compare
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1),
        resume_token: { _data: 'resume1' }
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10),
          resume_token: { _data: 'resume2' }
        }).comparable
    ).true;

    // The resume token should not affect comparison
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1),
        resume_token: { _data: '2' }
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10),
          resume_token: { _data: '1' }
        }).comparable
    ).true;

    // Resume token should not be required for comparison
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable > // Switching the order to test this case
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(9)
        }).comparable
    ).true;

    // Comparison should be backwards compatible with old LSNs
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable > ZERO_LSN
    ).true;
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(10),
        resume_token: { _data: '2' }
      }).comparable >
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(1)
        }).comparable.split('|')[0] // Simulate an old LSN
    ).true;
    expect(
      new MongoLSN({
        timestamp: mongo.Timestamp.fromNumber(1),
        resume_token: { _data: '2' }
      }).comparable <
        new MongoLSN({
          timestamp: mongo.Timestamp.fromNumber(10)
        }).comparable.split('|')[0] // Simulate an old LSN
    ).true;
  });
});

describe.skipIf(!env.TEST_MONGO_STORAGE)('MongoDB resume - mongo storage', () => {
  defineResumeTest(INITIALIZED_MONGO_STORAGE_FACTORY);
});

describe.skipIf(!env.TEST_POSTGRES_STORAGE)('MongoDB resume - postgres storage', () => {
  defineResumeTest(INITIALIZED_POSTGRES_STORAGE_FACTORY);
});

function defineResumeTest(factoryGenerator: (options?: TestStorageOptions) => Promise<BucketStorageFactory>) {
  test('resuming with a different source database', async () => {
    await using context = await ChangeStreamTestContext.open(factoryGenerator);
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
    await vi.waitFor(
      async () => {
        const checkpoint = await context.storage?.getCheckpoint();
        expect(MongoLSN.fromSerialized(checkpoint!.lsn!).resumeToken).exist;
      },
      { timeout: 5000 }
    );

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
    await using context2 = new ChangeStreamTestContext(factory, connectionManager);
    const activeContent = await factory.getActiveSyncRulesContent();
    context2.storage = factory.getInstance(activeContent!);

    const error = await context2.startStreaming().catch((ex) => ex);
    expect(error).exist;
    // The ChangeStreamReplicationJob will detect this and throw a ChangeStreamInvalidatedError
    expect(isMongoServerError(error) && error.hasErrorLabel('NonResumableChangeStreamError'));
  });
}
