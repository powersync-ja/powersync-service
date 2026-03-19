import { describe, expect, test } from 'vitest';

import { getCursorBatchBytes, trackChangeStreamBsonBytes } from '@module/replication/replication-index.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { clearTestDb, connectMongoData } from './util.js';

describe('internal mongodb utils', () => {
  // The implementation relies on internal APIs, so we verify this works as expected for various types of change streams.
  test('collection change stream size tracking', async () => {
    await testChangeStreamBsonBytes('collection');
  });

  test('db change stream size tracking', async () => {
    await testChangeStreamBsonBytes('db');
  });

  test('cluster change stream size tracking', async () => {
    await testChangeStreamBsonBytes('cluster');
  });

  test('cursor batch size tracking', async () => {
    const { db, client } = await connectMongoData();
    await using _ = { [Symbol.asyncDispose]: async () => await client.close() };
    await clearTestDb(db);
    const collection = db.collection('test_data');
    await collection.insertMany([{ test: 1 }, { test: 2 }, { test: 3 }, { test: 4 }, { test: 5 }]);

    const cursor = collection.find({}, { batchSize: 2 });
    let batchBytes: number[] = [];
    let totalBytes = 0;
    // We use this in the same way as ChunkedSnapshotQuery
    while (await cursor.hasNext()) {
      batchBytes.push(getCursorBatchBytes(cursor));
      totalBytes += batchBytes[batchBytes.length - 1];
      cursor.readBufferedDocuments();
    }

    // 3 batches: [2, 2, 1] documents. Should not change
    expect(batchBytes.length).toEqual(3);
    // Current tests show 839, but this may change depending on the MongoDB version and other conditions.
    expect(totalBytes).toBeGreaterThan(400);
    expect(totalBytes).toBeLessThan(1200);
  });

  async function testChangeStreamBsonBytes(type: 'db' | 'collection' | 'cluster') {
    // With MongoDB, replication uses the exact same document format
    // as normal queries. We test it anyway.
    const { db, client } = await connectMongoData();
    await using _ = { [Symbol.asyncDispose]: async () => await client.close() };
    await clearTestDb(db);
    const collection = db.collection('test_data');

    let stream: mongo.ChangeStream;
    if (type === 'collection') {
      stream = collection.watch([], {
        maxAwaitTimeMS: 5,
        fullDocument: 'updateLookup'
      });
    } else if (type === 'db') {
      stream = db.watch([], {
        maxAwaitTimeMS: 5,
        fullDocument: 'updateLookup'
      });
    } else {
      stream = client.watch([], {
        maxAwaitTimeMS: 5,
        fullDocument: 'updateLookup'
      });
    }

    let batchBytes: number[] = [];
    let totalBytes = 0;
    trackChangeStreamBsonBytes(stream, (bytes) => {
      batchBytes.push(bytes);
      totalBytes += bytes;
    });

    const readAll = async () => {
      while ((await stream.tryNext()) != null) {}
    };

    await readAll();

    await collection.insertOne({ test: 1 });
    await readAll();
    await collection.insertOne({ test: 2 });
    await readAll();
    await collection.insertOne({ test: 3 });
    await readAll();

    await stream.close();

    // The exact length by vary based on exact batching logic, but we do want to know when it changes.
    // Note: If this causes unstable tests, we can relax this check.
    expect(batchBytes.length).toEqual(8);

    // Current tests show 4464-4576 bytes for the size, depending on the type of change stream.
    // This can easily vary based on the mongodb version and general conditions, so we just check the general range.
    // For the most part, if any bytes are reported, the tracking is working.
    expect(totalBytes).toBeGreaterThan(2000);
    expect(totalBytes).toBeLessThan(8000);
  }
});
