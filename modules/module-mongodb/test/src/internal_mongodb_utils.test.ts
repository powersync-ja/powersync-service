import { describe, expect, test } from 'vitest';

import { ChangeStreamBatch, namespaceCollection, rawChangeStream } from '@module/replication/RawChangeStream.js';
import { getCursorBatchBytes } from '@module/replication/replication-index.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { bson } from '@powersync/service-core';
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
    const { db, client } = await connectMongoData();
    await using _ = { [Symbol.asyncDispose]: async () => await client.close() };
    await clearTestDb(db);
    const collection = db.collection('test_data');

    let stream: AsyncIterableIterator<ChangeStreamBatch>;
    const pipeline = [
      {
        $changeStream: {
          fullDocument: 'updateLookup',
          allChangesForCluster: type == 'cluster'
        }
      }
    ];
    if (type === 'collection') {
      stream = rawChangeStream(db, pipeline, {
        batchSize: 10,
        maxAwaitTimeMS: 5,
        maxTimeMS: 1_000,
        collection: collection.collectionName
      });
    } else if (type === 'db') {
      stream = rawChangeStream(db, pipeline, {
        batchSize: 10,
        maxAwaitTimeMS: 5,
        maxTimeMS: 1_000
      });
    } else {
      stream = rawChangeStream(client.db('admin'), pipeline, {
        batchSize: 10,
        maxAwaitTimeMS: 5,
        maxTimeMS: 1_000
      });
    }

    let batchBytes: number[] = [];
    let totalBytes = 0;

    const readAll = async () => {
      while (true) {
        const next = await stream.next();
        if (next.done) {
          break;
        }
        const bytes = next.value.byteSize;
        batchBytes.push(bytes);
        totalBytes += bytes;

        if (next.value.events.length == 0) {
          break;
        }
      }
    };

    await readAll();

    await collection.insertOne({ test: 1 });
    await readAll();
    await collection.insertOne({ test: 2 });
    await readAll();
    await collection.insertOne({ test: 3 });
    await readAll();

    await stream.return?.();

    // The exact length by vary based on exact batching logic, but we do want to know when it changes.
    // Note: If this causes unstable tests, we can relax this check.
    expect(batchBytes.length).toEqual(7);

    // Current tests show 4464-4576 bytes for the size, depending on the type of change stream.
    // This can easily vary based on the mongodb version and general conditions, so we just check the general range.
    // For the most part, if any bytes are reported, the tracking is working.
    expect(totalBytes).toBeGreaterThan(2000);
    expect(totalBytes).toBeLessThan(8000);
  }

  test('should resume on missing cursor (1)', async () => {
    // Many resumable errors are difficult to simulate, but CursorNotFound is easy.

    const { db, client } = await connectMongoData();
    await using _ = { [Symbol.asyncDispose]: async () => await client.close() };
    await clearTestDb(db);
    const collection = db.collection('test_data');

    const pipeline = [
      {
        $changeStream: {
          fullDocument: 'updateLookup'
        }
      }
    ];
    const stream = rawChangeStream(db, pipeline, {
      batchSize: 10,
      maxAwaitTimeMS: 5,
      maxTimeMS: 1_000
    });

    let readDocs: any[] = [];
    const readAll = async () => {
      while (true) {
        const next = await stream.next();
        if (next.done) {
          break;
        }

        if (next.value.events.length == 0) {
          break;
        }

        readDocs.push(...next.value.events.map((e) => bson.deserialize(e, { useBigInt64: true })));
      }
    };

    await readAll();

    await collection.insertOne({ test: 1 });
    await readAll();
    await collection.insertOne({ test: 2 });
    await readAll();
    await collection.insertOne({ test: 3 });
    await killChangeStreamCursor(db, client);
    await collection.insertOne({ test: 4 });
    await readAll();

    await stream.return?.();

    expect(readDocs.map((doc) => doc.fullDocument)).toMatchObject([{ test: 1 }, { test: 2 }, { test: 3 }, { test: 4 }]);
  });

  test('should resume on missing cursor (2)', async () => {
    const { db, client } = await connectMongoData();
    await using _ = { [Symbol.asyncDispose]: async () => await client.close() };
    await clearTestDb(db);
    const collection = db.collection('test_data');

    const currentOpStream = rawChangeStream(
      db,
      [
        {
          $changeStream: {
            fullDocument: 'updateLookup'
          }
        }
      ],
      {
        batchSize: 10,
        maxAwaitTimeMS: 5,
        maxTimeMS: 1_000
      }
    );
    const firstBatch = await currentOpStream.next();
    await currentOpStream.return();
    const resumeAfter = firstBatch.value!.resumeToken;

    const stream = rawChangeStream(
      db,
      [
        {
          $changeStream: {
            fullDocument: 'updateLookup',
            resumeAfter
          }
        }
      ],
      {
        batchSize: 10,
        maxAwaitTimeMS: 5,
        maxTimeMS: 1_000
      }
    );

    let readDocs: any[] = [];
    const readAll = async () => {
      while (true) {
        const next = await stream.next();
        if (next.done) {
          break;
        }

        if (next.value.events.length == 0) {
          break;
        }

        readDocs.push(...next.value.events.map((e) => bson.deserialize(e, { useBigInt64: true })));
      }
    };

    await readAll();

    await collection.insertOne({ test: 1 });
    await readAll();
    await collection.insertOne({ test: 2 });
    await readAll();
    await collection.insertOne({ test: 3 });
    await killChangeStreamCursor(db, client);
    await collection.insertOne({ test: 4 });
    await readAll();

    await stream.return?.();

    expect(readDocs.map((doc) => doc.fullDocument)).toMatchObject([{ test: 1 }, { test: 2 }, { test: 3 }, { test: 4 }]);
  });
});

async function killChangeStreamCursor(db: mongo.Db, client: mongo.MongoClient) {
  const ops = await client
    .db('admin')
    .aggregate<CurrentOpIdleCursor>([{ $currentOp: { idleCursors: true } }, { $match: { type: 'idleCursor' } }])
    .toArray();

  const ns = `${db.databaseName}.$cmd.aggregate`;
  const op = ops.find((op) => {
    const command = op.cursor?.originatingCommand;
    return op.ns == ns && Array.isArray(command?.pipeline) && command.pipeline[0]?.$changeStream != null;
  });

  if (op?.cursor == null) {
    throw new Error(
      `Could not find change stream cursor. Idle cursors: ${JSON.stringify(
        ops.map((op) => ({
          ns: op.ns,
          type: op.type,
          cursorId: op.cursor?.cursorId?.toString(),
          aggregate: op.cursor?.originatingCommand?.aggregate,
          pipeline: op.cursor?.originatingCommand?.pipeline
        }))
      )}`
    );
  }

  await db.command({
    killCursors: namespaceCollection(op.ns),
    cursors: [op.cursor.cursorId]
  });
}

type CurrentOpIdleCursor = {
  ns: string;
  type: string;
  cursor?: {
    cursorId: bigint;
    originatingCommand?: {
      aggregate?: unknown;
      pipeline?: any[];
    };
  };
};
