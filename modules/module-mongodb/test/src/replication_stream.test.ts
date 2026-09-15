import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { openMongoReplicationStream, readMongoReplicationStream } from '@module/replication/MongoReplicationStream.js';
import { ChangeStreamBatch } from '@module/replication/RawChangeStream.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { describe, expect, test, vi } from 'vitest';

describe('MongoDB replication stream reader', () => {
  test.each([
    { isDocumentDb: false, multipleDatabases: false, usePostImages: false },
    { isDocumentDb: false, multipleDatabases: false, usePostImages: true },
    { isDocumentDb: false, multipleDatabases: true, usePostImages: true },
    { isDocumentDb: true, multipleDatabases: false, usePostImages: true }
  ])('preserves the base cursor options for %j', async ({ isDocumentDb, multipleDatabases, usePostImages }) => {
    const client = new mongo.MongoClient('mongodb://127.0.0.1:27017');
    await using clientDisposer = { [Symbol.asyncDispose]: () => client.close() };
    const db = client.db('app');
    const resumeAfter = { _data: 'exact-position' };
    // Observe the real cursor-opening path, stopping before any network operation. An exact token must
    // take precedence even if the stored position also contains its transaction's shared timestamp.
    const command = vi.spyOn(mongo.Db.prototype, 'command').mockRejectedValue(new Error('observed command'));
    try {
      await expect(
        collect(
          openMongoReplicationStream({
            db,
            queryProvider: DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
            namespaceFilter: { $match: { 'ns.coll': 'documents' }, multipleDatabases },
            isDocumentDb,
            usePostImages,
            position: { resumeAfter, startAfter: new mongo.Timestamp({ t: 10, i: 1 }) },
            skipInitialTimestamp: true,
            options: { batchSize: 50, maxAwaitTimeMS: 200, maxTimeMS: 1000 }
          })
        )
      ).rejects.toThrow('observed command');
      expect(command).toHaveBeenCalledOnce();
      expect((command.mock.contexts[0] as mongo.Db).databaseName).toBe(
        isDocumentDb || multipleDatabases ? 'admin' : 'app'
      );
      expect(command.mock.calls[0][0]).toEqual({
        aggregate: 1,
        pipeline: [
          {
            $changeStream: {
              fullDocument: !isDocumentDb && usePostImages ? 'required' : 'updateLookup',
              ...(!isDocumentDb ? { showExpandedEvents: true } : {}),
              ...(isDocumentDb || multipleDatabases ? { allChangesForCluster: true } : {}),
              resumeAfter
            }
          },
          { $match: { 'ns.coll': 'documents' } },
          ...(!isDocumentDb ? [{ $changeStreamSplitLargeEvent: {} }] : [])
        ],
        cursor: { batchSize: 1 },
        maxTimeMS: 1000
      });
    } finally {
      command.mockRestore();
    }
  });

  test('opens adapter stages after namespace selection, with pre-images and the shared resume settings', async () => {
    const client = new mongo.MongoClient('mongodb://127.0.0.1:27017');
    await using clientDisposer = { [Symbol.asyncDispose]: () => client.close() };
    const command = vi.spyOn(mongo.Db.prototype, 'command').mockRejectedValue(new Error('observed command'));
    const timestamp = new mongo.Timestamp({ t: 10, i: 1 });
    try {
      await expect(
        collect(
          openMongoReplicationStream({
            db: client.db('app'),
            queryProvider: {
              ...DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER,
              openChangeStream: ({ open }) =>
                open({
                  pipelineStages: [{ $set: { marker: 'keep' } }],
                  imageOptions: { fullDocumentBeforeChange: 'whenAvailable' }
                })
            },
            namespaceFilter: { $match: { 'ns.coll': 'documents' }, multipleDatabases: false },
            isDocumentDb: false,
            usePostImages: true,
            position: { startAfter: timestamp },
            options: { batchSize: 50, maxAwaitTimeMS: 200, maxTimeMS: 1000 }
          })
        )
      ).rejects.toThrow('observed command');
      expect(command.mock.calls[0][0].pipeline).toEqual([
        {
          $changeStream: {
            fullDocumentBeforeChange: 'whenAvailable',
            fullDocument: 'required',
            showExpandedEvents: true,
            startAtOperationTime: timestamp
          }
        },
        { $match: { 'ns.coll': 'documents' } },
        { $set: { marker: 'keep' } },
        { $changeStreamSplitLargeEvent: {} }
      ]);
    } finally {
      command.mockRestore();
    }
  });

  test('keeps raw rows, buffered checkpoint information and idle progress', async () => {
    const onBatch = vi.fn();
    const batches = [batch([event('first'), event('second')], 'data'), batch([], 'idle')];
    const items = await collect(
      readMongoReplicationStream({
        batches: from(batches),
        defaultSchema: 'app',
        multipleDatabases: false,
        onBatch
      })
    );
    expect(items.map((item) => item.type)).toEqual(['change', 'change', 'progress', 'progress']);
    expect(items[0]).toMatchObject({ type: 'change', hasBufferedChanges: true });
    expect(items[1]).toMatchObject({ type: 'change', hasBufferedChanges: false });
    if (items[0].type != 'change' || !('fullDocument' in items[0].event)) throw new Error('Expected a row');
    expect(Buffer.isBuffer(items[0].event.fullDocument)).toBe(true);
    expect(mongo.BSON.deserialize(items[0].event.fullDocument!)).toEqual({ _id: 'first', body: 'raw row' });
    expect(items.slice(2)).toEqual([
      { type: 'progress', resumeToken: { _data: 'data' }, filteredCount: 0 },
      { type: 'progress', resumeToken: { _data: 'idle' }, filteredCount: 0 }
    ]);
    expect(onBatch.mock.calls.map(([value]) => value)).toEqual(batches);
  });

  test('withholds progress across split batches, including an idle response between fragments', async () => {
    const original = event('large');
    const first = { ...original, fullDocument: undefined, splitEvent: { fragment: 1, of: 2 } };
    const last = {
      _id: { _data: 'fragment-2' },
      fullDocument: original.fullDocument,
      splitEvent: { fragment: 2, of: 2 }
    };
    const onBatch = vi.fn();
    const items = await collect(
      readMongoReplicationStream({
        batches: from([
          batch([event('before'), first], 'unsafe'),
          batch([], 'still-unsafe'),
          batch([last, event('after')], 'safe')
        ]),
        defaultSchema: 'app',
        multipleDatabases: false,
        onBatch
      })
    );
    // A consumer must never save either partial token, even though all three transport batches are counted.
    expect(items.map((item) => item.type)).toEqual(['change', 'change', 'change', 'progress']);
    expect(items[1]).toMatchObject({
      type: 'change',
      event: { documentKey: { _id: 'large' }, _id: { _data: 'fragment-2' } }
    });
    expect(items[3]).toEqual({ type: 'progress', resumeToken: { _data: 'safe' }, filteredCount: 0 });
    expect(onBatch).toHaveBeenCalledTimes(3);
  });

  test.each([
    [{ ...event('broken'), splitEvent: { fragment: 2, of: 2 } }],
    [
      { ...event('broken'), splitEvent: { fragment: 1, of: 3 } },
      { _id: { _data: 'bad' }, splitEvent: { fragment: 3, of: 3 } }
    ],
    [{ ...event('broken'), splitEvent: { fragment: 1, of: 2 } }, event('unrelated')],
    [{ ...event('broken'), splitEvent: { fragment: 1, of: 2 } }]
  ])('fails closed on incomplete or out-of-order fragments %#', async (...events) => {
    const closed = vi.fn();
    const stream = readMongoReplicationStream({
      batches: from([batch(events, 'unsafe')], closed),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    await expect(collect(stream)).rejects.toThrow('splitEvent');
    expect(closed).toHaveBeenCalledOnce();
  });

  test.each(['return', 'abort'] as const)(
    'closes the upstream iterator on %s without acknowledging unread work',
    async (end) => {
      const closed = vi.fn();
      const controller = new AbortController();
      const stream = readMongoReplicationStream({
        batches: from([batch([event('first'), event('unread')], 'unsafe')], closed),
        defaultSchema: 'app',
        multipleDatabases: false,
        signal: controller.signal
      });
      expect((await stream.next()).value).toMatchObject({ type: 'change' });
      if (end == 'return') await stream.return(undefined);
      else {
        controller.abort(new Error('cancelled'));
        await expect(stream.next()).rejects.toThrow('cancelled');
      }
      expect(closed).toHaveBeenCalledOnce();
    }
  );

  test('normalizes Atlas Flex namespaces and deduplicates legacy positions before adapter evaluation', async () => {
    const timestamp = new mongo.Timestamp({ t: 10, i: 1 });
    const items = await collect(
      readMongoReplicationStream({
        batches: from([
          batch(
            [
              { ...event('old'), clusterTime: timestamp },
              { ...event('new'), ns: { db: 'prefix_app', coll: 'documents' } }
            ],
            'safe'
          )
        ]),
        defaultSchema: 'app',
        multipleDatabases: false,
        startAfter: timestamp
      })
    );
    expect(items).toHaveLength(2);
    expect(items[0]).toMatchObject({
      type: 'change',
      event: { ns: { db: 'app', coll: 'documents' }, documentKey: { _id: 'new' } }
    });
  });
});

function event(id: string): mongo.Document {
  return {
    _id: { _data: id },
    operationType: 'insert',
    ns: { db: 'app', coll: 'documents' },
    documentKey: { _id: id },
    fullDocument: { _id: id, body: 'raw row' }
  };
}

function batch(events: mongo.Document[], token: string): ChangeStreamBatch {
  return {
    events: events.map((event) => Buffer.from(mongo.BSON.serialize(event))),
    resumeToken: { _data: token },
    byteSize: 1234
  };
}

async function* from<T>(values: T[], closed?: () => void) {
  try {
    yield* values;
  } finally {
    closed?.();
  }
}

async function collect<T>(stream: AsyncIterable<T>): Promise<T[]> {
  const items: T[] = [];
  for await (const item of stream) items.push(item);
  return items;
}
