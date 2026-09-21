import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import { openMongoReplicationStream, readMongoReplicationStream } from '@module/replication/MongoReplicationStream.js';
import { ChangeStreamBatch } from '@module/replication/RawChangeStream.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { describe, expect, test, vi } from 'vitest';
import { env } from './env.js';

/**
 * Exercise cursor construction and stream decoding without contacting MongoDB. Cursor tests intercept
 * the driver's command method; reader tests supply BSON batches with explicit events and resume tokens.
 */
describe('MongoDB replication stream reader', () => {
  test.each([
    { isDocumentDb: false, multipleDatabases: false, usePostImages: false },
    { isDocumentDb: false, multipleDatabases: false, usePostImages: true },
    { isDocumentDb: false, multipleDatabases: true, usePostImages: true },
    { isDocumentDb: true, multipleDatabases: false, usePostImages: true }
  ])('preserves the base cursor options for %j', async ({ isDocumentDb, multipleDatabases, usePostImages }) => {
    const client = new mongo.MongoClient(env.MONGO_TEST_DATA_URL);
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
    // A provider requests pre-images and adds a stage. The shared opener must still supply the source
    // position and post-image settings, and run the provider stage before large events are split.
    const client = new mongo.MongoClient(env.MONGO_TEST_DATA_URL);
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
    // Two changes arrive together, followed by an empty response. Preserve raw BSON document bodies
    // and indicate whether another change is buffered, while exposing a resume boundary for each batch.
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
    // A large event spans two nonempty batches with an idle response between them. Saving either
    // intermediate token would lose the in-memory first fragment if replication restarted there.
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
    // The first fragment is missing.
    [{ ...event('broken'), splitEvent: { fragment: 2, of: 2 } }],
    // A fragment in the middle is missing.
    [
      { ...event('broken'), splitEvent: { fragment: 1, of: 3 } },
      { _id: { _data: 'bad' }, splitEvent: { fragment: 3, of: 3 } }
    ],
    // An ordinary event interrupts reassembly.
    [{ ...event('broken'), splitEvent: { fragment: 1, of: 2 } }, event('unrelated')],
    // The source iterator ends before the final fragment arrives.
    [{ ...event('broken'), splitEvent: { fragment: 1, of: 2 } }]
  ])('fails closed on incomplete or out-of-order fragments %#', async (...events) => {
    // Invalid fragment sequences must reject consumption and close the upstream iterator so a caller
    // cannot mistake a partial document for a complete change or continue from its batch token.
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
      // Stop after the first change while another remains in the same batch. Both consumer return
      // and signal cancellation must run upstream cleanup before the batch's progress is yielded.
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
    // Legacy positions contain only a timestamp, so an event at that timestamp is already covered.
    // The remaining event uses an Atlas Flex database prefix that must be removed before delivery.
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

/**
 * Minimal insert event for the app.documents fixture namespace; callers override fields for each scenario.
 */
function event(id: string): mongo.Document {
  return {
    _id: { _data: id },
    operationType: 'insert',
    ns: { db: 'app', coll: 'documents' },
    documentKey: { _id: id },
    fullDocument: { _id: id, body: 'raw row' }
  };
}

/**
 * Encode events as raw BSON, matching the reader's transport input. Tokens are opaque test labels;
 * the fixed byte count supports batch accounting assertions without depending on BSON payload size.
 */
function batch(events: mongo.Document[], token: string): ChangeStreamBatch {
  return {
    events: events.map((event) => Buffer.from(mongo.BSON.serialize(event))),
    resumeToken: { _data: token },
    byteSize: 1234
  };
}

/**
 * Supply deterministic source batches and expose iterator cleanup to cancellation and failure tests.
 */
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
