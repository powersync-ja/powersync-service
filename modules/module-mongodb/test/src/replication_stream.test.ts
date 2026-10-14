import { ChangeStreamInvalidatedError } from '@module/replication/ChangeStream.js';
import { DEFAULT_MONGO_REPLICATION_QUERY_PROVIDER } from '@module/replication/MongoReplicationQueryProvider.js';
import {
  createMongoReplicationStreamBatch,
  filterMongoReplicationStream,
  openMongoReplicationStream,
  parseMongoReplicationStream
} from '@module/replication/MongoReplicationStream.js';
import type {
  MongoReplicationParserOptions,
  MongoReplicationStreamBatch,
  RawMongoReplicationStreamBatch
} from '@module/replication/MongoReplicationStreamTypes.js';
import { MongoReplicationFilterResult } from '@module/replication/MongoReplicationStreamTypes.js';
import {
  mapChangeStreamError,
  parseChangeDocument,
  ProjectedChangeStreamDocument
} from '@module/replication/RawChangeStream.js';
import { mongo } from '@powersync/lib-service-mongodb';
import { DatabaseConnectionError } from '@powersync/lib-services-framework';
import { describe, expect, test, vi } from 'vitest';
import { env } from './env.js';

/**
 * Exercise cursor construction and stream parsing without contacting MongoDB. Cursor tests intercept
 * the driver's command method; reader tests supply BSON batches with explicit events and resume tokens.
 */
describe('MongoDB replication stream reader', () => {
  const read = (options: MongoReplicationParserOptions & { batches: AsyncIterable<RawMongoReplicationStreamBatch> }) =>
    flattenBatches(parseMongoReplicationStream(options));
  test.each(['pre', 'post'])('invalidates replication when a required %s-image is missing', (image) => {
    const error = new mongo.MongoServerError({
      code: 47,
      codeName: 'NoMatchingDocument',
      errmsg: `Change stream was configured to require a ${image}-image, but the ${image}-image was not found for event: example`
    });

    // Invalidated streams trigger a fresh snapshot instead of retrying an unrecoverable position.
    expect(() => mapChangeStreamError(error)).toThrow(ChangeStreamInvalidatedError);
  });

  test('does not invalidate replication for unrelated NoMatchingDocument errors', () => {
    const error = new mongo.MongoServerError({
      code: 47,
      codeName: 'NoMatchingDocument',
      errmsg: 'No matching document found'
    });

    expect(() => mapChangeStreamError(error)).toThrow(DatabaseConnectionError);
  });

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
        collectStream(
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
        collectStream(
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
    const batches = [batch([event('first'), event('second')], 'data'), batch([], 'idle')];
    const outputBatches = await collectBatches(
      read({
        batches: from(batches),
        defaultSchema: 'app',
        multipleDatabases: false
      })
    );
    expect(outputBatches.map((items) => items.length)).toEqual([3, 1]);
    const items = outputBatches.flat();
    expect(items.map((item) => item.type)).toEqual(['change', 'change', 'progress', 'progress']);
    expect(items[0]).toMatchObject({ type: 'change', hasBufferedChanges: true });
    expect(items[1]).toMatchObject({ type: 'change', hasBufferedChanges: false });
    if (items[0].type != 'change' || !('fullDocument' in items[0].event)) throw new Error('Expected a row');
    expect(Buffer.isBuffer(items[0].event.fullDocument)).toBe(true);
    expect(mongo.BSON.deserialize(items[0].event.fullDocument!)).toEqual({ _id: 'first', body: 'raw row' });
    expect(items.slice(2)).toEqual([
      { type: 'progress', resumeToken: { _data: 'data' }, filteredCount: null },
      { type: 'progress', resumeToken: { _data: 'idle' }, filteredCount: null }
    ]);
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
    const outputBatches = await collectBatches(
      read({
        batches: from([
          batch([event('before'), first], 'unsafe'),
          batch([], 'still-unsafe'),
          batch([last, event('after')], 'safe')
        ]),
        defaultSchema: 'app',
        multipleDatabases: false
      })
    );
    expect(outputBatches.map((items) => items.length)).toEqual([1, 0, 3]);
    const items = outputBatches.flat();
    // A consumer must never save either partial token, even though all three transport batches are counted.
    expect(items.map((item) => item.type)).toEqual(['change', 'change', 'change', 'progress']);
    expect(items[1]).toMatchObject({
      type: 'change',
      event: { documentKey: { _id: 'large' }, _id: { _data: 'fragment-2' } }
    });
    expect(items[3]).toEqual({ type: 'progress', resumeToken: { _data: 'safe' }, filteredCount: null });
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
    const stream = read({
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
      // Stop after the first batch while another remains unread. Both consumer return
      // and signal cancellation must close the source without delivering the next batch.
      const closed = vi.fn();
      const controller = new AbortController();
      const stream = read({
        batches: from([batch([event('first')], 'safe'), batch([event('unread')], 'unread')], closed),
        defaultSchema: 'app',
        multipleDatabases: false,
        signal: controller.signal
      });
      expect([...(await stream.next()).value!]).toMatchObject([
        { type: 'change' },
        { type: 'progress', resumeToken: { _data: 'safe' } }
      ]);
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
      read({
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

describe('lazy batch consumption contract', () => {
  test('keeps batch metadata independent of safe tokens and distinguishes unfiltered from zero', () => {
    for (const filteredCount of [null, 0, 2]) {
      const output = createMongoReplicationStreamBatch({
        events: [],
        resolveResumeToken: () => undefined,
        resolveMetadata: () => ({ filteredCount })
      });
      expect(() => output.metadata).toThrow('Consume all batch events');
      expect([...output.events]).toEqual([]);
      expect(output.getResumeToken()).toBeUndefined();
      expect(output.metadata.filteredCount).toBe(filteredCount);
      expect(Object.isFrozen(output.metadata)).toBe(true);
    }
    const unfiltered = createMongoReplicationStreamBatch({ events: [], resolveResumeToken: () => undefined });
    expect([...unfiltered.events]).toEqual([]);
    expect(unfiltered.metadata.filteredCount).toBeNull();
  });

  test('requires exhaustion even after the last event, and caches progress once', () => {
    const change = { event: parseChangeDocument(batch([event('one')], 'safe').events[0]), hasBufferedChanges: false };
    let filteredCount = 2;
    const progress = vi.fn(() => ({ _data: 'safe' }));
    const output = createMongoReplicationStreamBatch({
      events: [change],
      resolveResumeToken: progress,
      resolveMetadata: () => ({ filteredCount })
    });
    expect(() => output.getResumeToken()).toThrow('Consume all batch events');
    expect(() => output.metadata).toThrow('Consume all batch events');
    const iterator = output.events[Symbol.iterator]();
    expect(iterator.next().value).toBe(change);
    expect(() => output.getResumeToken()).toThrow('Consume all batch events');
    expect(() => output.metadata).toThrow('Consume all batch events');
    expect(progress).not.toHaveBeenCalled();
    expect(iterator.next().done).toBe(true);
    const saved = output.getResumeToken();
    filteredCount = 99;
    expect(output.getResumeToken()).toBe(saved);
    expect(output.metadata.filteredCount).toBe(2);
    expect(progress).toHaveBeenCalledOnce();
    expect(() => [...output.events]).toThrow('only be consumed once');
  });

  test.each(['return', 'failure'] as const)('withholds progress after iteration ends by %s', (end) => {
    const closed = vi.fn();
    const progress = vi.fn(() => ({ _data: 'unsafe' }));
    const output = createMongoReplicationStreamBatch({
      events: (function* () {
        try {
          yield { event: parseChangeDocument(batch([event('one')], 'safe').events[0]), hasBufferedChanges: true };
          throw new Error('filter failure');
        } finally {
          closed();
        }
      })(),
      resolveResumeToken: progress
    });
    const iterator = output.events[Symbol.iterator]();
    iterator.next();
    if (end == 'return') iterator.return?.();
    else expect(() => iterator.next()).toThrow('filter failure');
    expect(closed).toHaveBeenCalledOnce();
    expect(progress).not.toHaveBeenCalled();
    expect(() => output.getResumeToken()).toThrow('Consume all batch events');
    expect(() => output.metadata).toThrow('Consume all batch events');
  });

  test('rejects advancing the stream past an unconsumed batch and closes its source', async () => {
    const closed = vi.fn();
    const stream = parseMongoReplicationStream({
      batches: from([batch([event('one')], 'one'), batch([event('two')], 'two')], closed),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    await stream.next();
    await expect(stream.next()).rejects.toThrow('Consume all batch events');
    expect(closed).toHaveBeenCalledOnce();
  });

  test('an earlier batch keeps its progress result after later fragments complete', async () => {
    const original = event('large');
    const stream = parseMongoReplicationStream({
      batches: from([
        batch([{ ...original, fullDocument: undefined, splitEvent: { fragment: 1, of: 2 } }], 'unsafe'),
        batch(
          [{ _id: { _data: 'last' }, fullDocument: original.fullDocument, splitEvent: { fragment: 2, of: 2 } }],
          'safe'
        )
      ]),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    const first = (await stream.next()).value!;
    expect([...first.events]).toEqual([]);
    expect(first.getResumeToken()).toBeUndefined();
    const second = (await stream.next()).value!;
    expect([...second.events]).toHaveLength(1);
    expect(second.getResumeToken()).toEqual({ _data: 'safe' });
    expect(first.getResumeToken()).toBeUndefined();
    expect((await stream.next()).done).toBe(true);
  });

  test('filtering parses on demand and preserves buffered work when trailing events are excluded', async () => {
    const source = batch([event('checkpoint'), event('excluded')], 'safe');
    const stream = parseMongoReplicationStream({
      batches: from([source]),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    const input = (await stream.next()).value!;
    const inspected = vi.fn();
    let filtered = 0;
    const output = createMongoReplicationStreamBatch({
      events: (function* () {
        for (const change of input.events) {
          inspected(change.event);
          if ('documentKey' in change.event && change.event.documentKey._id == 'excluded') {
            filtered++;
            continue;
          }
          yield change;
        }
      })(),
      resolveResumeToken: () => input.getResumeToken(),
      resolveMetadata: () => ({ filteredCount: filtered })
    });
    expect(inspected).not.toHaveBeenCalled();
    const iterator = output.events[Symbol.iterator]();
    expect(iterator.next().value).toMatchObject({ hasBufferedChanges: true });
    expect(inspected).toHaveBeenCalledTimes(1);
    expect(() => output.getResumeToken()).toThrow('Consume all batch events');
    expect(() => output.metadata).toThrow('Consume all batch events');
    expect(iterator.next().done).toBe(true);
    expect(inspected).toHaveBeenCalledTimes(2);
    expect(output.getResumeToken()).toEqual({ _data: 'safe' });
    expect(output.metadata.filteredCount).toBe(1);
    expect((await stream.next()).done).toBe(true);
  });

  test('does not finalize again after a failure while resolving the token', () => {
    const resolve = vi.fn(() => {
      throw new Error('resolve failure');
    });
    const output = createMongoReplicationStreamBatch({ events: [], resolveResumeToken: resolve });
    const iterator = output.events[Symbol.iterator]();
    expect(() => iterator.next()).toThrow('resolve failure');
    expect(iterator.next().done).toBe(true);
    expect(resolve).toHaveBeenCalledOnce();
    expect(() => output.getResumeToken()).toThrow('Consume all batch events');
  });

  test('ends parsed event iteration on early return without exposing progress', async () => {
    const stream = parseMongoReplicationStream({
      batches: from([batch([event('one'), event('two')], 'safe')]),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    const output = (await stream.next()).value!;
    const iterator = output.events[Symbol.iterator]();
    expect(iterator.next().value).toMatchObject({ event: { documentKey: { _id: 'one' } } });
    expect(iterator.return?.(7)).toEqual({ done: true, value: 7 });
    expect(iterator.next().done).toBe(true);
    expect(() => output.getResumeToken()).toThrow('Consume all batch events');
    await stream.return(undefined);
  });

  test('a parse failure ends iteration without parsing later events or exposing progress', async () => {
    const stream = parseMongoReplicationStream({
      batches: from([batch([{ ...event('bad'), splitEvent: { fragment: 2, of: 2 } }, event('after')], 'safe')]),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    const output = (await stream.next()).value!;
    const iterator = output.events[Symbol.iterator]();
    expect(() => iterator.next()).toThrow('Unexpected splitEvent');
    expect(iterator.next().done).toBe(true);
    expect(() => output.getResumeToken()).toThrow('Consume all batch events');
    await stream.return(undefined);
  });

  test('rejects advancing past an empty batch whose events were never iterated', async () => {
    const closed = vi.fn();
    const stream = parseMongoReplicationStream({
      batches: from([batch([], 'one'), batch([], 'two')], closed),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    await stream.next();
    await expect(stream.next()).rejects.toThrow('Consume all batch events');
    expect(closed).toHaveBeenCalledOnce();
  });

  test('an abort after the last event does not interrupt finalization, leaving the stop to the consumer', async () => {
    const controller = new AbortController();
    const stream = parseMongoReplicationStream({
      batches: from([batch([event('one')], 'safe'), batch([event('two')], 'later')]),
      defaultSchema: 'app',
      multipleDatabases: false,
      signal: controller.signal
    });
    const output = (await stream.next()).value!;
    const iterator = output.events[Symbol.iterator]();
    expect(iterator.next().done).toBe(false);
    controller.abort(new Error('stopped'));
    // Consumers check the signal themselves before persisting this token.
    expect(iterator.next().done).toBe(true);
    expect(output.getResumeToken()).toEqual({ _data: 'safe' });
    // The next batch pull is where the abort surfaces.
    await expect(stream.next()).rejects.toThrow('stopped');
  });
});

describe('shared replication filter', () => {
  test('filters lazily, preserves control events and creates deletes with source metadata', async () => {
    const checkpoint = { ...event('checkpoint'), ns: { db: 'app', coll: '_powersync_checkpoints' } };
    const drop = { _id: { _data: 'drop' }, operationType: 'drop', ns: { db: 'app', coll: 'documents' } };
    const invalidate = { _id: { _data: 'invalidate' }, operationType: 'invalidate' };
    const leaving: mongo.Document = {
      ...event('leaving'),
      txnNumber: 7n,
      clusterTime: new mongo.Timestamp({ t: 10, i: 1 })
    };
    const filter = vi.fn((change: { documentKey: mongo.Document }) => {
      return change.documentKey._id == 'leaving'
        ? MongoReplicationFilterResult.DELETE
        : MongoReplicationFilterResult.EXCLUDE;
    });
    const stream = filterMongoReplicationStream({
      batches: parseMongoReplicationStream({
        batches: from([batch([checkpoint, drop, invalidate, leaving, event('excluded')], 'safe')]),
        defaultSchema: 'app',
        multipleDatabases: false
      }),
      filter
    });
    const output = (await stream.next()).value!;
    expect(filter).not.toHaveBeenCalled();
    const changes = [...output.events];
    expect(changes.map(({ event }) => event.operationType)).toEqual(['insert', 'drop', 'invalidate', 'delete']);
    expect(filter).toHaveBeenCalledTimes(2);
    expect(changes[3]).toMatchObject({
      hasBufferedChanges: true,
      event: {
        _id: leaving._id,
        documentKey: leaving.documentKey,
        ns: leaving.ns,
        txnNumber: 7n,
        clusterTime: leaving.clusterTime
      }
    });
    expect(output.metadata.filteredCount).toBe(1);
    expect(output.getResumeToken()).toEqual({ _data: 'safe' });
    expect((await stream.next()).done).toBe(true);
  });

  test('preserves split-event progress and counts fully filtered batches through composed filters', async () => {
    const large = event('large');
    const source = parseMongoReplicationStream({
      batches: from([
        batch([event('excluded'), { ...large, fullDocument: undefined, splitEvent: { fragment: 1, of: 2 } }], 'unsafe'),
        batch(
          [{ _id: { _data: 'last' }, fullDocument: large.fullDocument, splitEvent: { fragment: 2, of: 2 } }],
          'safe'
        ),
        batch([], 'idle')
      ]),
      defaultSchema: 'app',
      multipleDatabases: false
    });
    const stream = filterMongoReplicationStream({
      batches: filterMongoReplicationStream({ batches: source, filter: () => MongoReplicationFilterResult.EXCLUDE }),
      filter: () => MongoReplicationFilterResult.KEEP
    });
    const first = (await stream.next()).value!;
    expect([...first.events]).toEqual([]);
    expect(first.metadata.filteredCount).toBe(1);
    expect(first.getResumeToken()).toBeUndefined();
    const second = (await stream.next()).value!;
    expect([...second.events]).toEqual([]);
    expect(second.metadata.filteredCount).toBe(1);
    expect(second.getResumeToken()).toEqual({ _data: 'safe' });
    expect(first.getResumeToken()).toBeUndefined();
    const idle = (await stream.next()).value!;
    expect([...idle.events]).toEqual([]);
    expect(idle.metadata.filteredCount).toBe(0);
    expect(idle.getResumeToken()).toEqual({ _data: 'idle' });
    expect((await stream.next()).done).toBe(true);
  });

  test.each(['failure', 'return', 'unconsumed'] as const)('closes the source without progress on %s', async (end) => {
    const closed = vi.fn();
    const stream = filterMongoReplicationStream({
      batches: parseMongoReplicationStream({
        batches: from([batch([event('first'), event('second')], 'unsafe')], closed),
        defaultSchema: 'app',
        multipleDatabases: false
      }),
      filter: () => {
        if (end == 'failure') {
          throw new Error('filter failed');
        }
        return MongoReplicationFilterResult.KEEP;
      }
    });
    let output: MongoReplicationStreamBatch | undefined;
    if (end == 'unconsumed') {
      output = (await stream.next()).value!;
      await expect(stream.next()).rejects.toThrow('Consume all batch events');
    } else {
      const consume = async () => {
        for await (const batch of stream) {
          output = batch;
          for (const _change of batch.events) {
            return;
          }
        }
      };
      if (end == 'failure') {
        await expect(consume()).rejects.toThrow('filter failed');
      } else {
        await consume();
      }
    }
    expect(closed).toHaveBeenCalledOnce();
    expect(() => output!.getResumeToken()).toThrow('Consume all batch events');
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
 * Encode events as raw BSON, matching the reader's transport input. Tokens are opaque test labels.
 */
function batch(events: mongo.Document[], token: string): RawMongoReplicationStreamBatch {
  return {
    events: events.map((event) => Buffer.from(mongo.BSON.serialize(event))),
    resumeToken: { _data: token }
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

async function collect<T>(stream: AsyncIterable<Iterable<T>>): Promise<T[]> {
  const items: T[] = [];
  for await (const batch of stream) items.push(...batch);
  return items;
}

async function collectBatches<T>(stream: AsyncIterable<Iterable<T>>): Promise<T[][]> {
  const batches: T[][] = [];
  for await (const batch of stream) batches.push([...batch]);
  return batches;
}

/**
 * Flatten parsed batches only in tests to assert ordered events and progress across both paths.
 */
type MongoReplicationStreamItem =
  | { type: 'change'; event: ProjectedChangeStreamDocument; hasBufferedChanges: boolean }
  | { type: 'progress'; resumeToken: mongo.ResumeToken; filteredCount: number | null };

async function* flattenBatches(
  batches: AsyncIterable<MongoReplicationStreamBatch>
): AsyncGenerator<Iterable<MongoReplicationStreamItem>> {
  for await (const batch of batches) yield items(batch);
  function* items(batch: MongoReplicationStreamBatch): Generator<MongoReplicationStreamItem> {
    for (const change of batch.events) yield { type: 'change', ...change };
    const progress = batch.getResumeToken();
    if (progress != null) {
      yield { type: 'progress', resumeToken: progress, filteredCount: batch.metadata.filteredCount };
    }
  }
}

async function collectStream<T>(stream: AsyncIterable<T>): Promise<T[]> {
  const batches: T[] = [];
  for await (const batch of stream) batches.push(batch);
  return batches;
}
