import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { buildMongoChangeStreamPipeline, MongoReplicationQueryProvider } from './MongoReplicationQueryProvider.js';
import {
  MongoReplicationFilterOptions,
  MongoReplicationFilterResult,
  MongoReplicationParserOptions,
  MongoReplicationStreamBatch,
  MongoReplicationStreamBatchMetadata,
  MongoReplicationStreamChange,
  MongoReplicationStreamOptions,
  RawMongoReplicationStreamBatch
} from './MongoReplicationStreamTypes.js';
import {
  ChangeStreamBatch,
  parseChangeDocument,
  ProjectedChangeStreamDocument,
  rawChangeStream,
  RawChangeStreamOptions
} from './RawChangeStream.js';
import { CHECKPOINTS_COLLECTION } from './replication-utils.js';

/**
 * Wraps changes with a single-use iterator. Caches the token and metadata after successful exhaustion.
 */
export function createMongoReplicationStreamBatch({
  events,
  resolveResumeToken,
  resolveMetadata = () => ({ filteredCount: null })
}: {
  /**
   * Complete changes, optionally produced by a lazy parsing or filtering generator.
   */
  events: Iterable<MongoReplicationStreamChange>;
  /**
   * Compute a safe resume token once after successful exhaustion, using the final parser state.
   */
  resolveResumeToken: () => mongo.ResumeToken | undefined;
  /**
   * Compute this batch's final statistics after exhaustion. Defaults to no filtering.
   */
  resolveMetadata?: () => MongoReplicationStreamBatchMetadata;
}): MongoReplicationStreamBatch {
  let started = false;
  let complete = false;
  let resumeToken: mongo.ResumeToken | undefined;
  let metadata: MongoReplicationStreamBatchMetadata;
  function assertComplete() {
    // Progress is only safe to read after every event has been consumed and finalization succeeds.
    if (!complete) {
      throw new ReplicationAssertionError('Consume all batch events before requesting token or metadata');
    }
  }
  return {
    events: {
      [Symbol.iterator](): Iterator<MongoReplicationStreamChange> {
        // A second pass would repeat parsing/filtering against state that has already advanced.
        if (started) {
          throw new ReplicationAssertionError('Batch events can only be consumed once');
        }
        started = true;
        return iterate();
      }
    },
    getResumeToken() {
      assertComplete();
      return resumeToken;
    },
    get metadata() {
      assertComplete();
      return metadata;
    }
  };

  function* iterate() {
    // Delegate lazily; an error or early return skips the finalization below.
    yield* events;
    // Cache this batch's final results before later batches can change the parser or filter state.
    resumeToken = resolveResumeToken();
    metadata = Object.freeze({ ...resolveMetadata() });
    complete = true;
  }
}

/**
 * Filter complete data changes while retaining source batch boundaries and recovery bookkeeping.
 * Return MongoReplicationFilterResult.KEEP to forward a change, EXCLUDE to omit it, or DELETE for an exit.
 * Synthetic deletes retain the source identity, resume token and transaction metadata.
 * The callback runs synchronously and lazily; failures and early exits close the underlying stream.
 */
export async function* filterMongoReplicationStream({
  batches,
  filter
}: MongoReplicationFilterOptions): AsyncGenerator<MongoReplicationStreamBatch> {
  for await (const sourceBatch of batches) {
    let filteredCount = 0;
    const batch = createMongoReplicationStreamBatch({
      events: filterEvents(),
      resolveResumeToken: () => sourceBatch.getResumeToken(),
      resolveMetadata: () => ({ filteredCount: (sourceBatch.metadata.filteredCount ?? 0) + filteredCount })
    });
    yield batch;
    // Assert that the consumer exhausted this batch before requesting the next one, even if every event
    // was filtered out. The token value is unused; getResumeToken() throws if consumption is incomplete.
    batch.getResumeToken();

    function* filterEvents(): Generator<MongoReplicationStreamChange> {
      for (const change of sourceBatch.events) {
        const { event } = change;
        if (
          (event.operationType != 'insert' &&
            event.operationType != 'update' &&
            event.operationType != 'replace' &&
            event.operationType != 'delete') ||
          event.ns.coll == CHECKPOINTS_COLLECTION
        ) {
          yield change;
          continue;
        }
        const result = filter(event);
        if (result == MongoReplicationFilterResult.EXCLUDE) {
          filteredCount++;
        } else if (result == MongoReplicationFilterResult.DELETE) {
          yield { ...change, event: { ...event, operationType: 'delete' } };
        } else {
          yield change;
        }
      }
    }
  }
}

/**
 * Shared opening path for ongoing replication, snapshot barriers and resume validation.
 */
export function openMongoReplicationStream({
  db,
  queryProvider,
  namespaceFilter,
  isDocumentDb,
  usePostImages,
  position,
  skipInitialTimestamp = false,
  options,
  onBatch
}: {
  db: mongo.Db;
  queryProvider: MongoReplicationQueryProvider;
  namespaceFilter: { $match: mongo.Document; multipleDatabases: boolean };
  isDocumentDb: boolean;
  usePostImages: boolean;
  position: { resumeAfter?: mongo.ResumeToken | null; startAfter?: mongo.Timestamp | null } | null;
  /**
   * Legacy streaming positions exclude the initial timestamp; snapshot barrier probes must include it.
   */
  skipInitialTimestamp?: boolean;
  options: RawChangeStreamOptions;
  onBatch?: (batch: ChangeStreamBatch) => Disposable | void;
}): AsyncIterableIterator<MongoReplicationStreamBatch> {
  const parsing = {
    defaultSchema: db.databaseName,
    multipleDatabases: namespaceFilter.multipleDatabases,
    startAfter: skipInitialTimestamp && position?.resumeAfter == null ? position?.startAfter : undefined,
    signal: options.signal,
    logger: options.logger
  };

  const openRaw = async function* ({
    pipelineStages = [],
    imageOptions = {}
  }: MongoReplicationStreamOptions): AsyncIterableIterator<RawMongoReplicationStreamBatch> {
    const clusterScope = isDocumentDb || namespaceFilter.multipleDatabases;
    const streamOptions: mongo.Document = {
      ...imageOptions,
      fullDocument: !isDocumentDb && usePostImages ? 'required' : 'updateLookup',
      ...(!isDocumentDb ? { showExpandedEvents: true } : {}),
      ...(clusterScope ? { allChangesForCluster: true } : {})
    };
    // Only one resume option is legal. Legacy MongoDB snapshot positions may contain just a timestamp;
    // a fresh DocumentDB stream has neither and opens from now.
    if (position?.resumeAfter != null) {
      streamOptions.resumeAfter = position.resumeAfter;
    } else if (position?.startAfter != null) {
      streamOptions.startAtOperationTime = position.startAfter;
    }
    const pipeline = buildMongoChangeStreamPipeline({
      streamOptions,
      namespaceMatch: namespaceFilter.$match,
      pipelineStages,
      includeSplitLargeEvent: !isDocumentDb
    });
    for await (const batch of rawChangeStream(clusterScope ? db.client.db('admin') : db, pipeline, options)) {
      options.signal?.throwIfAborted();
      using batchScope = onBatch?.(batch) || undefined;
      yield { events: batch.events, resumeToken: batch.resumeToken };
    }
  };

  return queryProvider.openChangeStream({
    open: (options) => parseMongoReplicationStream({ batches: openRaw(options), ...parsing })
  });
}

/**
 * Creates a stateful parser for one raw MongoDB stream. Parses BSON entries, joins split-event
 * fragments across batches, normalizes namespaces and skips events covered by legacy timestamps.
 * After processing a batch, callers can request safe progress. Check completion on natural EOF,
 * not cancellation.
 */
export function createMongoReplicationStreamParser({
  defaultSchema,
  multipleDatabases,
  startAfter,
  signal,
  logger
}: MongoReplicationParserOptions) {
  let splitDocument: ProjectedChangeStreamDocument | null = null;
  let flexDbNameWorkaroundLogged = false;
  return {
    parse(rawEvent: Buffer): ProjectedChangeStreamDocument | null {
      signal?.throwIfAborted();
      let event = parseChangeDocument(rawEvent);
      const fragment = event.splitEvent;
      if (fragment != null) {
        // A retry can resume after a received fragment inside the raw reader. Keep the partial event in
        // memory across batches/retries, but never expose its token as a durable recovery boundary.
        const expectedFragment = splitDocument == null ? 1 : splitDocument.splitEvent!.fragment + 1;
        if (
          fragment.fragment != expectedFragment ||
          fragment.fragment > fragment.of ||
          (splitDocument != null && fragment.of != splitDocument.splitEvent!.of)
        ) {
          throw new ReplicationAssertionError(`Unexpected splitEvent: ${JSON.stringify(fragment)}`);
        }
        splitDocument = splitDocument == null ? event : Object.assign(splitDocument, event);
        if (fragment.fragment != fragment.of) {
          return null;
        }
        event = splitDocument;
        splitDocument = null;
      } else if (splitDocument != null) {
        throw new ReplicationAssertionError(`Incomplete splitEvent: ${JSON.stringify(splitDocument.splitEvent)}`);
      }

      if (startAfter != null && event.clusterTime?.lte(startAfter)) {
        return null;
      }

      if (
        !multipleDatabases &&
        'ns' in event &&
        event.ns.db != defaultSchema &&
        event.ns.db.endsWith(`_${defaultSchema}`)
      ) {
        // Atlas Flex can prefix the database name on events recorded while replication was paused.
        // Normalize before adapters inspect namespaces, just as the ordinary replication loop does.
        const originalDatabase = event.ns.db;
        event.ns.db = defaultSchema;
        if (!flexDbNameWorkaroundLogged) {
          flexDbNameWorkaroundLogged = true;
          logger?.warn(`Incorrect DB name in change stream: ${originalDatabase}. Changed to ${defaultSchema}.`);
        }
      }
      return event;
    },
    /**
     * Returns the given transport token if it is safe to persist, or undefined while a split event is unfinished.
     */
    getSafeResumeToken(resumeToken: mongo.ResumeToken) {
      return splitDocument == null ? resumeToken : undefined;
    },
    assertComplete() {
      if (splitDocument != null) {
        throw new ReplicationAssertionError(`Incomplete splitEvent: ${JSON.stringify(splitDocument.splitEvent)}`);
      }
    }
  };
}

/**
 * Wrap raw batches in lazy event iterables. BSON parsing and split-event assembly happen
 * as consumers request events, without building a parsed array. After exhaustion, each batch retains
 * its own resume-token result even when later batches change the parser state.
 * Consume each batch completely before requesting the next; returning early closes the source.
 */
export async function* parseMongoReplicationStream({
  batches,
  ...options
}: MongoReplicationParserOptions & {
  batches: AsyncIterable<RawMongoReplicationStreamBatch>;
}): AsyncGenerator<MongoReplicationStreamBatch> {
  const parser = createMongoReplicationStreamParser(options);
  for await (const sourceBatch of batches) {
    options.signal?.throwIfAborted();
    const batch = createMongoReplicationStreamBatch({
      events: parseEvents(sourceBatch.events),
      resolveResumeToken: () => parser.getSafeResumeToken(sourceBatch.resumeToken)
    });
    yield batch;
    // Split-event state is shared across batches, so the consumer must finish this one before the next is read.
    // This also covers empty batches.
    batch.getResumeToken();
  }
  parser.assertComplete();

  function* parseEvents(rawEvents: Buffer[]): Generator<MongoReplicationStreamChange> {
    for (let index = 0; index < rawEvents.length; index++) {
      const event = parser.parse(rawEvents[index]);
      if (event != null) {
        yield { event, hasBufferedChanges: index < rawEvents.length - 1 };
      }
    }
  }
}
