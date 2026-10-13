import { mongo } from '@powersync/lib-service-mongodb';
import { Logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { buildMongoChangeStreamPipeline, MongoReplicationQueryProvider } from './MongoReplicationQueryProvider.js';
import {
  ChangeStreamBatch,
  parseChangeDocument,
  ProjectedChangeStreamDocument,
  rawChangeStream,
  RawChangeStreamOptions
} from './RawChangeStream.js';

/**
 * An unparsed MongoDB response batch. Consumers parse its entries incrementally and retain unfinished
 * split events across batches before deciding whether its resume token is safe to persist.
 */
export interface RawMongoReplicationStreamBatch {
  /**
   * Original BSON entries in source order, including possible split-event fragments.
   */
  events: Buffer[];
  /**
   * Transport position after this batch. It may cover an incomplete event and is not yet safe progress.
   */
  resumeToken: mongo.ResumeToken;
}

/**
 * A complete event and its position within the original source batch.
 */
export interface MongoReplicationStreamChange {
  event: ProjectedChangeStreamDocument;
  /**
   * MongoDB returned more entries after this event, even if a provider later excludes them or the
   * parser retains them as unfinished split fragments. For [checkpoint, excluded change], preserve
   * true on the checkpoint so ChangeStream groups commits instead of treating it as caught up.
   * Providers must preserve this value when forwarding or transforming events.
   */
  hasBufferedChanges: boolean;
}

/**
 * Statistics for one fully consumed batch, independent of whether its resume token is safe.
 */
export interface MongoReplicationStreamBatchMetadata {
  /**
   * Complete events excluded from this batch. Null means no filtering was applied; zero means
   * filtering was applied but no events were excluded. Counts do not carry across batches.
   */
  readonly filteredCount: number | null;
}

/**
 * A single-use batch of complete events, parsed or filtered lazily as the consumer iterates.
 * Exhaust events before reading its resume token, metadata or the next batch.
 */
export interface MongoReplicationStreamBatch {
  /**
   * Complete changes in source order, parsed or filtered lazily during synchronous iteration.
   * Consume once and fully. It may be empty after filtering or while assembling a split event.
   */
  events: Iterable<MongoReplicationStreamChange>;
  /**
   * Returns the safe resume token after events are fully consumed, or undefined while a split event
   * is unfinished. Throws before exhaustion. The result remains stable after later batches.
   */
  getResumeToken(): mongo.ResumeToken | undefined;
  /**
   * Final statistics for this batch, available even without a safe resume token.
   * Throws before events are fully consumed; the result is immutable and cached afterward.
   */
  readonly metadata: MongoReplicationStreamBatchMetadata;
}

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
    if (!complete) {
      throw new ReplicationAssertionError('Consume all batch events before requesting token or metadata');
    }
  }
  return {
    events: {
      [Symbol.iterator]() {
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
    yield* events;
    resumeToken = resolveResumeToken();
    metadata = Object.freeze({ ...resolveMetadata() });
    complete = true;
  }
}

/**
 * Provider-controlled additions to the shared cursor setup. Source scope, resume position, post-image
 * requirements and cancellation remain controlled by the MongoDB replication module.
 */
export interface MongoReplicationStreamOptions {
  /**
   * Inserted after namespace selection and before the final large-event split stage.
   */
  pipelineStages?: mongo.Document[];
  /**
   * Request pre-images without overriding the reader's post-image, resume or scope options.
   */
  imageOptions?: Pick<mongo.ChangeStreamOptions, 'fullDocumentBeforeChange'>;
}

/**
 * Bound source opener supplied to a query provider for one stream invocation.
 * Providers may wrap the lazy event iterables to filter or transform complete changes.
 */
export interface MongoReplicationStreamContext {
  /**
   * Open batches whose events are parsed lazily, with cursor scope, position, cancellation and
   * accounting already bound. Each call owns independent split-event assembly and parser state.
   * Events are complete; progress is withheld while a split event is unfinished.
   * Close the iterator on completion, cancellation or failure (for await handles this).
   */
  open(options: MongoReplicationStreamOptions): AsyncIterableIterator<MongoReplicationStreamBatch>;
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
 * Source context for lazy parsing of raw BSON events in default and filtering providers.
 * These options control namespace normalization, legacy timestamp skipping and cancellation; split-event
 * state belongs to each parser instance and must not be shared between independently opened streams.
 */
export interface MongoReplicationParserOptions {
  /**
   * Expected database name when correcting Atlas Flex's prefixed namespaces in single-database mode.
   */
  defaultSchema: string;
  /**
   * Preserve database names when the stream covers multiple databases instead of normalizing them.
   */
  multipleDatabases: boolean;
  /**
   * Exclude complete events at or before this legacy resume timestamp. Leave unset for exact-token
   * resumes, where later events can share the same timestamp, and for snapshot barrier probes.
   */
  startAfter?: mongo.Timestamp | null;
  /**
   * Abort parsing and progress evaluation when the owning replication attempt is cancelled.
   */
  signal?: AbortSignal;
  /**
   * Receives the namespace-correction warning, at most once per parser instance.
   */
  logger?: Logger;
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
  const state: { splitDocument: ProjectedChangeStreamDocument | null } = { splitDocument: null };
  let flexDbNameWorkaroundLogged = false;
  return {
    parse(rawEvent: Buffer): ProjectedChangeStreamDocument | null {
      signal?.throwIfAborted();
      let event = parseChangeDocument(rawEvent);
      const fragment = event.splitEvent;
      if (fragment != null) {
        // A retry can resume after a received fragment inside the raw reader. Keep the partial event in
        // memory across batches/retries, but never expose its token as a durable recovery boundary.
        const expectedFragment = state.splitDocument == null ? 1 : state.splitDocument.splitEvent!.fragment + 1;
        if (
          fragment.fragment != expectedFragment ||
          fragment.fragment > fragment.of ||
          (state.splitDocument != null && fragment.of != state.splitDocument.splitEvent!.of)
        ) {
          throw new ReplicationAssertionError(`Unexpected splitEvent: ${JSON.stringify(fragment)}`);
        }
        state.splitDocument = state.splitDocument == null ? event : Object.assign(state.splitDocument, event);
        if (fragment.fragment != fragment.of) return null;
        event = state.splitDocument;
        state.splitDocument = null;
      } else if (state.splitDocument != null) {
        throw new ReplicationAssertionError(`Incomplete splitEvent: ${JSON.stringify(state.splitDocument.splitEvent)}`);
      }

      if (startAfter != null && event.clusterTime?.lte(startAfter)) return null;

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
    getResumeToken(resumeToken: mongo.ResumeToken) {
      signal?.throwIfAborted();
      return state.splitDocument == null ? resumeToken : undefined;
    },
    assertComplete() {
      if (state.splitDocument != null) {
        throw new ReplicationAssertionError(`Incomplete splitEvent: ${JSON.stringify(state.splitDocument.splitEvent)}`);
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
      events: parseEvents(sourceBatch),
      resolveResumeToken: () => parser.getResumeToken(sourceBatch.resumeToken)
    });
    yield batch;
    // Validate consumption before requesting the next source batch, including empty batches.
    batch.getResumeToken();
  }
  parser.assertComplete();

  /**
   * Parses raw entries only as the consumer requests the next complete event, without building a parsed array.
   */
  function* parseEvents(sourceBatch: RawMongoReplicationStreamBatch): Generator<MongoReplicationStreamChange> {
    for (let index = 0; index < sourceBatch.events.length; index++) {
      const event = parser.parse(sourceBatch.events[index]);
      if (event != null) {
        yield { event, hasBufferedChanges: index < sourceBatch.events.length - 1 };
      }
    }
  }
}
