import type { mongo } from '@powersync/lib-service-mongodb';
import type { Logger } from '@powersync/lib-services-framework';
import type { ProjectedChangeStreamDocument } from './RawChangeStream.js';

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
   * Abort parsing when the owning replication attempt is cancelled. Resolving a batch's progress is not
   * interrupted, so consumers must check the signal themselves before persisting a token.
   */
  signal?: AbortSignal;
  /**
   * Receives the namespace-correction warning, at most once per parser instance.
   */
  logger?: Logger;
}

/**
 * A complete data change eligible for filtering. Checkpoints and DDL bypass the filter.
 */
export type MongoReplicationDataChange = Extract<
  ProjectedChangeStreamDocument,
  { operationType: 'insert' | 'update' | 'replace' | 'delete' }
>;

/**
 * The action to apply to a complete data change after evaluating filter membership.
 */
export enum MongoReplicationFilterResult {
  /**
   * Forward the original change.
   */
  KEEP = 'keep',
  /**
   * Omit the change while retaining its batch's recovery progress.
   */
  EXCLUDE = 'exclude',
  /**
   * Emit a delete because the document left the filtered set.
   */
  DELETE = 'delete'
}

/**
 * Synchronously decide how to replicate a complete data change. Checkpoints and DDL bypass this callback.
 */
export type MongoReplicationFilter = (event: MongoReplicationDataChange) => MongoReplicationFilterResult;

/**
 * Source batches and membership decisions for the shared replication filter.
 */
export interface MongoReplicationFilterOptions {
  /**
   * Ordered source batches whose boundaries and recovery progress must be preserved.
   */
  batches: AsyncIterable<MongoReplicationStreamBatch>;
  /**
   * Decide whether to keep, exclude or delete each complete data change.
   */
  filter: MongoReplicationFilter;
}
