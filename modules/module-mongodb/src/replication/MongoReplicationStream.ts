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
 * Ordered adapter output. Progress advances source recovery, not the client-visible checkpoint.
 */
export type MongoReplicationStreamItem =
  | {
      type: 'change';
      event: ProjectedChangeStreamDocument;
      /**
       * More envelopes remain in the received batch, including ones an adapter may exclude.
       */
      hasBufferedChanges: boolean;
    }
  | {
      type: 'progress';
      /**
       * Covers only complete events already delivered or intentionally excluded by the adapter.
       */
      resumeToken: mongo.ResumeToken;
      /**
       * Excluded complete events since the previous progress item; reset after every boundary.
       */
      filteredCount: number;
    };

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

export interface MongoReplicationStreamContext {
  /**
   * Open the source reader with the current position, namespace, deadlines and cancellation already bound.
   * The reader returns parsed, reassembled changes followed by safe progress boundaries. A provider can
   * add stages here and wrap the result to discard marked events or turn filter exits into deletes.
   * Supplying this callback keeps cursor setup shared with snapshot barriers and resume validation.
   *
   * The default provider simply returns open({}). Wrappers must preserve event/progress ordering and
   * close the returned iterator on completion, cancellation or failure (for await handles this).
   */
  open(options: MongoReplicationStreamOptions): AsyncIterableIterator<MongoReplicationStreamItem>;
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
}): AsyncIterableIterator<MongoReplicationStreamItem> {
  const open: MongoReplicationStreamContext['open'] = ({ pipelineStages = [], imageOptions = {} }) => {
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
    return readMongoReplicationStream({
      batches: rawChangeStream(clusterScope ? db.client.db('admin') : db, pipeline, options),
      defaultSchema: db.databaseName,
      multipleDatabases: namespaceFilter.multipleDatabases,
      // An exact token can fall inside a transaction. Later events may share its timestamp and must
      // still be delivered. Timestamp deduplication applies only to legacy positions without tokens.
      startAfter: skipInitialTimestamp && position?.resumeAfter == null ? position?.startAfter : undefined,
      signal: options.signal,
      logger: options.logger,
      onBatch
    });
  };
  return queryProvider.openChangeStream({ open });
}

/**
 * Parse envelopes once, retaining raw BSON row bodies and withholding progress during split reassembly.
 */
export async function* readMongoReplicationStream({
  batches,
  defaultSchema,
  multipleDatabases,
  startAfter,
  signal,
  logger,
  onBatch
}: {
  batches: AsyncIterable<ChangeStreamBatch>;
  defaultSchema: string;
  multipleDatabases: boolean;
  startAfter?: mongo.Timestamp | null;
  signal?: AbortSignal;
  logger?: Logger;
  /**
   * Transport accounting happens before parsing/filtering, including partial fragments and empty batches.
   */
  onBatch?: (batch: ChangeStreamBatch) => Disposable | void;
}): AsyncGenerator<MongoReplicationStreamItem> {
  let splitDocument: ProjectedChangeStreamDocument | null = null;
  let flexDbNameWorkaroundLogged = false;
  for await (const batch of batches) {
    signal?.throwIfAborted();
    // The optional scope measures batch processing, excluding the next network wait.
    using batchScope = onBatch?.(batch) || undefined;
    for (const [index, rawEvent] of batch.events.entries()) {
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
        if (fragment.fragment != fragment.of) continue;
        event = splitDocument;
        splitDocument = null;
      } else if (splitDocument != null) {
        throw new ReplicationAssertionError(`Incomplete splitEvent: ${JSON.stringify(splitDocument.splitEvent)}`);
      }

      if (startAfter != null && event.clusterTime?.lte(startAfter)) continue;

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
      yield { type: 'change', event, hasBufferedChanges: index < batch.events.length - 1 };
    }
    if (splitDocument == null) {
      yield { type: 'progress', resumeToken: batch.resumeToken, filteredCount: 0 };
    }
  }
  if (splitDocument != null) {
    throw new ReplicationAssertionError(`Incomplete splitEvent: ${JSON.stringify(splitDocument.splitEvent)}`);
  }
}
