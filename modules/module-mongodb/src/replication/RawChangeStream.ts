import { isMongoNetworkTimeoutError, isMongoServerError, mongo } from '@powersync/lib-service-mongodb';
import {
  DatabaseConnectionError,
  ErrorCode,
  Logger,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import { PerformanceTracer } from '@powersync/service-core';
import { ChangeStreamInvalidatedError } from './ChangeStream.js';

export interface RawChangeStreamOptions {
  signal?: AbortSignal;

  /**
   * How long to wait for new data per batch (max time for long-polling).
   *
   * This is used for maxTimeMS for the getMore command.
   */
  maxAwaitTimeMS: number;

  /**
   * Timeout for the initial aggregate command.
   */
  maxTimeMS: number;

  /**
   * batchSize for the getMore commands.
   *
   * The aggregate command always uses a batchSize of 1.
   *
   * After a timeout error, the batchSize will be reduced and ramped up again.
   */
  batchSize: number;

  /**
   * Mostly for testing.
   */
  collection?: string;

  logger?: Logger;

  tracer?: PerformanceTracer<'changestream'>;
}

export interface ChangeStreamBatch {
  resumeToken: mongo.ResumeToken;
  events: Buffer[];
  /**
   * Size in bytes of this event.
   */
  byteSize: number;

  /**
   * Time in milliseconds that we waited for a response from MongoDB.
   *
   * This includes:
   * 1. Time to send the command.
   * 2. Time MongoDB waits for new data to be available.
   * 3. Time MongoDB scans through the oplog.
   * 4. Time to send the data back over the network, and parse the outer metadata.
   */
  commandDuration: number;
}

const deserialize = mongo.BSON.deserialize;
const DESERIALIZE_DEFAULT = { useBigInt64: true };
const DESERIALIZE_RAW: mongo.BSON.DeserializeOptions = {
  ...DESERIALIZE_DEFAULT,
  raw: true,
  // No need to validate utf8 for the core change stream fields
  validation: { utf8: false }
};
const DESERIALIZE_CHANGE_STREAM = {
  ...DESERIALIZE_DEFAULT,
  // These are embedded _arrays_ that we want to preserve as buffers
  fieldsAsRaw: { firstBatch: true, nextBatch: true },
  // No need to validate utf8 for the core change stream response
  validation: { utf8: false }
};

export async function* rawChangeStream(db: mongo.Db, pipeline: mongo.Document[], options: RawChangeStreamOptions) {
  // We generally attempt to follow the spec at: https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md
  // Intentional differences:
  // 1. We don't attempt to follow the client API.
  // 2. We require that `postBatchResumeToken` is present.
  // 3. We don't attempt to handle resumeToken from individual documents - the consumer can handle that.
  if (!('$changeStream' in pipeline[0])) {
    throw new ReplicationAssertionError(`First pipeline stage must be $changeStream`);
  }
  let lastResumeToken: unknown | null = null;

  const batchSizer = new AdaptiveBatchSize(options.batchSize);

  // > If the server supports sessions, the resume attempt MUST use the same session as the previous attempt's command.
  const session = db.client.startSession();
  await using _ = { [Symbol.asyncDispose]: () => session.endSession() };

  while (true) {
    options.signal?.throwIfAborted();
    try {
      let innerPipeline = pipeline;
      if (lastResumeToken != null) {
        const [first, ...rest] = pipeline;
        const changeStreamStage = { ...first.$changeStream };
        delete changeStreamStage.startAtOperationTime;
        delete changeStreamStage.startAfter;
        changeStreamStage.resumeAfter = lastResumeToken;
        innerPipeline = [{ $changeStream: changeStreamStage }, ...rest];
      }
      const inner = rawChangeStreamInner(
        session,
        db,
        innerPipeline,
        {
          ...options
        },
        batchSizer
      );
      for await (let batch of inner) {
        yield batch;
        lastResumeToken = batch.resumeToken;
      }
    } catch (e) {
      if (e instanceof ResumableChangeStreamError) {
        // This is only triggered on the getMore command.
        // If there is a persistent error, we expect it to occur on the aggregate command as well,
        // which will trigger a hard error on the next attempt.
        // This matches the change stream spec of:
        // > A change stream MUST attempt to resume a single time if it encounters any resumable error per Resumable Error. A change stream MUST NOT attempt to resume on any other type of error.
        // > An error on an aggregate command is not a resumable error. Only errors on a getMore command may be considered resumable errors.
        // https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md#resume-process

        // Technically we don't _need_ this resume functionality - we can let the replication job handle the failure
        // and restart. However, this provides a faster restart path in common cases.
        options.logger?.warn(`Resumable change stream error, retrying: ${e.message}`, e.cause);
        continue;
      } else {
        throw e;
      }
    }
  }
}

async function* rawChangeStreamInner(
  session: mongo.ClientSession,
  db: mongo.Db,
  pipeline: mongo.Document[],
  options: RawChangeStreamOptions,
  batchSizer: AdaptiveBatchSize
): AsyncGenerator<ChangeStreamBatch> {
  // See specs:
  // https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md

  let cursorId: bigint | null = null;

  /**
   * Typically '$cmd.aggregate', but we need to use the ns from the cursor.
   */
  let nsCollection: string | null = null;

  const collection = options.collection ?? 1;
  let abortPromise: Promise<any> | null = null;

  const onAbort = () => {
    if (cursorId != null && cursorId !== 0n && nsCollection != null) {
      // This would result in a CursorKilled error.
      abortPromise = db
        .command({
          killCursors: nsCollection,
          cursors: [cursorId]
        })
        .catch(() => {});
    }
  };
  options.signal?.addEventListener('abort', onAbort);

  try {
    {
      const start = performance.now();
      using aggregateSpan = options.tracer?.span('changestream', 'aggregate');
      // Step 1: Send the aggregate command to start the change stream
      const aggregateResult = await db
        .command(
          {
            aggregate: collection,
            pipeline,
            cursor: {
              // Always use batchSize of 1 for the initial aggregate command,
              // to maximize the chance of success in case of timeouts.
              batchSize: 1
            },
            maxTimeMS: options.maxTimeMS
          },
          { session, raw: true }
        )
        .catch((e) => {
          throw mapChangeStreamError(e);
        });

      const aggregateDuration = performance.now() - start;
      aggregateSpan?.end();

      const cursor = deserialize(aggregateResult.cursor, DESERIALIZE_CHANGE_STREAM);

      cursorId = BigInt(cursor.id);
      nsCollection = namespaceCollection(cursor.ns);

      let batch = cursor.firstBatch;

      if (cursor.postBatchResumeToken == null) {
        // Deviation from spec: We require that the server always returns a postBatchResumeToken.
        // postBatchResumeToken is returned in MongoDB 4.0.7 and later, and we support 6.0+
        throw new ReplicationAssertionError(`postBatchResumeToken from aggregate response`);
      }

      yield {
        events: batch,
        resumeToken: cursor.postBatchResumeToken,
        byteSize: aggregateResult.cursor.byteLength,
        commandDuration: aggregateDuration
      };
    }

    // Step 2: Poll using getMore until the cursor is closed
    while (cursorId && cursorId !== 0n) {
      options.signal?.throwIfAborted();

      const start = performance.now();
      using commandSpan = options.tracer?.span('changestream', 'getmore');
      const getMoreResult: mongo.Document = await db
        .command(
          {
            getMore: cursorId,
            collection: nsCollection,
            batchSize: batchSizer.next(),
            maxTimeMS: options.maxAwaitTimeMS
          },
          { session, raw: true }
        )
        .catch((e) => {
          if (isMongoServerError(e) && e.codeName == 'CursorKilled') {
            // This may be due to the killCursors command issued when aborting.
            // In that case, use the abort error instead.
            options.signal?.throwIfAborted();
          }

          if (isResumableChangeStreamError(e)) {
            if (isTimeoutError(e)) {
              batchSizer.reduceAfterError();
            }
            throw new ResumableChangeStreamError(e.message, { cause: e });
          }
          throw mapChangeStreamError(e);
        });

      const getMoreDuration = performance.now() - start;
      commandSpan?.end();

      const cursor = deserialize(getMoreResult.cursor, DESERIALIZE_CHANGE_STREAM);
      cursorId = BigInt(cursor.id);
      const nextBatch = cursor.nextBatch;

      if (cursor.postBatchResumeToken == null) {
        // Deviation from spec: We require that the server always returns a postBatchResumeToken.
        // postBatchResumeToken is returned in MongoDB 4.0.7 and later, and we support 6.0+
        throw new ReplicationAssertionError(`postBatchResumeToken from aggregate response`);
      }
      yield {
        events: nextBatch,
        resumeToken: cursor.postBatchResumeToken,
        byteSize: getMoreResult.cursor.byteLength,
        commandDuration: getMoreDuration
      };
    }

    options.signal?.throwIfAborted();
    throw new ReplicationAssertionError(`Change stream ended unexpectedly`);
  } finally {
    options.signal?.removeEventListener('abort', onAbort);
    if (abortPromise != null) {
      // killCursors is already sent - we wait for the response in abortPromise.
      await abortPromise;
    } else if (cursorId != null && cursorId !== 0n && nsCollection != null) {
      await db
        .command({
          killCursors: nsCollection,
          cursors: [cursorId]
        })
        .catch(() => {});
    }
  }
}

class ResumableChangeStreamError extends Error {}

/**
 * After a timeout error, we reduce the batch size to this number, then multiply by this for each batch.
 *
 * Must be an integer >= 2 with the current implementation.
 */
const BATCH_SIZE_MULTIPLIER = 2;

/**
 * Manage batch sizes after timeout errors.
 *
 * This starts with the initial batch size.
 *
 * After a timeout error, we reduce the batch size for aggregate command to 1,
 * then multiply by BATCH_SIZE_MULTIPLIER for each subsequent batch, until we reach the initial batch size again.
 *
 * We use this to protect against timeout errors:
 *   [PSYNC_S1345] Timeout while reading MongoDB ChangeStream
 *
 * When we run into that, the stream is restarted automatically. starting with an aggregate command
 * with batchSize: 1.
 *
 * We then ramp up the batchSize for getMore commands.
 */
class AdaptiveBatchSize {
  private nextBatchSize: number;

  constructor(private maxBatchSize: number) {
    this.nextBatchSize = maxBatchSize;
  }

  /**
   * Get the next batchSize for a getMore command.
   */
  next() {
    const current = this.nextBatchSize;
    this.nextBatchSize = Math.min(this.maxBatchSize, this.nextBatchSize * BATCH_SIZE_MULTIPLIER);
    return current;
  }

  /**
   * After a timeout error, the next aggregate command will start with a batchSize of 1.
   *
   * The next getMore will then have a batchSize of BATCH_SIZE_MULTIPLIER.
   */
  reduceAfterError() {
    this.nextBatchSize = BATCH_SIZE_MULTIPLIER;
  }
}

type RawBsonValue =
  | string
  | number
  | boolean
  | bigint
  | null
  | undefined
  | Date
  | Buffer
  | mongo.Binary
  | mongo.ObjectId
  | mongo.Timestamp
  | mongo.Long
  | mongo.Int32
  | mongo.Double
  | mongo.Decimal128
  | mongo.MaxKey
  | mongo.MinKey
  | mongo.BSONRegExp
  | mongo.BSONSymbol
  | mongo.Code
  | mongo.DBRef
  | mongo.UUID;

type Rawify<T> = T extends RawBsonValue
  ? T
  : T extends (infer U)[]
    ? Rawify<U>[]
    : T extends readonly (infer U)[]
      ? readonly Rawify<U>[]
      : Buffer;

/**
 * Converts the type as parsed using {raw: true}
 */
type MapRawDocument<T> = T extends unknown ? { [K in keyof T]: Rawify<T[K]> } : never;

export type ProjectedChangeStreamDocument =
  | Omit<mongo.ChangeStreamDropDocument, 'wallTime' | 'collectionUUID'>
  | Omit<mongo.ChangeStreamRenameDocument, 'wallTime'>
  | Omit<mongo.ChangeStreamDeleteDocument<Buffer>, 'wallTime'>
  | Omit<mongo.ChangeStreamInsertDocument<Buffer>, 'wallTime'>
  | Omit<mongo.ChangeStreamUpdateDocument<Buffer>, 'wallTime' | 'updateDescription'>
  | Omit<mongo.ChangeStreamReplaceDocument<Buffer>, 'wallTime'>;

type ChangeStreamDocumentInput = MapRawDocument<ProjectedChangeStreamDocument>;

/**
 * Parse a change stream document, while keeping `fullDocument` as a Buffer.
 *
 * @param Buffer the raw change stream document
 */
export function parseChangeDocument(buffer: Buffer): ProjectedChangeStreamDocument {
  const doc = deserialize(buffer, DESERIALIZE_RAW) as ChangeStreamDocumentInput;
  // We update the document in-place
  doc._id = deserialize(doc._id, DESERIALIZE_DEFAULT) as any;
  if (doc.lsid != null) {
    doc.lsid = deserialize(doc.lsid, DESERIALIZE_DEFAULT) as any;
  }
  if (doc.splitEvent != null) {
    doc.splitEvent = deserialize(doc.splitEvent, DESERIALIZE_DEFAULT) as any;
  }

  if ('ns' in doc) {
    doc.ns = deserialize(doc.ns, DESERIALIZE_DEFAULT) as any;
  }
  if ('to' in doc) {
    doc.to = deserialize(doc.to, DESERIALIZE_DEFAULT) as any;
  }
  if ('documentKey' in doc) {
    doc.documentKey = deserialize(doc.documentKey, DESERIALIZE_DEFAULT) as any;
  }
  return doc as any;
}

function isTimeoutError(e: unknown) {
  return isMongoNetworkTimeoutError(e) || (isMongoServerError(e) && e.codeName == 'MaxTimeMSExpired');
}

function isResumableChangeStreamError(e: unknown) {
  // See: https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md#resumable-error
  if (!isMongoServerError(e)) {
    // Any error encountered which is not a server error (e.g. a socket timeout error or network error)
    return true;
  } else if (e.codeName == 'CursorNotFound') {
    // A server error with code 43 (CursorNotFound)
    return true;
  } else if (e.hasErrorLabel('ResumableChangeStreamError')) {
    // For servers with wire version 9 or higher (server version 4.4 or higher), any server error with the ResumableChangeStreamError error label.
    return true;
  } else if (e.codeName == 'MaxTimeMSExpired') {
    // Our own exception for MaxTimeMSExpired.
    // This can help us retry faster, with a smaller batch size (if initialBatchSize is set to 1), which should hopefully avoid the timeout.
    return true;
  } else {
    // Other errors are not retried.
    // We ignore the spec for servers with wire version less than 9, since we only support MongoDB 6.0+.
    return false;
  }
}

export function mapChangeStreamError(e: unknown) {
  if (isTimeoutError(e)) {
    // For isMongoNetworkTimeoutError():
    //   This typically has an unhelpful message like "connection 2 to 159.41.94.47:27017 timed out".
    //   We wrap the error to make it more useful.
    // Example for MaxTimeMSExpired:
    //   MongoServerError: Executor error during aggregate command on namespace: powersync_test_data.$cmd.aggregate :: caused by :: operation exceeded time limit
    throw new DatabaseConnectionError(ErrorCode.PSYNC_S1345, `Timeout while reading MongoDB ChangeStream`, e);
  } else if (
    isMongoServerError(e) &&
    e.codeName == 'NoMatchingDocument' &&
    e.errmsg?.includes('post-image was not found')
  ) {
    throw new ChangeStreamInvalidatedError(e.errmsg, e);
  } else if (isMongoServerError(e) && e.hasErrorLabel('NonResumableChangeStreamError')) {
    throw new ChangeStreamInvalidatedError(e.message, e);
  } else {
    throw new DatabaseConnectionError(ErrorCode.PSYNC_S1346, `Error reading MongoDB ChangeStream`, e);
  }
}

/**
 * Get the "collection" from a ns.
 *
 * This drops everything before the first . character.
 *
 * "my_db_name.$cmd.aggregate" -> "$cmd.aggregate"
 */
export function namespaceCollection(ns: string): string {
  const dot = ns.indexOf('.');
  return ns.substring(dot + 1);
}
