import { isMongoNetworkTimeoutError, isMongoServerError, mongo } from '@powersync/lib-service-mongodb';
import {
  DatabaseConnectionError,
  ErrorCode,
  Logger,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import { ChangeStreamInvalidatedError } from './ChangeStream.js';

export interface RawChangeStreamOptions {
  signal?: AbortSignal;
  /**
   * How long to wait for new data per batch (max time for long-polling).
   */
  maxAwaitTimeMS: number;
  /**
   * Timeout for the initial aggregate command.
   */
  maxTimeMS: number;
  batchSize: number;

  /**
   * Mostly for testing.
   */
  collection?: string;

  logger?: Logger;
}

export interface ChangeStreamBatch {
  resumeToken: mongo.ResumeToken;
  events: Buffer[];
  /**
   * Size in bytes of this event.
   */
  byteSize: number;
}

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

  // > If the server supports sessions, the resume attempt MUST use the same session as the previous attempt's command.
  const session = db.client.startSession();
  await using _ = { [Symbol.asyncDispose]: () => session.endSession() };

  while (true) {
    options.signal?.throwIfAborted();
    try {
      let innerPipeline = pipeline;
      if (lastResumeToken != null) {
        const [first, ...rest] = pipeline;
        const options = { ...first.$changeStream };
        delete options.startAtOperationTime;
        delete options.startAfter;
        options.resumeAfter = lastResumeToken;
        innerPipeline = [{ $changeStream: options }, ...rest];
      }
      const inner = rawChangeStreamInner(session, db, innerPipeline, {
        ...options
      });
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
  options: RawChangeStreamOptions
): AsyncGenerator<ChangeStreamBatch> {
  // See specs:
  // https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md

  let cursorId: bigint | null = null;

  /**
   * Typically '$cmd.aggregate', but we need to use the ns from the cursor.
   */
  let nsCollection: string | null = null;

  const maxTimeMS = options.maxAwaitTimeMS;
  const batchSize = options.batchSize;
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
      // Step 1: Send the aggregate command to start the change stream
      const aggregateResult = await db
        .command(
          {
            aggregate: collection,
            pipeline,
            cursor: { batchSize },
            maxTimeMS: options.maxTimeMS
          },
          { session, raw: true }
        )
        .catch((e) => {
          throw mapChangeStreamError(e);
        });

      const cursor = mongo.BSON.deserialize(aggregateResult.cursor, {
        useBigInt64: true,
        fieldsAsRaw: { firstBatch: true }
      });

      cursorId = BigInt(cursor.id);
      nsCollection = namespaceCollection(cursor.ns);

      let batch = cursor.firstBatch;

      if (cursor.postBatchResumeToken == null) {
        // Deviation from spec: We require that the server always returns a postBatchResumeToken.
        // postBatchResumeToken is returned in MongoDB 4.0.7 and later, and we support 6.0+
        throw new ReplicationAssertionError(`postBatchResumeToken from aggregate response`);
      }

      yield { events: batch, resumeToken: cursor.postBatchResumeToken, byteSize: aggregateResult.cursor.byteLength };
    }

    // Step 2: Poll using getMore until the cursor is closed
    while (cursorId && cursorId !== 0n) {
      options.signal?.throwIfAborted();

      const getMoreResult: mongo.Document = await db
        .command(
          {
            getMore: cursorId,
            collection: nsCollection,
            batchSize,
            maxTimeMS
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
            throw new ResumableChangeStreamError(e.message, { cause: e });
          }
          throw mapChangeStreamError(e);
        });

      const cursor = mongo.BSON.deserialize(getMoreResult.cursor, {
        useBigInt64: true,
        fieldsAsRaw: { nextBatch: true }
      });
      cursorId = BigInt(cursor.id);
      const nextBatch = cursor.nextBatch;

      if (cursor.postBatchResumeToken == null) {
        // Deviation from spec: We require that the server always returns a postBatchResumeToken.
        // postBatchResumeToken is returned in MongoDB 4.0.7 and later, and we support 6.0+
        throw new ReplicationAssertionError(`postBatchResumeToken from aggregate response`);
      }
      yield { events: nextBatch, resumeToken: cursor.postBatchResumeToken, byteSize: getMoreResult.cursor.byteLength };
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
  const doc = mongo.BSON.deserialize(buffer, { useBigInt64: true, raw: true }) as ChangeStreamDocumentInput;
  // We update the document in-place
  doc._id = mongo.BSON.deserialize(doc._id) as any;
  if (doc.lsid != null) {
    doc.lsid = mongo.BSON.deserialize(doc.lsid) as any;
  }
  if (doc.splitEvent != null) {
    doc.splitEvent = mongo.BSON.deserialize(doc.splitEvent) as any;
  }

  if ('ns' in doc) {
    doc.ns = mongo.BSON.deserialize(doc.ns) as any;
  }
  if ('to' in doc) {
    doc.to = mongo.BSON.deserialize(doc.to) as any;
  }
  if ('documentKey' in doc) {
    doc.documentKey = mongo.BSON.deserialize(doc.documentKey, { useBigInt64: true }) as any;
  }
  return doc as any;
}

function isResumableChangeStreamError(e: unknown) {
  // See: https://github.com/mongodb/specifications/blob/master/source/change-streams/change-streams.md#resumable-error
  if (!isMongoServerError(e)) {
    // Any error encountered which is not a server error (e.g. a timeout error or network error)
    return true;
  } else if (e.codeName == 'CursorNotFound') {
    // A server error with code 43 (CursorNotFound)
    return true;
  } else if (e.hasErrorLabel('ResumableChangeStreamError')) {
    // For servers with wire version 9 or higher (server version 4.4 or higher), any server error with the ResumableChangeStreamError error label.
    return true;
  } else {
    // We ignore servers with wire version less than 9, since we only support MongoDB 6.0+.
    return false;
  }
}

export function mapChangeStreamError(e: unknown) {
  if (isMongoNetworkTimeoutError(e)) {
    // This typically has an unhelpful message like "connection 2 to 159.41.94.47:27017 timed out".
    // We wrap the error to make it more useful.
    throw new DatabaseConnectionError(ErrorCode.PSYNC_S1345, `Timeout while reading MongoDB ChangeStream`, e);
  } else if (isMongoServerError(e) && e.codeName == 'MaxTimeMSExpired') {
    // maxTimeMS was reached. Example message:
    // MongoServerError: Executor error during aggregate command on namespace: powersync_test_data.$cmd.aggregate :: caused by :: operation exceeded time limit
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
