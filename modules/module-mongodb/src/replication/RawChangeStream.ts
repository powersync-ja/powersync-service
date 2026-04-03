import { isMongoNetworkTimeoutError, isMongoServerError, mongo } from '@powersync/lib-service-mongodb';
import { DatabaseConnectionError, ErrorCode } from '@powersync/lib-services-framework';
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
}

export interface ChangeStreamBatch {
  resumeToken: mongo.ResumeToken;
  events: Buffer[];
  /**
   * Size in bytes of this event.
   */
  byteSize: number;
}

export async function* rawChangeStream(
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

  options.signal?.addEventListener('abort', () => {
    if (cursorId != null && cursorId !== 0n && nsCollection != null) {
      // This would result in a CursorKilled error.
      abortPromise = db
        .command({
          killCursors: nsCollection,
          cursors: [cursorId]
        })
        .catch(() => {});
    }
  });

  const session = db.client.startSession();
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

      yield { events: batch, resumeToken: cursor.postBatchResumeToken, byteSize: aggregateResult.cursor.byteLength };
    }

    // Step 2: Poll using getMore until the cursor is closed
    while (cursorId && cursorId !== 0n) {
      if (options.signal?.aborted) {
        break;
      }
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
          throw mapChangeStreamError(e);
        });

      const cursor = mongo.BSON.deserialize(getMoreResult.cursor, {
        useBigInt64: true,
        fieldsAsRaw: { nextBatch: true }
      });
      cursorId = BigInt(cursor.id);
      const nextBatch = cursor.nextBatch;

      yield { events: nextBatch, resumeToken: cursor.postBatchResumeToken, byteSize: getMoreResult.cursor.byteLength };
    }
  } finally {
    if (abortPromise != null) {
      await abortPromise;
    }
    if (cursorId != null && cursorId !== 0n && abortPromise != null) {
      await db
        .command({
          killCursors: nsCollection,
          cursors: [cursorId]
        })
        .catch(() => {});
    }
    await session.endSession();
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
