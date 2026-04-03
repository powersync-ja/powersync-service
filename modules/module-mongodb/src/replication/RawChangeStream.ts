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
}

export interface ChangeStreamBatch {
  resumeToken: mongo.ResumeToken;
  events: mongo.ChangeStreamDocument[];
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
  let abortPromise: Promise<any> | null = null;

  options.signal?.addEventListener('abort', () => {
    if (cursorId != null && cursorId !== 0n && nsCollection != null) {
      // This would result in a CursorKilled error.
      abortPromise = db.command({
        killCursors: nsCollection,
        cursors: [cursorId]
      });
    }
  });

  const session = db.client.startSession();
  try {
    // Step 1: Send the aggregate command to start the change stream
    const aggregateResult = await db
      .command(
        {
          aggregate: 1,
          pipeline,
          cursor: { batchSize },
          maxTimeMS: options.maxTimeMS
        },
        { session, raw: false }
      )
      .catch((e) => {
        throw mapChangeStreamError(e);
      });

    cursorId = BigInt(aggregateResult.cursor.id);
    nsCollection = namespaceCollection(aggregateResult.cursor.ns);

    let batch = aggregateResult.cursor.firstBatch;

    yield { events: batch, resumeToken: aggregateResult.cursor.postBatchResumeToken };

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
          { session, raw: false }
        )
        .catch((e) => {
          throw mapChangeStreamError(e);
        });

      cursorId = BigInt(getMoreResult.cursor.id);
      const nextBatch = getMoreResult.cursor.nextBatch;

      yield { events: nextBatch, resumeToken: getMoreResult.cursor.postBatchResumeToken };
    }
  } finally {
    if (abortPromise != null) {
      await abortPromise;
    }
    if (cursorId != null && cursorId !== 0n && abortPromise != null) {
      await db.command({
        killCursors: nsCollection,
        cursors: [cursorId]
      });
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
