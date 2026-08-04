import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { Logger, ReplicationAbortedError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { storage, utils } from '@powersync/service-core';
import * as bson from 'bson';
import * as crypto from 'crypto';
import * as timers from 'node:timers/promises';
import * as uuid from 'uuid';
import { BucketDataDoc } from '../storage/implementation/common/BucketDataDoc.js';
import { DEFAULT_CLEAR_BATCH_THROTTLE_RATE } from '../types/types.js';

/**
 * Default and max batch size for clearing.
 */
const CLEAR_BATCH_SIZE = 10_000;

/**
 * Minimum batch size for clearing when a batch takes too long.
 */
const CLEAR_MIN_BATCH_SIZE = 100;

/**
 * We target batch duration to stay between the min and max. If the druation is outside this range,
 * we increase or decrease the batch size.
 *
 * MONGO_CLEAR_OPERATION_TIMEOUT_MS controls the absolute max of individual find and delete operations,
 * but we try to avoid hitting that.
 */
const CLEAR_BATCH_MIN_DURATION_MS = 500;
const CLEAR_BATCH_MAX_DURATION_MS = 2000;

type ClearCollectionFilter<T extends mongo.Document> = mongo.Filter<T> & {
  _id: mongo.FilterOperators<mongo.InferIdType<T>>;
};

function throwIfClearAborted(signal?: AbortSignal): void {
  if (signal?.aborted) {
    throw new ReplicationAbortedError('Aborted clearing data', signal.reason);
  }
}

async function waitWithSignal(delayMs: number, signal: AbortSignal | undefined, abortMessage: string): Promise<void> {
  try {
    await timers.setTimeout(delayMs, undefined, { signal });
  } catch (error) {
    if (signal?.aborted) {
      throw new ReplicationAbortedError(abortMessage, signal.reason);
    }
    throw error;
  }
}

export function idPrefixFilter<T>(prefix: Partial<T>, rest: (keyof T)[]): mongo.FilterOperators<T> {
  let filter = {
    $gte: {
      ...prefix
    } as any,
    $lt: {
      ...prefix
    } as any
  };

  for (let key of rest) {
    filter.$gte[key] = new bson.MinKey();
    filter.$lt[key] = new bson.MaxKey();
  }

  return filter;
}

export function generateReplicationStreamName(prefix: string, replicationStreamId: number) {
  const slot_suffix = crypto.randomBytes(2).toString('hex');
  return `${prefix}${replicationStreamId}_${slot_suffix}`;
}

/**
 * Read a single batch of data from a cursor, then close it.
 *
 * We do our best to avoid MongoDB fetching any more data than this single batch.
 *
 * This is similar to using `singleBatch: true` in find options.
 * However, that makes `has_more` detection very difficult, since the cursor is always closed
 * after the first batch. Instead, we do a workaround to only fetch a single batch below.
 *
 * For this to be effective, set batchSize = limit + 1 in the find command.
 */
export async function readSingleBatch<T>(cursor: mongo.AbstractCursor<T>): Promise<{ data: T[]; hasMore: boolean }> {
  try {
    let data: T[];
    let hasMore = true;
    // Let MongoDB load the first batch of data
    const hasAny = await cursor.hasNext();
    // Now it's in memory, and we can read it
    data = cursor.readBufferedDocuments();
    if (!hasAny || cursor.id?.isZero()) {
      // A zero id means the cursor is exhaused.
      // No results (hasAny == false) means even this batch doesn't have data.
      // This should similar results as `await cursor.hasNext()`, but without
      // actually fetching the next batch.
      //
      // Note that it is safe (but slightly inefficient) to return `hasMore: true`
      // without there being more data, as long as the next batch
      // will return `hasMore: false`.
      hasMore = false;
    }
    return { data, hasMore };
  } finally {
    // Match the from the cursor iterator logic here:
    // https://github.com/mongodb/node-mongodb-native/blob/e02534e7d1c627bf50b85ca39f5995dbf165ad44/src/cursor/abstract_cursor.ts#L327-L331
    if (!cursor.closed) {
      await cursor.close();
    }
  }
}

export function mapOpEntry(row: BucketDataDoc): utils.OplogEntry {
  if (row.op == 'PUT' || row.op == 'REMOVE') {
    return {
      op_id: utils.internalToExternalOpId(row.o),
      op: row.op,
      object_type: row.table,
      object_id: row.row_id,
      checksum: Number(row.checksum),
      subkey: replicaIdToSubkey(row.source_table!, row.source_key!),
      data: row.data
    };
  } else {
    // MOVE, CLEAR

    return {
      op_id: utils.internalToExternalOpId(row.o),
      op: row.op,
      checksum: Number(row.checksum)
    };
  }
}

export function replicaIdToSubkey(table: storage.SourceTableId, id: storage.ReplicaId): string {
  if (storage.isUUID(id)) {
    // Special case for UUID for backwards-compatiblity
    return `${tableIdString(table)}/${id.toHexString()}`;
  } else {
    // Hashed UUID from the table and id
    const repr = bson.serialize({ table, id });
    return uuid.v5(repr, utils.ID_NAMESPACE);
  }
}

export function mongoTableId(table: storage.SourceTableId): bson.ObjectId {
  if (typeof table == 'string') {
    throw new ServiceAssertionError(`Got string table id, expected ObjectId`);
  }
  return table;
}

function tableIdString(table: storage.SourceTableId) {
  if (typeof table == 'string') {
    return table;
  } else {
    return table.toHexString();
  }
}

export function setSessionSnapshotTime(session: mongo.ClientSession, time: bson.Timestamp) {
  // This is a workaround for the lack of direct support for snapshot reads in the MongoDB driver.
  if (!session.snapshotEnabled) {
    throw new ServiceAssertionError(`Session must be a snapshot session`);
  }
  if ((session as any).snapshotTime == null) {
    (session as any).snapshotTime = time;
  } else {
    throw new ServiceAssertionError(`Session snapshotTime is already set`);
  }
}

export async function retryOnMongoMaxTimeMSExpired<T>(
  operation: () => Promise<T>,
  options: {
    signal?: AbortSignal;
    abortMessage?: string;
    retryDelayMs: number;
    onRetry?: (retryCount: number) => void;
  }
): Promise<T> {
  let retryCount = 0;
  // Retry indefinitely on MaxTimeMSExpired errors with exponential backoff.
  while (true) {
    if (options.signal?.aborted) {
      throw new ReplicationAbortedError(options.abortMessage ?? 'Aborted MongoDB operation', options.signal.reason);
    }
    try {
      return await operation();
    } catch (e) {
      if (!lib_mongo.isMaxTimeMSExpiredError(e)) {
        throw e;
      }
      retryCount += 1;
      options.onRetry?.(retryCount);
      await waitWithSignal(options.retryDelayMs, options.signal, options.abortMessage ?? 'Aborted MongoDB operation');
    }
  }
}

async function clearCollectionInBatches(
  logger: Logger,
  label: string,
  findAndDeleteBatch: (batchSize: number) => Promise<number>,
  signal?: AbortSignal,
  throttleRate = DEFAULT_CLEAR_BATCH_THROTTLE_RATE
): Promise<number> {
  let batchSize = CLEAR_BATCH_SIZE;
  let deletedCount = 0;
  while (true) {
    throwIfClearAborted(signal);
    try {
      const batchStartedAt = performance.now();
      const foundBatchSize = await findAndDeleteBatch(batchSize);
      const batchDurationMs = performance.now() - batchStartedAt;
      deletedCount += foundBatchSize;
      if (foundBatchSize > 0) {
        logger.info(
          `Cleared batch of ${label} (${foundBatchSize} documents) in ${Math.round(batchDurationMs)}ms, continuing...`
        );
      }
      if (foundBatchSize < batchSize) {
        return deletedCount;
      }
      if (batchDurationMs > CLEAR_BATCH_MAX_DURATION_MS) {
        batchSize = Math.max(CLEAR_MIN_BATCH_SIZE, Math.floor(batchSize / 2));
      } else if (batchDurationMs < CLEAR_BATCH_MIN_DURATION_MS) {
        batchSize = Math.min(CLEAR_BATCH_SIZE, batchSize * 2);
      }
      await waitWithSignal(batchDurationMs * throttleRate, signal, 'Aborted clearing data');
    } catch (error) {
      if (!lib_mongo.isTimeoutError(error) || batchSize == CLEAR_MIN_BATCH_SIZE) {
        throw error;
      }
      const nextBatchSize = Math.max(CLEAR_MIN_BATCH_SIZE, Math.floor(batchSize / 2));
      logger.info(
        `Clearing batch of ${label} timed out with batch size ${batchSize}, retrying find and delete with ${nextBatchSize}...`
      );
      batchSize = nextBatchSize;

      // Pause for at least the equivalent of one query timeout before retrying, since the cause may be high database load.
      const pauseDuration = Math.max(throttleRate, 1.0) * lib_mongo.MONGO_CLEAR_OPERATION_TIMEOUT_MS;
      await waitWithSignal(pauseDuration, signal, 'Aborted clearing data');
    }
  }
}

export async function clearCollectionInIdRanges<T extends mongo.Document>(
  logger: Logger,
  label: string,
  collection: mongo.Collection<T>,
  filter: ClearCollectionFilter<T>,
  signal?: AbortSignal,
  throttleRate = DEFAULT_CLEAR_BATCH_THROTTLE_RATE
): Promise<number> {
  return clearCollectionInBatches(
    logger,
    label,
    async (batchSize) => {
      const queryOptions: mongo.FindOptions<T> = {
        maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS,
        projection: { _id: 1 }
      };
      const [batchEnd] = await collection
        .find(filter, queryOptions)
        .sort({ _id: 1 })
        .skip(batchSize)
        .limit(1)
        .toArray();
      throwIfClearAborted(signal);

      if (batchEnd == null) {
        // We're on the last batch
        const result = await collection.deleteMany(filter, {
          maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS
        });
        return result.deletedCount;
      }

      const idRange: mongo.FilterOperators<mongo.InferIdType<T>> = {
        ...filter._id,
        $lt: batchEnd._id
      };
      await collection.deleteMany(
        {
          ...filter,
          _id: idRange
        } as mongo.Filter<T>,
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      );
      return batchSize;
    },
    signal,
    throttleRate
  );
}

export async function clearCollectionInIdBatches<T extends mongo.Document>(
  logger: Logger,
  label: string,
  collection: mongo.Collection<T>,
  filter: mongo.Filter<T>,
  signal?: AbortSignal,
  throttleRate = DEFAULT_CLEAR_BATCH_THROTTLE_RATE
): Promise<number> {
  return clearCollectionInBatches(
    logger,
    label,
    async (batchSize) => {
      const documents = await collection
        .find(filter, {
          maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS,
          projection: { _id: 1 }
        })
        .limit(batchSize)
        .toArray();
      throwIfClearAborted(signal);

      if (documents.length === 0) {
        return 0;
      }
      await collection.deleteMany(
        {
          _id: {
            $in: documents.map((document) => document._id)
          }
        } as mongo.Filter<T>,
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      );
      return documents.length;
    },
    signal,
    throttleRate
  );
}

export const createPaginatedConnectionQuery = async <T extends mongo.Document>(
  query: mongo.Filter<T>,
  collection: mongo.Collection<T>,
  limit: number,
  cursor?: string
) => {
  const createQuery = (cursor?: string) => {
    if (!cursor) {
      return query;
    }
    const connected_at = query.connected_at
      ? { $lt: new Date(cursor), $gte: query.connected_at.$gte }
      : { $lt: new Date(cursor) };
    return {
      ...query,
      connected_at
    } as mongo.Filter<T>;
  };

  const findCursor = collection.find(createQuery(cursor), {
    sort: {
      /** We are sorting by connected at date descending to match cursor Postgres implementation */
      connected_at: -1
    }
  });

  const items = await findCursor.limit(limit).toArray();
  const count = items.length;
  /** The returned total has been defaulted to 0 due to the overhead using documentCount from the mogo driver.
   * cursor.count has been deprecated.
   * */
  return {
    items,
    count,
    /** Setting the cursor to the connected at date of the last item in the list */
    cursor: count === limit ? items[items.length - 1].connected_at.toISOString() : undefined,
    more: !(count !== limit)
  };
};
