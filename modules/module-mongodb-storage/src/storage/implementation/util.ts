import { storage, utils } from '@powersync/service-core';
import * as bson from 'bson';
import * as crypto from 'crypto';
import * as mongo from 'mongodb';
import * as uuid from 'uuid';
import { PowerSyncMongo } from './db.js';
import { BucketDataDocument } from './models.js';

export function idPrefixFilter<T>(prefix: Partial<T>, rest: (keyof T)[]): mongo.Condition<T> {
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

export function generateSlotName(prefix: string, sync_rules_id: number) {
  const slot_suffix = crypto.randomBytes(2).toString('hex');
  return `${prefix}${sync_rules_id}_${slot_suffix}`;
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
 * For this to be effective, set batchSize = limit in the find command.
 */
export async function readSingleBatch<T>(cursor: mongo.FindCursor<T>): Promise<{ data: T[]; hasMore: boolean }> {
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

export function mapOpEntry(row: BucketDataDocument): utils.OplogEntry {
  if (row.op == 'PUT' || row.op == 'REMOVE') {
    return {
      op_id: utils.timestampToOpId(row._id.o),
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
      op_id: utils.timestampToOpId(row._id.o),
      op: row.op,
      checksum: Number(row.checksum)
    };
  }
}

export function replicaIdToSubkey(table: bson.ObjectId, id: storage.ReplicaId): string {
  if (storage.isUUID(id)) {
    // Special case for UUID for backwards-compatiblity
    return `${table.toHexString()}/${id.toHexString()}`;
  } else {
    // Hashed UUID from the table and id
    const repr = bson.serialize({ table, id });
    return uuid.v5(repr, utils.ID_NAMESPACE);
  }
}

/**
 * Helper function for creating a MongoDB client from consumers of this package
 */
export const createMongoClient = (url: string, options?: mongo.MongoClientOptions) => {
  return new mongo.MongoClient(url, options);
};

/**
 * Helper for unit tests
 */
export const connectMongoForTests = (url: string, isCI: boolean) => {
  // Short timeout for tests, to fail fast when the server is not available.
  // Slightly longer timeouts for CI, to avoid arbitrary test failures
  const client = createMongoClient(url, {
    connectTimeoutMS: isCI ? 15_000 : 5_000,
    socketTimeoutMS: isCI ? 15_000 : 5_000,
    serverSelectionTimeoutMS: isCI ? 15_000 : 2_500
  });
  return new PowerSyncMongo(client);
};

/**
 * MongoDB bulkWrite internally splits the operations into batches
 * so that no batch exceeds 16MB. However, there are cases where
 * the batch size is very close to 16MB, where additional metadata
 * on the server pushes it over the limit, resulting in this error
 * from the server:
 *
 * > MongoBulkWriteError: BSONObj size: 16814023 (0x1008FC7) is invalid. Size must be between 0 and 16793600(16MB) First element: insert: "bucket_data"
 *
 * We work around the issue by doing our own batching, limiting the
 * batch size to 15MB. This does add additional overhead with
 * BSON.calculateObjectSize.
 */
export async function safeBulkWrite<T extends mongo.Document>(
  collection: mongo.Collection<T>,
  operations: mongo.AnyBulkWriteOperation<T>[],
  options: mongo.BulkWriteOptions
) {
  // Must be below 16MB.
  // We could probably go a little closer, but 15MB is a safe threshold.
  const BULK_WRITE_LIMIT = 15 * 1024 * 1024;

  let batch: mongo.AnyBulkWriteOperation<T>[] = [];
  let currentSize = 0;
  // Estimated overhead per operation, should be smaller in reality.
  const keySize = 8;
  for (let op of operations) {
    const bsonSize =
      mongo.BSON.calculateObjectSize(op, {
        checkKeys: false,
        ignoreUndefined: true
      } as any) + keySize;
    if (batch.length > 0 && currentSize + bsonSize > BULK_WRITE_LIMIT) {
      await collection.bulkWrite(batch, options);
      currentSize = 0;
      batch = [];
    }
    batch.push(op);
    currentSize += bsonSize;
  }
  if (batch.length > 0) {
    await collection.bulkWrite(batch, options);
  }
}
