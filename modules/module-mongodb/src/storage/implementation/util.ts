import { storage, utils } from '@powersync/service-core';
import { SqliteJsonValue } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import * as crypto from 'crypto';
import * as mongo from 'mongodb';
import * as uuid from 'uuid';
import { BucketDataDocument } from './models.js';

/**
 * Lookup serialization must be number-agnostic. I.e. normalize numbers, instead of preserving numbers.
 * @param lookup
 */

export function serializeLookup(lookup: SqliteJsonValue[]) {
  const normalized = lookup.map((value) => {
    if (typeof value == 'number' && Number.isInteger(value)) {
      return BigInt(value);
    } else {
      return value;
    }
  });
  return new bson.Binary(bson.serialize({ l: normalized }));
}

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

export const BSON_DESERIALIZE_OPTIONS: bson.DeserializeOptions = {
  // use bigint instead of Long
  useBigInt64: true
};

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

/**
 * Returns true if two ReplicaId values are the same (serializes to the same BSON value).
 */
export function replicaIdEquals(a: storage.ReplicaId, b: storage.ReplicaId) {
  if (a === b) {
    return true;
  } else if (typeof a == 'string' && typeof b == 'string') {
    return a == b;
  } else if (isUUID(a) && isUUID(b)) {
    return a.equals(b);
  } else if (a == null && b == null) {
    return true;
  } else if (a != null || b != null) {
    return false;
  } else {
    // There are many possible primitive values, this covers them all
    return (bson.serialize({ id: a }) as Buffer).equals(bson.serialize({ id: b }));
  }
}

export function replicaIdToSubkey(table: bson.ObjectId, id: storage.ReplicaId): string {
  if (isUUID(id)) {
    // Special case for UUID for backwards-compatiblity
    return `${table.toHexString()}/${id.toHexString()}`;
  } else {
    // Hashed UUID from the table and id
    const repr = bson.serialize({ table, id });
    return uuid.v5(repr, utils.ID_NAMESPACE);
  }
}

/**
 * True if this is a bson.UUID.
 *
 * Works even with multiple copies of the bson package.
 */
export function isUUID(value: any): value is bson.UUID {
  if (value == null || typeof value != 'object') {
    return false;
  }
  const uuid = value as bson.UUID;
  return uuid._bsontype == 'Binary' && uuid.sub_type == bson.Binary.SUBTYPE_UUID;
}


/**
 * Helper function for creating a MongoDB client from consumers of this package
 */
export const createMongoClient =(url: string, options?: mongo.MongoClientOptions) => {
  return new mongo.MongoClient(url, options);
}
