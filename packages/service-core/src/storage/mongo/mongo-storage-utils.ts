import { SqliteJsonValue, SqliteRow, ToastableSqliteRow } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import * as mongo from 'mongodb';
import * as crypto from 'crypto';
import * as uuid from 'uuid';

import * as replication from '../../replication/replication-index.js';

// TODO can this be more generic?
export const ZERO_LSN = '00000000/00000000';

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

function getRawReplicaIdentity(
  tuple: ToastableSqliteRow,
  columns: replication.ReplicationColumn[]
): Record<string, any> {
  let result: Record<string, any> = {};
  for (let column of columns) {
    const name = column.name;
    result[name] = tuple[name];
  }
  return result;
}
const ID_NAMESPACE = 'a396dd91-09fc-4017-a28d-3df722f651e9';

export function getUuidReplicaIdentityBson(
  tuple: ToastableSqliteRow,
  columns: replication.ReplicationColumn[]
): bson.UUID {
  if (columns.length == 0) {
    // REPLICA IDENTITY NOTHING - generate random id
    return new bson.UUID(uuid.v4());
  }
  const rawIdentity = getRawReplicaIdentity(tuple, columns);

  return uuidForRowBson(rawIdentity);
}

export function hasToastedValues(row: ToastableSqliteRow) {
  for (let key in row) {
    if (typeof row[key] == 'undefined') {
      return true;
    }
  }
  return false;
}

export function isCompleteRow(row: ToastableSqliteRow): row is SqliteRow {
  return !hasToastedValues(row);
}

export function uuidForRowBson(row: SqliteRow): bson.UUID {
  // Important: This must not change, since it will affect how ids are generated.
  // Use BSON so that it's a well-defined format without encoding ambiguities.
  const repr = bson.serialize(row);
  const buffer = Buffer.alloc(16);
  return new bson.UUID(uuid.v5(repr, ID_NAMESPACE, buffer));
}

export function getUuidReplicaIdentityString(
  tuple: ToastableSqliteRow,
  columns: replication.ReplicationColumn[]
): string {
  const rawIdentity = getRawReplicaIdentity(tuple, columns);

  return uuidForRow(rawIdentity);
}

export function uuidForRow(row: SqliteRow): string {
  // Important: This must not change, since it will affect how ids are generated.
  // Use BSON so that it's a well-defined format without encoding ambiguities.
  const repr = bson.serialize(row);
  return uuid.v5(repr, ID_NAMESPACE);
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
