import * as bson from 'bson';
import * as crypto from 'crypto';
import * as uuid from 'uuid';

import { mongo } from '@powersync/lib-service-mongodb';
import { storage, utils } from '@powersync/service-core';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDataDocument } from '../storage/implementation/models.js';

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

export function mapOpEntry(row: BucketDataDocument): utils.OplogEntry {
  if (row.op == 'PUT' || row.op == 'REMOVE') {
    return {
      op_id: utils.internalToExternalOpId(row._id.o),
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
      op_id: utils.internalToExternalOpId(row._id.o),
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
    total: 0,
    count,
    /** Setting the cursor to the connected at date of the last item in the list */
    cursor: count === limit ? items[items.length - 1].connected_at.toISOString() : undefined,
    more: !(count !== limit)
  };
};
