import * as bson from 'bson';
import * as crypto from 'crypto';
import * as uuid from 'uuid';
import * as fsPromises from 'node:fs/promises';
import { mongo } from '@powersync/lib-service-mongodb';
import { storage, utils } from '@powersync/service-core';
import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDataDocument } from '../storage/implementation/models.js';
import { FsCachePaths } from '../types/types.js';
import path from 'node:path';

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
    count,
    /** Setting the cursor to the connected at date of the last item in the list */
    cursor: count === limit ? items[items.length - 1].connected_at.toISOString() : undefined,
    more: !(count !== limit)
  };
};

/**
 * Caches data in a file and processes it using a provided function.
 *
 * This function checks if a file exists at the specified `filename` path. If the file exists,
 * it reads the cached data from the file and passes it, along with the provided `text`,
 * to the `func` callback for processing. If the file does not exist, it writes the `text`
 * to the file and then calls the `func` with the `text` as both the cache and input.
 *
 * @template T - The return type of the `func` callback.
 * @param {FsCachePaths} filename - The path to the cache file.
 * @param dir
 * @param {string} text - The text to cache or compare against the cached data.
 * @param {(cache: string, text: string) => Promise<T>} func - A callback function that processes the cached data
 * and the provided text. It receives the cached data (or the `text` if no cache exists) as the first argument
 * and the `text` as the second argument.
 * @returns {Promise<T>} - A promise that resolves to the result of the `func` callback.
 */
export async function fsCache<T>(
  filename: FsCachePaths,
  dir: string,
  text: string,
  func: (cache: string, text: string) => Promise<T>
): Promise<T> {
  try {
    await fsPromises.access(dir, fsPromises.constants.R_OK);
  } catch (error) {
    await fsPromises.mkdir(dir, { recursive: true });
  }
  try {
    await fsPromises.access(path.join(dir, filename), fsPromises.constants.R_OK);
    const cache = await fsPromises.readFile(path.join(dir, filename), 'utf-8');
    return func(cache, text);
  } catch (error) {
    await fsPromises.writeFile(path.join(dir, filename), text, 'utf-8');
    return func(text, text);
  }
}

/**
 * Compares cached text with new text and updates the cache file if they differ.
 * Returns true if the cache was updated, false otherwise.
 */
export function syncLockCheck(dir: string) {
  return async (cache: string, text: string): Promise<boolean> => {
    if (cache === text) {
      return false;
    }
    await fsPromises.writeFile(path.join(dir, FsCachePaths.SYNC_RULES_LOCK), text, 'utf-8');
    return true;
  };
}
