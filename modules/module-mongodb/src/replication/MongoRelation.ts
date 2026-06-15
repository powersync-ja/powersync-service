import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { JsonContainer } from '@powersync/service-jsonbig';
import {
  CompatibilityContext,
  CustomArray,
  CustomObject,
  CustomSqliteValue,
  DateTimeSourceOptions,
  DateTimeValue,
  SqliteInputRow,
  SqliteInputValue,
  TimeValuePrecision
} from '@powersync/service-sync-rules';

import { ErrorCode, logger, ServiceAssertionError, ServiceError } from '@powersync/lib-services-framework';
import { CosmosDBLSN, normalizeSentinel } from '../common/CosmosDBLSN.js';
import { MongoLSN } from '../common/MongoLSN.js';

export function getMongoRelation(
  source: mongo.ChangeStreamNameSpace,
  connectionTag: string
): storage.SourceEntityDescriptor {
  return {
    connectionTag,
    name: source.coll,
    schema: source.db,
    // Not relevant for MongoDB - we use db + coll name as the identifier
    objectId: undefined,
    replicaIdColumns: [{ name: '_id' }]
  } satisfies storage.SourceEntityDescriptor;
}

/**
 * For in-memory cache only.
 */
export function getCacheIdentifier(source: storage.SourceEntityDescriptor | storage.SourceTable): string {
  if (source instanceof storage.SourceTable) {
    return `${source.schema}.${source.name}`;
  }
  return `${source.schema}.${source.name}`;
}

export function constructAfterRecord(document: mongo.Document): SqliteInputRow {
  let record: SqliteInputRow = {};
  for (let key of Object.keys(document)) {
    record[key] = toMongoSyncRulesValue(document[key]);
  }
  return record;
}

export function toMongoSyncRulesValue(data: any): SqliteInputValue {
  const autoBigNum = true;
  if (data === null) {
    return null;
  } else if (typeof data == 'undefined') {
    // We consider `undefined` in top-level fields as missing replicated value,
    // so use null instead.
    return null;
  } else if (typeof data == 'string') {
    return data;
  } else if (typeof data == 'number') {
    if (Number.isInteger(data) && autoBigNum) {
      return BigInt(data);
    } else {
      return data;
    }
  } else if (typeof data == 'bigint') {
    return data;
  } else if (typeof data == 'boolean') {
    return data ? 1n : 0n;
  } else if (data instanceof mongo.ObjectId) {
    return data.toHexString();
  } else if (data instanceof mongo.UUID) {
    return data.toHexString();
  } else if (data instanceof Date) {
    const isoString = data.toISOString();
    return new DateTimeValue(isoString, undefined, mongoTimeOptions);
  } else if (data instanceof mongo.Binary) {
    return new Uint8Array(data.buffer);
  } else if (data instanceof mongo.Long) {
    return data.toBigInt();
  } else if (data instanceof mongo.Decimal128) {
    return data.toString();
  } else if (data instanceof mongo.MinKey || data instanceof mongo.MaxKey) {
    return null;
  } else if (data instanceof RegExp) {
    return JSON.stringify({ pattern: data.source, options: data.flags });
  } else if (Array.isArray(data)) {
    return new CustomArray(data, filterJsonData);
  } else if (data instanceof Uint8Array) {
    return data;
  } else if (data instanceof JsonContainer) {
    return data.toString();
  } else if (typeof data == 'object') {
    return new CustomObject(data, filterJsonData);
  } else {
    return null;
  }
}

const DEPTH_LIMIT = 20;

function filterJsonData(data: any, context: CompatibilityContext, depth = 0): any {
  const autoBigNum = true;
  if (depth > DEPTH_LIMIT) {
    // This is primarily to prevent infinite recursion
    throw new ServiceError(ErrorCode.PSYNC_S1004, `json nested object depth exceeds the limit of ${DEPTH_LIMIT}`);
  }
  if (data === null) {
    return data;
  } else if (typeof data == 'undefined') {
    // For nested data, keep as undefined.
    // In arrays, this is converted to null.
    // In objects, the key is excluded.
    return undefined;
  } else if (typeof data == 'string') {
    return data;
  } else if (typeof data == 'number') {
    if (autoBigNum && Number.isInteger(data)) {
      return BigInt(data);
    } else if (!Number.isFinite(data)) {
      // Only finite numbers can be represented in JSON.
      return null;
    } else {
      return data;
    }
  } else if (typeof data == 'boolean') {
    return data ? 1n : 0n;
  } else if (typeof data == 'bigint') {
    return data;
  } else if (data instanceof Date) {
    const isoString = data.toISOString();
    return new DateTimeValue(isoString, undefined, mongoTimeOptions).toSqliteValue(context);
  } else if (data instanceof mongo.ObjectId) {
    return data.toHexString();
  } else if (data instanceof mongo.UUID) {
    return data.toHexString();
  } else if (data instanceof mongo.Binary) {
    return undefined;
  } else if (data instanceof mongo.Long) {
    return data.toBigInt();
  } else if (data instanceof mongo.Decimal128) {
    return data.toString();
  } else if (data instanceof mongo.MinKey || data instanceof mongo.MaxKey) {
    return null;
  } else if (data instanceof RegExp) {
    return { pattern: data.source, options: data.flags };
  } else if (Array.isArray(data)) {
    return data.map((element) => filterJsonData(element, context, depth + 1));
  } else if (ArrayBuffer.isView(data)) {
    return undefined;
  } else if (data instanceof CustomSqliteValue) {
    return data.toSqliteValue(context);
  } else if (data instanceof JsonContainer) {
    // Can be stringified directly when using our JSONBig implementation
    return data;
  } else if (typeof data == 'object') {
    let record: Record<string, any> = {};
    for (let key of Object.keys(data)) {
      record[key] = filterJsonData(data[key], context, depth + 1);
    }
    return record;
  } else {
    return undefined;
  }
}

/**
 * Id for checkpoints not associated with any specific replication stream.
 *
 * Use this for write checkpoints, or any other case where we want to process
 * the checkpoint immediately, and not wait for batching.
 */
export const STANDALONE_CHECKPOINT_ID = '_standalone_checkpoint';

/**
 * Create a checkpoint by upserting a document in _powersync_checkpoints.
 *
 * Returns either:
 * - A standard LSN string (from operationTime or wall clock) for storage
 *   boundaries like no_checkpoint_before, where lexicographic comparison is used.
 * - A sentinel string ('sentinel:<id>:<i>') for the streaming loop's
 *   waitForCheckpointLsn, where the loop matches by document content instead
 *   of comparing LSNs.
 *
 * Cosmos DB is detected automatically: when session.operationTime is null
 * (Cosmos DB does not provide it), the function falls back to wall clock
 * timestamps or sentinel format depending on the mode.
 *
 * @param mode
 *   'lsn' (default) — return a real LSN string. Uses operationTime when
 *     available (standard MongoDB), falls back to wall clock (Cosmos DB).
 *   'sentinel' — return a sentinel marker for event-based matching in the
 *     streaming loop.
 * @param globalSentinel
 *   Cosmos DB only: the current standalone checkpoint counter value, embedded
 *   in the barrier document. This lets the stream read the global LSN
 *   coordinate from its own barrier event, without depending on the standalone
 *   checkpoint event having been delivered first — change stream ordering
 *   across different documents is not guaranteed.
 */
export async function createCheckpoint(
  client: mongo.MongoClient,
  db: mongo.Db,
  id: mongo.ObjectId | string,
  options?: { mode?: 'lsn' | 'sentinel'; globalSentinel?: bigint }
): Promise<string> {
  const TRIES = 2;
  for (let i = 0; i < TRIES; i++) {
    try {
      return await createCheckpointInner(client, db, id, options);
    } catch (e) {
      if (i < TRIES - 1) {
        logger.warn(`Failed to create checkpoint on attempt ${i + 1}`, e);
      } else {
        throw e;
      }
    }
  }
  throw new ServiceAssertionError(`Unreachable code`);
}

async function createCheckpointInner(
  client: mongo.MongoClient,
  db: mongo.Db,
  id: mongo.ObjectId | string,
  options?: { mode?: 'lsn' | 'sentinel'; globalSentinel?: bigint }
): Promise<string> {
  const mode = options?.mode ?? 'lsn';
  // We use an unique id per process, and clear documents on startup.
  // This is so that we can filter events for our own process only, and ignore
  // events from other processes.

  // We use a command instead of a regular update to avoid auto retries on writes.
  // An auto retry on the write can trigger a weird edge case where the change stream event
  // has the clusterTime of the first write, while the returned operation time is for the second no-op write.
  // Instead, we do manual retries, which does not have the same write de-duplication logic.
  // A sentinal-based approach would be better here, but that is a much bigger change.

  const update: mongo.Document = { $inc: { i: 1 } };
  if (options?.globalSentinel != null) {
    // Cosmos DB only: embed the global standalone counter in this stream's
    // barrier document, so the barrier event is self-describing and does not
    // depend on the standalone event being delivered first. See the sentinel
    // implementation in CheckpointImplementation.ts.
    update.$set = { globalSentinel: mongo.Long.fromBigInt(options.globalSentinel) };
  }

  const response = await db.command({
    findAndModify: '_powersync_checkpoints',
    query: {
      _id: id as any
    },
    new: true,
    upsert: true,
    update
  });

  if (mode === 'sentinel') {
    // Sentinel path (Cosmos DB): the streaming loop matches this barrier by
    // document content (id + increment), not by LSN comparison. operationTime
    // is not available on Cosmos and is not needed here.
    const i = response.value?.i;
    return `sentinel:${id}:${i}`;
  }

  const time = response.operationTime as mongo.Timestamp | undefined;
  if (time == null) {
    throw new ServiceError(ErrorCode.PSYNC_S1004, `clusterTime not available for checkpoint`);
  }
  return new MongoLSN({ timestamp: time }).comparable;
}

/**
 * Create a Cosmos DB comparable LSN by advancing the shared standalone
 * checkpoint document. The returned LSN uses the checkpoint increment as its
 * ordered component instead of a wallTime-derived MongoDB timestamp.
 *
 * This counter is intentionally global to the source database. It is used for
 * storage/client checkpoint comparisons and write checkpoint heads, so it must
 * not reset when a new ChangeStream instance or new sync rules start. Stream
 * local sentinel ids are still useful as private commit barriers, but their
 * counters are not safe as client-visible LSN coordinates.
 */
export async function createCosmosCheckpointLsn(client: mongo.MongoClient, db: mongo.Db): Promise<string> {
  const session = client.startSession();
  try {
    const collection = db.collection('_powersync_checkpoints');

    for (let attempt = 0; attempt < 3; attempt++) {
      // Common path: increment the existing counter.
      const result = await collection.findOneAndUpdate(
        {
          _id: STANDALONE_CHECKPOINT_ID as any,
          i: { $exists: true }
        },
        {
          $inc: { i: 1 }
        },
        {
          returnDocument: 'after',
          session
        }
      );
      if (result != null) {
        return new CosmosDBLSN({ sentinel: normalizeSentinel(result.i) }).comparable;
      }

      // The counter document does not exist: first run, or a consumer deleted
      // it in their source database. Seed the counter at the current epoch
      // milliseconds instead of starting at 1. Counter increments accumulate
      // far slower than one per millisecond, so a re-created counter always
      // resumes ahead of any previously issued coordinate — keeping the LSN
      // domain monotonic even across deletion, instead of silently moving it
      // backwards (which would let new write checkpoint heads resolve against
      // old, higher committed LSNs).
      //
      // $setOnInsert cannot be combined with $inc on the same field, so this
      // is a separate upsert; the loop then retries the increment. The
      // $setOnInsert is a no-op if another process created the document
      // concurrently.
      await collection.updateOne(
        {
          _id: STANDALONE_CHECKPOINT_ID as any
        },
        {
          $setOnInsert: { i: mongo.Long.fromBigInt(BigInt(Date.now())) }
        },
        {
          upsert: true,
          session
        }
      );
    }

    throw new ServiceError(
      ErrorCode.PSYNC_S1301,
      `Failed to increment the standalone checkpoint counter - the checkpoint document may be getting deleted concurrently.`
    );
  } finally {
    await session.endSession();
  }
}

const mongoTimeOptions: DateTimeSourceOptions = {
  subSecondPrecision: TimeValuePrecision.milliseconds,
  defaultSubSecondPrecision: TimeValuePrecision.milliseconds
};
