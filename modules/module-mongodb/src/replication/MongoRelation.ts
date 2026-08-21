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
import { ObjectId } from 'bson';
import { MongoLSN } from '../common/MongoLSN.js';
import { SentinelLSN } from '../common/SentinelLSN.js';

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
 * Id for checkpoint records managed by the {@link SentinelCheckpointImplementation} implementation.
 */
export const SENTINEL_CHECKPOINT_ID = '_sentinel_checkpoint';

/**
 * Create a checkpoint by upserting a document in _powersync_checkpoints, and
 * return a comparable LSN string derived from the write's operationTime.
 *
 * Standard MongoDB only: this requires session.operationTime, which DocumentDB
 * does not provide (it throws PSYNC_S1004 when it is missing). The DocumentDB /
 * sentinel path builds LSNs via {@link createSentinelCheckpointLsn} instead.
 */
export async function createCheckpoint(db: mongo.Db, id: mongo.ObjectId | string): Promise<string> {
  const TRIES = 2;
  for (let i = 0; i < TRIES; i++) {
    try {
      return await createCheckpointInner(db, id);
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

async function createCheckpointInner(db: mongo.Db, id: mongo.ObjectId | string): Promise<string> {
  // We use an unique id per process, and clear documents on startup.
  // This is so that we can filter events for our own process only, and ignore
  // events from other processes.

  // We use a command instead of a regular update to avoid auto retries on writes.
  // An auto retry on the write can trigger a weird edge case where the change stream event
  // has the clusterTime of the first write, while the returned operation time is for the second no-op write.
  // Instead, we do manual retries, which does not have the same write de-duplication logic.
  // A sentinal-based approach would be better here, but that is a much bigger change.

  const update: mongo.Document = { $inc: { i: 1 } };

  const response = await db.command({
    findAndModify: '_powersync_checkpoints',
    query: {
      _id: id as any
    },
    new: true,
    upsert: true,
    update
  });

  const time = response.operationTime as mongo.Timestamp | undefined;
  if (time == null) {
    throw new ServiceError(ErrorCode.PSYNC_S1004, `clusterTime not available for checkpoint`);
  }
  return new MongoLSN({ timestamp: time }).comparable;
}

/**
 * Create a DocumentDB comparable LSN by advancing the shared sentinel checkpoint
 * document ({@link SENTINEL_CHECKPOINT_ID}). The returned LSN encodes the
 * checkpoint counter in the same 16-hex shape as a MongoDB timestamp LSN (see
 * {@link SentinelLSN}), so the two formats are directly comparable. The counter is
 * seeded in the epoch-seconds range (see below), so a sentinel LSN always sorts
 * above any real-timestamp LSN issued in the past.
 *
 * This counter is intentionally global to the source database. It is used for
 * storage/client checkpoint comparisons and write checkpoint heads, so it must
 * not reset when a new ChangeStream instance or new sync rules start.
 *
 * The document is a single shared record whose `stream_id` field alternates:
 * batch/keepalive bumps stamp it with the calling stream's id (so a stream can
 * recognise its own private barriers), while standalone bumps (write checkpoint
 * heads, snapshot markers) clear it to null. `i` advances globally regardless.
 *
 * @param changeStreamId
 *   When provided, the bump is attributed to this stream (a private barrier).
 *   When omitted, it is a standalone bump and stream_id is cleared to null.
 */
export async function createSentinelCheckpointLsn(
  client: mongo.MongoClient,
  db: mongo.Db,
  changeStreamId?: ObjectId
): Promise<string> {
  const session = client.startSession();
  try {
    const collection = db.collection('_powersync_checkpoints');

    for (let attempt = 0; attempt < 3; attempt++) {
      // Common path: increment the existing counter.
      const result = await collection.findOneAndUpdate(
        {
          _id: SENTINEL_CHECKPOINT_ID as any,
          i: { $exists: true }
        },
        {
          $inc: { i: 1 },
          // Standalone bumps (no changeStreamId) must explicitly clear the
          // stream_id left by a previous batch bump — this is a single shared
          // document whose stream_id alternates. Write null rather than relying
          // on `undefined` being serialized (which depends on the driver's
          // ignoreUndefined option, and collapses to an empty $set if enabled).
          $set: {
            stream_id: changeStreamId ?? null
          }
        },
        {
          returnDocument: 'after',
          session
        }
      );

      if (result != null) {
        // `i` is a bigint: the client is configured with useBigInt64, and the
        // seed exceeds 2^53 so it could not be safely represented as a number.
        return new SentinelLSN({ sentinel: result.i }).comparable;
      }

      // The counter document does not exist: first run, or a consumer deleted
      // it in their source database. Seed the counter in the epoch-SECONDS range
      // (seconds in the high 32 bits, mirroring a MongoDB timestamp) rather than
      // starting at 1. Two properties follow:
      //
      // - A sentinel LSN sorts above any real-timestamp LSN issued in the past,
      //   because its high 32 bits are the current epoch seconds.
      // - A re-created counter jumps forward instead of backward across deletion:
      //   the seed advances by 2^32 each wall-clock second, while checkpoints add
      //   1 each, so any later re-seed exceeds the previously issued coordinate
      //   (keeping the LSN domain monotonic; otherwise new write checkpoint heads
      //   could resolve against old, higher committed LSNs).
      //
      // $setOnInsert cannot be combined with $inc on the same field, so this
      // is a separate upsert; the loop then retries the increment. The
      // $setOnInsert is a no-op if another process created the document
      // concurrently.
      await collection.updateOne(
        {
          _id: SENTINEL_CHECKPOINT_ID as any
        },
        {
          $setOnInsert: {
            i: BigInt(Math.floor(Date.now() / 1000)) << 32n,
            stream_id: changeStreamId ?? null
          }
        },
        {
          upsert: true,
          session
        }
      );
    }

    throw new ServiceError(
      ErrorCode.PSYNC_S1301,
      `Failed to increment the sentinel checkpoint counter - the checkpoint document may be getting deleted concurrently.`
    );
  } finally {
    await session.endSession();
  }
}

const mongoTimeOptions: DateTimeSourceOptions = {
  subSecondPrecision: TimeValuePrecision.milliseconds,
  defaultSubSecondPrecision: TimeValuePrecision.milliseconds
};
