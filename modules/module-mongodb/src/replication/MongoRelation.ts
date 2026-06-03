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

export async function createCheckpoint(
  client: mongo.MongoClient,
  db: mongo.Db,
  id: mongo.ObjectId | string
): Promise<string> {
  const TRIES = 2;
  for (let i = 0; i < TRIES; i++) {
    try {
      return await createCheckpointInner(client, db, id);
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
  id: mongo.ObjectId | string
): Promise<string> {
  // We use an unique id per process, and clear documents on startup.
  // This is so that we can filter events for our own process only, and ignore
  // events from other processes.

  // We use a command instead of a regular update to avoid auto retries on writes.
  // An auto retry on the write can trigger a weird edge case where the change stream event
  // has the clusterTime of the first write, while the returned operation time is for the second no-op write.
  // Instead, we do manual retries, which does not have the same write de-duplication logic.
  // A sentinal-based approach would be better here, but that is a much bigger change.

  const response = await db.command({
    findAndModify: '_powersync_checkpoints',
    query: {
      _id: id as any
    },
    new: true,
    upsert: true,
    update: {
      $inc: { i: 1 }
    }
  });

  const time = response.operationTime as mongo.Timestamp | undefined;
  if (time == null) {
    throw new ServiceError(ErrorCode.PSYNC_S1004, `clusterTime not available for checkpoint`);
  }
  return new MongoLSN({ timestamp: time }).comparable;
}

const mongoTimeOptions: DateTimeSourceOptions = {
  subSecondPrecision: TimeValuePrecision.milliseconds,
  defaultSubSecondPrecision: TimeValuePrecision.milliseconds
};
