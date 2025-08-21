import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import {
  CompatibilityContext,
  CustomArray,
  CustomObject,
  CustomSqliteValue,
  DatabaseInputValue,
  SqliteInputRow,
  SqliteInputValue,
  SqliteRow,
  SqliteValue,
  DateTimeValue
} from '@powersync/service-sync-rules';

import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { MongoLSN } from '../common/MongoLSN.js';
import { CHECKPOINTS_COLLECTION } from './replication-utils.js';

export function getMongoRelation(source: mongo.ChangeStreamNameSpace): storage.SourceEntityDescriptor {
  return {
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
    return new DateTimeValue(isoString);
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
    } else {
      return data;
    }
  } else if (typeof data == 'boolean') {
    return data ? 1n : 0n;
  } else if (typeof data == 'bigint') {
    return data;
  } else if (data instanceof Date) {
    const isoString = data.toISOString();
    return new DateTimeValue(isoString).toSqliteValue(context);
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
  const session = client.startSession();
  try {
    // We use an unique id per process, and clear documents on startup.
    // This is so that we can filter events for our own process only, and ignore
    // events from other processes.
    await db.collection(CHECKPOINTS_COLLECTION).findOneAndUpdate(
      {
        _id: id as any
      },
      {
        $inc: { i: 1 }
      },
      {
        upsert: true,
        returnDocument: 'after',
        session
      }
    );
    const time = session.operationTime!;
    // TODO: Use the above when we support custom write checkpoints
    return new MongoLSN({ timestamp: time }).comparable;
  } finally {
    await session.endSession();
  }
}
