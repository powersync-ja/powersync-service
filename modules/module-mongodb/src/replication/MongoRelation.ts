import { mongo } from '@powersync/lib-service-mongodb';
import { storage } from '@powersync/service-core';
import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { SqliteRow, SqliteValue } from '@powersync/service-sync-rules';

import { CHECKPOINTS_COLLECTION } from './replication-utils.js';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';

export function getMongoRelation(source: mongo.ChangeStreamNameSpace): storage.SourceEntityDescriptor {
  return {
    name: source.coll,
    schema: source.db,
    objectId: source.coll,
    replicationColumns: [{ name: '_id' }]
  } satisfies storage.SourceEntityDescriptor;
}

export function getMongoLsn(timestamp: mongo.Timestamp) {
  const a = timestamp.high.toString(16).padStart(8, '0');
  const b = timestamp.low.toString(16).padStart(8, '0');
  return a + b;
}

export function mongoLsnToTimestamp(lsn: string | null) {
  if (lsn == null) {
    return null;
  }
  const a = parseInt(lsn.substring(0, 8), 16);
  const b = parseInt(lsn.substring(8, 16), 16);
  return mongo.Timestamp.fromBits(b, a);
}

export function constructAfterRecord(document: mongo.Document): SqliteRow {
  let record: SqliteRow = {};
  for (let key of Object.keys(document)) {
    record[key] = toMongoSyncRulesValue(document[key]);
  }
  return record;
}

export function toMongoSyncRulesValue(data: any): SqliteValue {
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
    return data.toISOString().replace('T', ' ');
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
    // We may be able to avoid some parse + stringify cycles here for JsonSqliteContainer.
    return JSONBig.stringify(data.map((element) => filterJsonData(element)));
  } else if (data instanceof Uint8Array) {
    return data;
  } else if (data instanceof JsonContainer) {
    return data.toString();
  } else if (typeof data == 'object') {
    let record: Record<string, any> = {};
    for (let key of Object.keys(data)) {
      record[key] = filterJsonData(data[key]);
    }
    return JSONBig.stringify(record);
  } else {
    return null;
  }
}

const DEPTH_LIMIT = 20;

function filterJsonData(data: any, depth = 0): any {
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
    return data.toISOString().replace('T', ' ');
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
    return data.map((element) => filterJsonData(element, depth + 1));
  } else if (ArrayBuffer.isView(data)) {
    return undefined;
  } else if (data instanceof JsonContainer) {
    // Can be stringified directly when using our JSONBig implementation
    return data;
  } else if (typeof data == 'object') {
    let record: Record<string, any> = {};
    for (let key of Object.keys(data)) {
      record[key] = filterJsonData(data[key], depth + 1);
    }
    return record;
  } else {
    return undefined;
  }
}

export async function createCheckpoint(client: mongo.MongoClient, db: mongo.Db): Promise<string> {
  const session = client.startSession();
  try {
    // Note: If multiple PowerSync instances are replicating the same source database,
    // they'll modify the same checkpoint document. This is fine - it could create
    // more replication load than required, but won't break anything.
    await db.collection(CHECKPOINTS_COLLECTION).findOneAndUpdate(
      {
        _id: 'checkpoint' as any
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
    return getMongoLsn(time);
  } finally {
    await session.endSession();
  }
}
