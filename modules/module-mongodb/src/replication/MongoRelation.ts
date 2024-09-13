import { storage } from '@powersync/service-core';
import { SqliteRow, SqliteValue, toSyncRulesRow } from '@powersync/service-sync-rules';
import * as mongo from 'mongodb';
import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';

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
  if (data == null) {
    // null or undefined
    return data;
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
  if (depth > DEPTH_LIMIT) {
    // This is primarily to prevent infinite recursion
    throw new Error(`json nested object depth exceeds the limit of ${DEPTH_LIMIT}`);
  }
  if (data == null) {
    return data; // null or undefined
  } else if (typeof data == 'string' || typeof data == 'number') {
    return data;
  } else if (typeof data == 'boolean') {
    return data ? 1n : 0n;
  } else if (typeof data == 'bigint') {
    return data;
  } else if (data instanceof mongo.ObjectId) {
    return data.toHexString();
  } else if (data instanceof mongo.UUID) {
    return data.toHexString();
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
