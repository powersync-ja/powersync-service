import { Statement, SelectFromStatement } from 'pgsql-ast-parser';
import { DatabaseInputRow, SqliteRow, SqliteJsonRow, SqliteJsonValue, SqliteValue, SyncParameters } from './types.js';
import { SQLITE_FALSE, SQLITE_TRUE } from './sql_support.js';
import { JsonContainer } from '@powersync/service-jsonbig';
import { JSONBig, stringifyRaw, Replacer } from '@powersync/service-jsonbig';

export function isSelectStatement(q: Statement): q is SelectFromStatement {
  return q.type == 'select';
}

export function getBucketId(
  descriptor_id: string,
  bucket_parameters: string[],
  params: Record<string, SqliteJsonValue>
): string {
  // Important: REAL and INTEGER values matching the same number needs the same representation in the bucket name.
  const paramArray = bucket_parameters.map((name) => params[`bucket.${name}`]);
  return `${descriptor_id}${JSONBucketNameSerialize.stringify(paramArray)}`;
}

const DEPTH_LIMIT = 10;

/**
 * SqliteRow -> SqliteJsonRow.
 *
 * Use wherever data should be persisted.
 *
 * Basically just removes Uint8Array.
 */
export function filterJsonRow(data: SqliteRow): SqliteJsonRow {
  let record: Record<string, any> = {};
  for (let key of Object.keys(data)) {
    const value = data[key];
    if (isJsonValue(value)) {
      record[key] = value;
    }
  }
  return record;
}

/**
 * Convert a parsed JSON scalar value to SqliteValue.
 *
 * Types specifically not supported in output are `boolean` and `undefined`.
 */
export function jsonValueToSqlite(value: null | undefined | string | number | bigint | boolean): SqliteValue {
  if (typeof value == 'boolean') {
    return value ? SQLITE_TRUE : SQLITE_FALSE;
  } else {
    return value ?? null;
  }
}

export function isJsonValue(value: SqliteValue): value is SqliteJsonValue {
  return value == null || typeof value == 'string' || typeof value == 'number' || typeof value == 'bigint';
}

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
    return data ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (typeof data == 'bigint') {
    return data;
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

/**
 * Map database row to SqliteRow for use in sync rules.
 */
export function toSyncRulesRow(row: DatabaseInputRow): SqliteRow {
  let record: SqliteRow = {};
  for (let key of Object.keys(row)) {
    record[key] = toSyncRulesValue(row[key], false, true);
  }
  return record;
}

/**
 * Convert parameter query input to a SqliteJsonRow.
 *
 * @param parameters Generic JSON input
 */
export function toSyncRulesParameters(parameters: Record<string, any>): SqliteJsonRow {
  let record: SqliteJsonRow = {};
  for (let key of Object.keys(parameters)) {
    record[key] = toSyncRulesValue(parameters[key], true, false) as SqliteJsonValue;
  }
  return record;
}

/**
 * Convert to a SQLITE-equivalent value: NULL, TEXT, INTEGER, REAL or BLOB.
 *
 * Any object or array is converted to JSON TEXT.
 */
export function toSyncRulesValue(data: any, autoBigNum?: boolean, keepUndefined?: boolean): SqliteValue {
  if (data == null) {
    // null or undefined
    if (keepUndefined) {
      return data;
    }
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
    return data ? SQLITE_TRUE : SQLITE_FALSE;
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

export function normalizeTokenParameters(
  token_parameters: Record<string, any>,
  user_parameters?: Record<string, any>
): SyncParameters {
  const raw_user_parameters = JSONBig.stringify(user_parameters ?? {});
  return {
    token_parameters: toSyncRulesParameters(token_parameters),
    user_parameters: toSyncRulesParameters(user_parameters ?? {}),
    raw_user_parameters
  };
}

/**
 * Only use this for serializing bucket names. Bucket names should never be parsed except perhaps for debug purposes.
 *
 * Important: REAL and INTEGER values matching the same number needs the same representation in the bucket name.
 */
export const JSONBucketNameSerialize = {
  stringify(value: any, replacer?: Replacer, space?: string | number): string {
    return stringifyRaw(value, replacer, space)!;
  }
};
