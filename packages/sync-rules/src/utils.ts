import { JSONBig, JsonContainer, Replacer, stringifyRaw } from '@powersync/service-jsonbig';
import { SelectFromStatement, Statement } from 'pgsql-ast-parser';
import { SQLITE_FALSE, SQLITE_TRUE } from './sql_support.js';
import { DatabaseInputRow, SqliteJsonRow, SqliteJsonValue, SqliteRow, SqliteValue } from './types.js';
import { SyncRuleProcessingError as SyncRulesProcessingError } from './errors.js';

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
export function jsonValueToSqlite(value: null | undefined | string | number | bigint | boolean | any): SqliteValue {
  if (typeof value == 'boolean') {
    return value ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (typeof value == 'object' || Array.isArray(value)) {
    // Objects and arrays must be stringified
    return JSONBig.stringify(value);
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
    // TODO: Proper error class
    throw new SyncRulesProcessingError(
      // FIXME: Use @powersync/service-errors
      'PSYNC_S1004',
      `json nested object depth exceeds the limit of ${DEPTH_LIMIT}`
    );
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

/**
 * Lookup serialization must be number-agnostic. I.e. normalize numbers, instead of preserving numbers.
 */
export function normalizeParameterValue(value: SqliteJsonValue): SqliteJsonValue {
  if (typeof value == 'number' && Number.isInteger(value)) {
    return BigInt(value);
  }
  return value;
}
