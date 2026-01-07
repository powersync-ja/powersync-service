import { JSONBig, JsonContainer, Replacer, stringifyRaw } from '@powersync/service-jsonbig';
import { SelectFromStatement, Statement } from 'pgsql-ast-parser';
import { CompatibilityContext } from './compatibility.js';
import { SyncRuleProcessingError as SyncRulesProcessingError } from './errors.js';
import { BucketDataScope } from './HydrationState.js';
import { SQLITE_FALSE, SQLITE_TRUE } from './sql_support.js';
import {
  DatabaseInputRow,
  DatabaseInputValue,
  SqliteInputRow,
  SqliteInputValue,
  SqliteJsonRow,
  SqliteJsonValue,
  SqliteRow,
  SqliteValue
} from './types.js';
import { CustomArray, CustomObject, CustomSqliteValue } from './types/custom_sqlite_value.js';

export function isSelectStatement(q: Statement): q is SelectFromStatement {
  return q.type == 'select';
}

export function buildBucketName(scope: BucketDataScope, serializedParameters: string): string {
  return scope.bucketPrefix + serializedParameters;
}

export function serializeBucketParameters(bucketParameters: string[], params: Record<string, SqliteJsonValue>): string {
  // Important: REAL and INTEGER values matching the same number needs the same representation in the bucket name.
  const paramArray = bucketParameters.map((name) => params[`bucket.${name}`]);
  return JSONBucketNameSerialize.stringify(paramArray);
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
export function jsonValueToSqlite(
  fixedJsonBehavior: boolean,
  value: null | undefined | string | number | bigint | boolean | any
): SqliteValue {
  let isObject = typeof value == 'object';
  if (fixedJsonBehavior) {
    // With the fixed json behavior, make json_extract() not represent a null value as 'null' but instead use a SQL NULL
    // value.
    isObject = isObject && value != null;
  }

  if (typeof value == 'boolean') {
    return value ? SQLITE_TRUE : SQLITE_FALSE;
  } else if (isObject || Array.isArray(value)) {
    // Objects and arrays must be stringified
    return JSONBig.stringify(value);
  } else {
    return value ?? null;
  }
}

export function isJsonValue(value: SqliteValue): value is SqliteJsonValue {
  return value == null || typeof value == 'string' || typeof value == 'number' || typeof value == 'bigint';
}

function filterJsonData(data: any, context: CompatibilityContext, depth = 0): any {
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
    return data.map((element) => filterJsonData(element, context, depth + 1));
  } else if (ArrayBuffer.isView(data)) {
    return undefined;
  } else if (data instanceof JsonContainer) {
    // Can be stringified directly when using our JSONBig implementation
    return data as any;
  } else if (data instanceof CustomSqliteValue) {
    return data.toSqliteValue(context);
  } else if (typeof data == 'object') {
    let record: Record<string, any> = {};
    for (let key of Object.keys(data)) {
      record[key] = filterJsonData(data[key], context, depth + 1);
    }
    return record as any;
  } else {
    return undefined;
  }
}

/**
 * Map database row to SqliteRow for use in sync rules.
 */
export function toSyncRulesRow(row: DatabaseInputRow): SqliteInputRow {
  let record: SqliteInputRow = {};
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
export function toSyncRulesParameters(parameters: Record<string, any>, context: CompatibilityContext): SqliteJsonRow {
  let record: SqliteJsonRow = {};
  for (let key of Object.keys(parameters)) {
    record[key] = applyValueContext(toSyncRulesValue(parameters[key], true, false), context) as SqliteJsonValue;
  }
  return record;
}

/**
 * Convert to a SQLITE-equivalent value: NULL, TEXT, INTEGER, REAL or BLOB.
 *
 * Any object or array is converted to JSON TEXT.
 */
export function toSyncRulesValue(
  data: DatabaseInputValue,
  autoBigNum?: boolean,
  keepUndefined?: boolean
): SqliteInputValue {
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
    return new CustomArray(data, filterJsonData);
  } else if (data instanceof Uint8Array || data instanceof CustomSqliteValue) {
    return data;
  } else if (data instanceof JsonContainer) {
    return data.toString();
  } else if (typeof data == 'object') {
    return new CustomObject(data, filterJsonData);
  } else {
    return null;
  }
}

export function applyValueContext(value: SqliteInputValue, context: CompatibilityContext): SqliteValue {
  if (value instanceof CustomSqliteValue) {
    return value.toSqliteValue(context);
  } else {
    return value;
  }
}

export function applyRowContext<MaybeToast extends undefined = never>(
  value: SqliteRow<SqliteInputValue | MaybeToast>,
  context: CompatibilityContext
): SqliteRow<SqliteValue | MaybeToast> {
  let replacedCustomValues: SqliteRow<SqliteValue> = {};
  let didReplaceValue = false;

  for (let [key, rawValue] of Object.entries(value)) {
    if (rawValue instanceof CustomSqliteValue) {
      replacedCustomValues[key] = rawValue.toSqliteValue(context);
      didReplaceValue = true;
    }
  }

  if (didReplaceValue) {
    return Object.assign({ ...value }, replacedCustomValues);
  } else {
    // The cast is safe - no values in the original row are CustomSqliteValues.
    return value as SqliteRow<SqliteValue | MaybeToast>;
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
