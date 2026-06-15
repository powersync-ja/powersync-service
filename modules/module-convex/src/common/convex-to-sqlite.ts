import { JSONBig } from '@powersync/service-jsonbig';
import {
  DatabaseInputRow,
  DatabaseInputValue,
  ExpressionType,
  SqliteInputRow,
  toSyncRulesRow
} from '@powersync/service-sync-rules';
import { ConvexRawDocument } from '../client/ConvexAPITypes.js';

export enum SupportedJSONSchemaPropertyType {
  ID = 'id',
  STRING = 'string',
  BYTES = 'bytes',
  ARRAY = 'array',
  OBJECT = 'object',
  NULL = 'null',
  INT64 = 'int64',
  FLOAT64 = 'float64',
  BOOLEAN = 'boolean',
  UNKNOWN = 'unknown'
}

export const CONVEX_TO_SQLITE_TYPE_MAP: Record<SupportedJSONSchemaPropertyType, ExpressionType> = {
  [SupportedJSONSchemaPropertyType.ID]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.STRING]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.BYTES]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.ARRAY]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.OBJECT]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.NULL]: ExpressionType.NONE,
  [SupportedJSONSchemaPropertyType.INT64]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.FLOAT64]: ExpressionType.REAL,
  [SupportedJSONSchemaPropertyType.BOOLEAN]: ExpressionType.INTEGER,
  [SupportedJSONSchemaPropertyType.UNKNOWN]: ExpressionType.TEXT
} as const;

const INTERNAL_KEYS = new Set(['_table', '_deleted', '_ts', '_component']);

export function jsonSchemaToSQLiteType(jsonType: SupportedJSONSchemaPropertyType): ExpressionType {
  return CONVEX_TO_SQLITE_TYPE_MAP[jsonType];
}

export function toSqliteInputRow(change: ConvexRawDocument): SqliteInputRow {
  const row: DatabaseInputRow = {};

  for (const [key, value] of Object.entries(change)) {
    if (INTERNAL_KEYS.has(key)) {
      continue;
    }

    row[key] = toDatabaseValue(value);
  }

  return toSyncRulesRow(row);
}

export function extractProperties(schema: Record<string, any>): Record<string, unknown> {
  // Convex returns each table schema as a standard JSON Schema object.
  // For example: { "type": "object", "properties": { "name": { "type": "string" } } }.
  // The table columns live under `properties`; top-level keys such as `type`
  // describe the schema itself and must not be treated as columns.
  return isRecord(schema.properties) ? schema.properties : {};
}

/**
 * Converts a Convex row value to a DatabaseInputValue.
 * This intentionally ignores Convex json_schemas metadata so the same source column keeps a stable
 * representation regardless of whether Convex reported that field in json_schemas.
 */
function toDatabaseValue(value: unknown): DatabaseInputValue {
  if (value == null) {
    return null;
  } else if (
    typeof value == 'string' ||
    typeof value == 'boolean' ||
    typeof value == 'number' ||
    typeof value == 'bigint'
  ) {
    return value;
  } else if (Array.isArray(value) || typeof value == 'object') {
    return JSONBig.stringify(value);
  } else {
    return null;
  }
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value == 'object' && value != null && !Array.isArray(value);
}

export function readConvexFieldJsonType(jsonSchemaProperty: unknown): SupportedJSONSchemaPropertyType {
  if (!isRecord(jsonSchemaProperty)) {
    // Invalid schema property entry received
    return SupportedJSONSchemaPropertyType.UNKNOWN;
  }

  const description = jsonSchemaProperty['$description'];
  const type = jsonSchemaProperty['type'];
  /**
   * An Int64 example
   *   "points": {"$description": "int64 represented as base10 string", "type": "string"},
   */
  if (description == 'int64 represented as base10 string' && type == 'string') {
    return SupportedJSONSchemaPropertyType.INT64;
  } else if (description == 'base64 bytes' && type == 'string') {
    /**
     * Buffer example
     * "attachment_data": {"$description": "base64 bytes", "type": "string"},
     */
    return SupportedJSONSchemaPropertyType.BYTES;
  } else if (type == 'string' || type == 'id') {
    return SupportedJSONSchemaPropertyType.STRING;
  } else if (type == 'boolean') {
    return SupportedJSONSchemaPropertyType.BOOLEAN;
  } else if (type == 'number') {
    // number and float64 seem to both be represented as 'number' in the reported JSON schema
    return SupportedJSONSchemaPropertyType.FLOAT64;
  } else if (type == 'array') {
    return SupportedJSONSchemaPropertyType.ARRAY;
  } else if (type == 'object') {
    return SupportedJSONSchemaPropertyType.OBJECT;
  } else if (type == 'null') {
    return SupportedJSONSchemaPropertyType.NULL;
  }

  return SupportedJSONSchemaPropertyType.UNKNOWN;
}
