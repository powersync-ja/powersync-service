import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
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
  RECORD = 'record',
  NULL = 'null',
  INT64 = 'int64',
  FLOAT64 = 'float64',
  BOOLEAN = 'boolean',
  UNKNOWN = 'unknown'
}

export const CONVEX_TO_SQLITE_TYPE_MAP: Record<SupportedJSONSchemaPropertyType, ExpressionType> = {
  [SupportedJSONSchemaPropertyType.ID]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.STRING]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.BYTES]: ExpressionType.BLOB,
  [SupportedJSONSchemaPropertyType.ARRAY]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.OBJECT]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.RECORD]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.NULL]: ExpressionType.NONE,
  [SupportedJSONSchemaPropertyType.INT64]: ExpressionType.INTEGER,
  [SupportedJSONSchemaPropertyType.FLOAT64]: ExpressionType.REAL,
  [SupportedJSONSchemaPropertyType.BOOLEAN]: ExpressionType.INTEGER,
  [SupportedJSONSchemaPropertyType.UNKNOWN]: ExpressionType.TEXT
} as const;

/**
 * From Convex docs:
 * Every document in Convex automatically has two system fields:

    _id - a unique document ID with validator v.id("tableName")
    _creationTime - a creation timestamp with validator v.number()

    Technically _creationTime should be handlded differently as the other metadata excludes defined below, but we include it here for convenience since it's not necessary for replication.
 */
const INTERNAL_KEYS = new Set(['_table', '_deleted', '_ts', '_component', '_creationTime']);

export function toSqliteInputRow(
  change: ConvexRawDocument,
  jsonSchemaProperties?: Record<string, unknown>
): SqliteInputRow {
  const row: DatabaseInputRow = {};

  for (const [key, value] of Object.entries(change)) {
    if (INTERNAL_KEYS.has(key)) {
      continue;
    }

    row[key] = toDatabaseValue(value, readConvexFieldType(jsonSchemaProperties?.[key]));
  }

  return toSyncRulesRow(row);
}

/**
 * Normalizes the Convex Table columns JSON schema property type
 * to a set of common supported JSON schema property types.
 */
function normalizeConvexJsonSchemaType(type: string | undefined): SupportedJSONSchemaPropertyType {
  const normalized = type?.trim().toLowerCase();
  switch (normalized) {
    case SupportedJSONSchemaPropertyType.ID:
    case SupportedJSONSchemaPropertyType.STRING:
    case SupportedJSONSchemaPropertyType.BYTES:
    case SupportedJSONSchemaPropertyType.ARRAY:
    case SupportedJSONSchemaPropertyType.OBJECT:
    case SupportedJSONSchemaPropertyType.RECORD:
    case SupportedJSONSchemaPropertyType.NULL:
      return normalized;
    case 'integer':
    case 'int64':
      return SupportedJSONSchemaPropertyType.INT64;
    case 'number':
    case 'float':
    case 'float64':
      return SupportedJSONSchemaPropertyType.FLOAT64;
    case 'bool':
    case 'boolean':
      return SupportedJSONSchemaPropertyType.BOOLEAN;
    case 'bytea':
    case 'blob':
      return SupportedJSONSchemaPropertyType.BYTES;
    default:
      return SupportedJSONSchemaPropertyType.UNKNOWN;
  }
}

/**
 * Connverts a Convex JSON Schema property type to A SQLite ExpressionType
 */
export function toExpressionTypeFromConvexType(type: string | undefined): ExpressionType {
  return CONVEX_TO_SQLITE_TYPE_MAP[normalizeConvexJsonSchemaType(type)];
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
 * We typically receive the Schema for each table from Convex in the form of a JSON schema.
 * Each column in a table has a JSON Schema property type.
 *
 * In some cases, we have observed that the Schema returned from Convex does not include column
 * definitions unless if rows exist which have values defined for the column.
 * We don't exclusively rely on the jsonSchemaType for this reason. Type mappings are performed using
 * type checks in these scenarios.
 */
function toDatabaseValue(value: unknown, jsonSchemaType: string): DatabaseInputValue {
  // Use the schema type if available
  switch (normalizeConvexJsonSchemaType(jsonSchemaType)) {
    case 'bytes':
      return toBytesValue(value);
    case 'boolean':
      if (typeof value == 'boolean') {
        return value;
      }
      break;
    default:
      break;
  }

  // The schema did not match at this point, we continue with runtime type checks

  if (value == null) {
    return null;
  }

  if (typeof value == 'string') {
    return value;
  }

  if (typeof value == 'number') {
    if (Number.isInteger(value)) {
      return BigInt(value);
    }
    return value;
  }

  if (typeof value == 'bigint') {
    return value;
  }

  if (typeof value == 'boolean') {
    return value;
  }

  if (value instanceof Uint8Array) {
    return value;
  }

  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }

  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }

  if (Array.isArray(value) || typeof value == 'object') {
    return value as DatabaseInputValue;
  }

  return null;
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value == 'object' && value != null && !Array.isArray(value);
}

function toBytesValue(value: unknown): Uint8Array | null {
  if (value == null) {
    return null;
  }

  if (value instanceof Uint8Array) {
    return value;
  }

  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }

  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }

  if (typeof value != 'string') {
    throw new ServiceError(
      ErrorCode.PSYNC_S1004,
      `Convex bytes value must be a base64 string or binary buffer, got ${typeof value}`
    );
  }

  const normalized = value.replace(/\s+/g, '');
  const buffer = Buffer.from(normalized, 'base64');
  const canonical = buffer.toString('base64').replace(/=+$/g, '');
  if (normalized != '' && canonical != normalized.replace(/=+$/g, '')) {
    throw new ServiceError(ErrorCode.PSYNC_S1004, 'Convex bytes value is not valid base64');
  }

  return new Uint8Array(buffer);
}

/**
 * Reads the Convex field type from the Convex table column's JSON schema entry.
 */
export function readConvexFieldType(jsonSchemaProperty: unknown): string {
  if (!isRecord(jsonSchemaProperty)) {
    // Invalid schema property entry received
    return 'unknown';
  }

  // Convex can expose logical string subtypes through JSON schema `format`.
  // For example: { "type": "string", "format": "id" } or { "type": "string", "format": "bytes" }.
  // Check this before `type`, otherwise these would be treated as plain strings.
  const format =
    typeof jsonSchemaProperty.format == 'string' ? normalizeConvexJsonSchemaType(jsonSchemaProperty.format) : null;
  if (format == 'bytes' || format == 'id') {
    return format;
  }

  // Some schema exporters describe binary data as a base64-encoded string.
  // For example: { "type": "string", "contentEncoding": "base64" }.
  const contentEncoding =
    typeof jsonSchemaProperty.contentEncoding == 'string' ? jsonSchemaProperty.contentEncoding.toLowerCase() : null;
  if (contentEncoding == 'base64') {
    return 'bytes';
  }

  // Standard JSON schema uses a direct `type`.
  // For example: { "type": "integer" }, { "type": "number" }, or { "type": "boolean" }.
  const directType = typeof jsonSchemaProperty.type == 'string' ? jsonSchemaProperty.type : null;
  if (directType != null) {
    return normalizeConvexJsonSchemaType(directType);
  }

  // Nullable JSON schema fields can use a type array.
  // For example: { "type": ["string", "null"] }. Use the first concrete string type.
  if (Array.isArray(jsonSchemaProperty.type)) {
    const firstString = jsonSchemaProperty.type.find((entry) => typeof entry == 'string');
    if (typeof firstString == 'string') {
      return normalizeConvexJsonSchemaType(firstString);
    }
  }

  // Convex-generated or intermediate schema metadata can carry the same type
  // under non-standard keys. For example: { "valueType": "record" },
  // { "fieldType": "bytes" }, or { "kind": "array" }.
  const alternateType =
    typeof jsonSchemaProperty.valueType == 'string'
      ? jsonSchemaProperty.valueType
      : typeof jsonSchemaProperty.fieldType == 'string'
        ? jsonSchemaProperty.fieldType
        : typeof jsonSchemaProperty.kind == 'string'
          ? jsonSchemaProperty.kind
          : null;
  if (alternateType != null) {
    return normalizeConvexJsonSchemaType(alternateType);
  }

  return 'unknown';
}
