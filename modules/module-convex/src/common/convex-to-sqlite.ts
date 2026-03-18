import {
  DatabaseInputValue,
  DatabaseInputRow,
  ExpressionType,
  SqliteInputRow,
  toSyncRulesRow
} from '@powersync/service-sync-rules';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { ConvexRawDocument } from '../client/ConvexApiClient.js';


/**
 * From Convex docs:
 * Every document in Convex automatically has two system fields:

    _id - a unique document ID with validator v.id("tableName")
    _creationTime - a creation timestamp with validator v.number()

    Technically _creationTime should be handlded differently as the other metadata excludes defined below, but we include it here for convenience since it's not necessary for replication.
 */
const INTERNAL_KEYS = new Set(['_table', '_deleted', '_ts', '_component', '_creationTime']); 

export function toSqliteInputRow(change: ConvexRawDocument, properties?: Record<string, unknown>): SqliteInputRow {
  const row: DatabaseInputRow = {};

  for (const [key, value] of Object.entries(change)) {
    if (INTERNAL_KEYS.has(key)) {
      continue;
    }

    row[key] = toConvexDatabaseValue(value, readConvexFieldType(properties?.[key]));
  }

  return toSyncRulesRow(row);
}

export function toExpressionTypeFromConvexType(type: string | undefined): ExpressionType {
  switch (normalizeConvexType(type)) {
    case 'int64':
      return ExpressionType.INTEGER;
    case 'float64':
      return ExpressionType.REAL;
    case 'boolean':
      return ExpressionType.INTEGER;
    case 'bytes':
      return ExpressionType.BLOB;
    case 'null':
      return ExpressionType.NONE;
    case 'array':
    case 'object':
    case 'record':
      return ExpressionType.TEXT;
    case 'id':
    case 'string':
    case 'unknown':
    default:
      return ExpressionType.TEXT;
  }
}

export function extractProperties(schema: Record<string, any>) {
  const direct = schema.properties;
  if (isRecord(direct)) {
    return direct;
  }

  const nested = schema.schema?.properties;
  if (isRecord(nested)) {
    return nested;
  }

  return {};
}

export function readConvexFieldType(value: unknown): string {
  if (!isRecord(value)) {
    return 'unknown';
  }

  const format = typeof value.format == 'string' ? normalizeConvexType(value.format) : null;
  if (format == 'bytes' || format == 'id') {
    return format;
  }

  const contentEncoding = typeof value.contentEncoding == 'string' ? value.contentEncoding.toLowerCase() : null;
  if (contentEncoding == 'base64') {
    return 'bytes';
  }

  const directType = typeof value.type == 'string' ? value.type : null;
  if (directType != null) {
    return normalizeConvexType(directType);
  }

  if (Array.isArray(value.type)) {
    const firstString = value.type.find((entry) => typeof entry == 'string');
    if (typeof firstString == 'string') {
      return normalizeConvexType(firstString);
    }
  }

  const alternateType =
    typeof value.valueType == 'string'
      ? value.valueType
      : typeof value.fieldType == 'string'
        ? value.fieldType
        : typeof value.kind == 'string'
          ? value.kind
          : null;
  if (alternateType != null) {
    return normalizeConvexType(alternateType);
  }

  return 'unknown';
}

function toConvexDatabaseValue(value: unknown, type: string): DatabaseInputValue {
  switch (normalizeConvexType(type)) {
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

function normalizeConvexType(type: string | undefined): string {
  const normalized = type?.trim().toLowerCase();
  switch (normalized) {
    case 'id':
    case 'string':
    case 'bytes':
    case 'array':
    case 'object':
    case 'record':
    case 'null':
      return normalized;
    case 'integer':
    case 'int64':
      return 'int64';
    case 'number':
    case 'float':
    case 'float64':
      return 'float64';
    case 'bool':
    case 'boolean':
      return 'boolean';
    case 'bytea':
    case 'blob':
      return 'bytes';
    default:
      return normalized ?? 'unknown';
  }
}

function isRecord(value: unknown): value is Record<string, any> {
  return typeof value == 'object' && value != null && !Array.isArray(value);
}
