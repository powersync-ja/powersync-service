import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
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
  [SupportedJSONSchemaPropertyType.BYTES]: ExpressionType.BLOB,
  [SupportedJSONSchemaPropertyType.ARRAY]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.OBJECT]: ExpressionType.TEXT,
  [SupportedJSONSchemaPropertyType.NULL]: ExpressionType.NONE,
  [SupportedJSONSchemaPropertyType.INT64]: ExpressionType.INTEGER,
  [SupportedJSONSchemaPropertyType.FLOAT64]: ExpressionType.REAL,
  [SupportedJSONSchemaPropertyType.BOOLEAN]: ExpressionType.INTEGER,
  [SupportedJSONSchemaPropertyType.UNKNOWN]: ExpressionType.TEXT
} as const;

const INTERNAL_KEYS = new Set(['_table', '_deleted', '_ts', '_component']);

export function jsonSchemaToSQLiteType(jsonType: SupportedJSONSchemaPropertyType): ExpressionType {
  return CONVEX_TO_SQLITE_TYPE_MAP[jsonType];
}

export function toSqliteInputRow(
  change: ConvexRawDocument,
  jsonSchemaProperties?: Record<string, unknown>
): SqliteInputRow {
  const row: DatabaseInputRow = {};

  for (const [key, value] of Object.entries(change)) {
    if (INTERNAL_KEYS.has(key)) {
      continue;
    }

    row[key] = toDatabaseValue(value, readConvexFieldJsonType(jsonSchemaProperties?.[key]));
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
 * We typically receive the Schema for each table from Convex in the form of a JSON schema.
 * Each column in a table has a JSON Schema property type.
 *
 * In some cases, we have observed that the Schema returned from Convex does not include column
 * definitions unless if rows exist which have values defined for the column.
 * We don't exclusively rely on the jsonSchemaType for this reason. Type mappings are performed using
 * type checks in these scenarios.
 */
function toDatabaseValue(value: unknown, jsonSchemaType: SupportedJSONSchemaPropertyType): DatabaseInputValue {
  // If we have the schema available for the value, we can perform basic assertions.
  switch (jsonSchemaType) {
    case SupportedJSONSchemaPropertyType.BYTES:
      if (typeof value != 'string') {
        // we should have received a string for this
        throw new ServiceError(
          ErrorCode.PSYNC_S1004,
          `Convex bytes value must be a base64 string or binary buffer, got ${typeof value}`
        );
      }
      return new Uint8Array(Buffer.from(value, 'base64'));
    case SupportedJSONSchemaPropertyType.INT64:
      // Convex can return values for int64 as a qouted string
      if (typeof value == 'number' || typeof value == 'string') {
        // We can cast this number to bigint
        return BigInt(value);
      }
      break;
    case SupportedJSONSchemaPropertyType.BOOLEAN:
      if (typeof value == 'number') {
        return value != 0 ? 1n : 0n;
      } else if (typeof value == 'boolean') {
        return value;
      } else {
        throw new ServiceError(ErrorCode.PSYNC_S1004, `Convex boolean value must be a boolean, got ${typeof value}`);
      }
    case SupportedJSONSchemaPropertyType.NULL:
      return null;
    case SupportedJSONSchemaPropertyType.FLOAT64:
      if (typeof value !== 'number') {
        return Number(value);
      } else {
        return value;
      }
    case SupportedJSONSchemaPropertyType.ARRAY:
    case SupportedJSONSchemaPropertyType.OBJECT:
      return JSONBig.stringify(value);
    case SupportedJSONSchemaPropertyType.ID:
    case SupportedJSONSchemaPropertyType.STRING:
      if (typeof value !== 'string') {
        throw new ServiceError(
          ErrorCode.PSYNC_S1004,
          `Convex ${jsonSchemaType} value must be a string, got ${typeof value}`
        );
      } else {
        return value;
      }
    case SupportedJSONSchemaPropertyType.UNKNOWN:
      // TODO! It seems like Convex might not report the schema value for values which have not
      // been populated in the DB yet. This can cause many issues - and we need to work around this.
      // We perform runtime checks and conversions at this point.
      if (value == null) {
        return null;
      } else if (
        typeof value == 'string' ||
        typeof value == 'boolean' ||
        typeof value == 'number' ||
        typeof value == 'bigint'
      ) {
        // We can return the value as is in this case
        return value;
      } else if (Array.isArray(value) || typeof value == 'object') {
        return JSONBig.stringify(value);
      } else {
        return null;
      }
  }

  return null;
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
