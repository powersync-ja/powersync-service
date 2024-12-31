import * as sync_rules from '@powersync/service-sync-rules';
import { ExpressionType } from '@powersync/service-sync-rules';
import { ColumnDescriptor } from '@powersync/service-core';
import mysql from 'mysql2';
import { JSONBig, JsonContainer } from '@powersync/service-jsonbig';
import { ColumnDefinition, TableMapEntry } from '@powersync/mysql-zongji';

export enum ADDITIONAL_MYSQL_TYPES {
  DATETIME2 = 18,
  TIMESTAMP2 = 17,
  BINARY = 100,
  VARBINARY = 101,
  TEXT = 102
}

export const MySQLTypesMap: { [key: number]: string } = {};
for (const [name, code] of Object.entries(mysql.Types)) {
  MySQLTypesMap[code as number] = name;
}
for (const [name, code] of Object.entries(ADDITIONAL_MYSQL_TYPES)) {
  MySQLTypesMap[code as number] = name;
}

export function toColumnDescriptors(columns: mysql.FieldPacket[]): Map<string, ColumnDescriptor>;
export function toColumnDescriptors(tableMap: TableMapEntry): Map<string, ColumnDescriptor>;

export function toColumnDescriptors(columns: mysql.FieldPacket[] | TableMapEntry): Map<string, ColumnDescriptor> {
  const columnMap = new Map<string, ColumnDescriptor>();
  if (Array.isArray(columns)) {
    for (const column of columns) {
      columnMap.set(column.name, toColumnDescriptorFromFieldPacket(column));
    }
  } else {
    for (const column of columns.columns) {
      columnMap.set(column.name, toColumnDescriptorFromDefinition(column));
    }
  }

  return columnMap;
}

export function toColumnDescriptorFromFieldPacket(column: mysql.FieldPacket): ColumnDescriptor {
  let typeId = column.type!;
  const BINARY_FLAG = 128;
  const MYSQL_ENUM_FLAG = 256;
  const MYSQL_SET_FLAG = 2048;

  switch (column.type) {
    case mysql.Types.STRING:
      if (((column.flags as number) & BINARY_FLAG) !== 0) {
        typeId = ADDITIONAL_MYSQL_TYPES.BINARY;
      } else if (((column.flags as number) & MYSQL_ENUM_FLAG) !== 0) {
        typeId = mysql.Types.ENUM;
      } else if (((column.flags as number) & MYSQL_SET_FLAG) !== 0) {
        typeId = mysql.Types.SET;
      }
      break;

    case mysql.Types.VAR_STRING:
      typeId = ((column.flags as number) & BINARY_FLAG) !== 0 ? ADDITIONAL_MYSQL_TYPES.VARBINARY : column.type;
      break;
    case mysql.Types.BLOB:
      typeId = ((column.flags as number) & BINARY_FLAG) === 0 ? ADDITIONAL_MYSQL_TYPES.TEXT : column.type;
      break;
  }

  const columnType = MySQLTypesMap[typeId];

  return {
    name: column.name,
    type: columnType,
    typeId: typeId
  };
}

export function toColumnDescriptorFromDefinition(column: ColumnDefinition): ColumnDescriptor {
  let typeId = column.type;

  switch (column.type) {
    case mysql.Types.STRING:
      typeId = !column.charset ? ADDITIONAL_MYSQL_TYPES.BINARY : column.type;
      break;
    case mysql.Types.VAR_STRING:
    case mysql.Types.VARCHAR:
      typeId = !column.charset ? ADDITIONAL_MYSQL_TYPES.VARBINARY : column.type;
      break;
    case mysql.Types.BLOB:
      typeId = column.charset ? ADDITIONAL_MYSQL_TYPES.TEXT : column.type;
      break;
  }

  const columnType = MySQLTypesMap[typeId];

  return {
    name: column.name,
    type: columnType,
    typeId: typeId
  };
}

export function toSQLiteRow(row: Record<string, any>, columns: Map<string, ColumnDescriptor>): sync_rules.SqliteRow {
  let result: sync_rules.DatabaseInputRow = {};
  for (let key in row) {
    // We are very much expecting the column to be there
    const column = columns.get(key)!;

    if (row[key] !== null) {
      switch (column.typeId) {
        case mysql.Types.DATE:
          // Only parse the date part
          {
            const date = row[key] as Date;
            if (isNaN(date.getTime()) && false) {
              // Invalid dates, such as 2024-00-00.
              // we can't do anything meaningful with this, so just use null.
              result[key] = null;
            } else {
              result[key] = date.toISOString().split('T')[0];
            }
          }
          break;
        case mysql.Types.DATETIME:
        case ADDITIONAL_MYSQL_TYPES.DATETIME2:
        case mysql.Types.TIMESTAMP:
        case ADDITIONAL_MYSQL_TYPES.TIMESTAMP2:
          {
            const date = row[key] as Date;
            if (isNaN(date.getTime())) {
              // Invalid dates, such as 2024-00-00.
              // we can't do anything meaningful with this, so just use null.
              result[key] = null;
            } else {
              result[key] = date.toISOString();
            }
          }
          break;
        case mysql.Types.JSON:
          if (typeof row[key] === 'string') {
            result[key] = new JsonContainer(row[key]);
          }
          break;
        case mysql.Types.BIT:
        case mysql.Types.BLOB:
        case mysql.Types.TINY_BLOB:
        case mysql.Types.MEDIUM_BLOB:
        case mysql.Types.LONG_BLOB:
        case ADDITIONAL_MYSQL_TYPES.BINARY:
        case ADDITIONAL_MYSQL_TYPES.VARBINARY:
          result[key] = new Uint8Array(Object.values(row[key]));
          break;
        case mysql.Types.LONGLONG:
          if (typeof row[key] === 'string') {
            result[key] = BigInt(row[key]);
          } else if (typeof row[key] === 'number') {
            // Zongji returns BIGINT as a number when it can be represented as a number
            result[key] = BigInt(row[key]);
          } else {
            result[key] = row[key];
          }
          break;
        case mysql.Types.TINY:
        case mysql.Types.SHORT:
        case mysql.Types.LONG:
        case mysql.Types.INT24:
          // Handle all integer values a BigInt
          if (typeof row[key] === 'number') {
            result[key] = BigInt(row[key]);
          } else {
            result[key] = row[key];
          }
          break;
        case mysql.Types.SET:
          // Convert to JSON array from string
          const values = row[key].split(',');
          result[key] = JSONBig.stringify(values);
          break;
        default:
          result[key] = row[key];
          break;
      }
    }
  }
  return sync_rules.toSyncRulesRow(result);
}

export function toExpressionTypeFromMySQLType(mysqlType: string | undefined): ExpressionType {
  if (!mysqlType) {
    return ExpressionType.TEXT;
  }

  const upperCaseType = mysqlType.toUpperCase();
  // Handle type with parameters like VARCHAR(255), DECIMAL(10,2), etc.
  const baseType = upperCaseType.split('(')[0];

  switch (baseType) {
    case 'BIT':
    case 'BOOL':
    case 'BOOLEAN':
    case 'TINYINT':
    case 'SMALLINT':
    case 'MEDIUMINT':
    case 'INT':
    case 'INTEGER':
    case 'BIGINT':
    case 'UNSIGNED BIGINT':
      return ExpressionType.INTEGER;
    case 'BINARY':
    case 'VARBINARY':
    case 'TINYBLOB':
    case 'MEDIUMBLOB':
    case 'LONGBLOB':
    case 'BLOB':
    case 'GEOMETRY':
    case 'POINT':
    case 'LINESTRING':
    case 'POLYGON':
    case 'MULTIPOINT':
    case 'MULTILINESTRING':
    case 'MULTIPOLYGON':
    case 'GEOMETRYCOLLECTION':
      return ExpressionType.BLOB;
    case 'FLOAT':
    case 'DOUBLE':
    case 'REAL':
      return ExpressionType.REAL;
    case 'JSON':
      return ExpressionType.TEXT;
    default:
      // In addition to the normal text types, includes: DECIMAL, NUMERIC, DATE, TIME, DATETIME, TIMESTAMP, YEAR, ENUM, SET
      return ExpressionType.TEXT;
  }
}
