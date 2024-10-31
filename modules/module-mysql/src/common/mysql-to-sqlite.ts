import * as sync_rules from '@powersync/service-sync-rules';
import { ExpressionType } from '@powersync/service-sync-rules';
import { ColumnDescriptor } from '@powersync/service-core';
import mysql from 'mysql2';
import { JsonContainer } from '@powersync/service-jsonbig';
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

  switch (column.type) {
    case mysql.Types.STRING:
      typeId = ((column.flags as number) & BINARY_FLAG) !== 0 ? ADDITIONAL_MYSQL_TYPES.BINARY : column.type;
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
  for (let key in row) {
    // We are very much expecting the column to be there
    const column = columns.get(key)!;

    if (row[key] !== null) {
      switch (column.typeId) {
        case mysql.Types.DATE:
          // Only parse the date part
          row[key] = row[key].toISOString().split('T')[0];
          break;
        case mysql.Types.DATETIME:
        case ADDITIONAL_MYSQL_TYPES.DATETIME2:
        case mysql.Types.TIMESTAMP:
        case ADDITIONAL_MYSQL_TYPES.TIMESTAMP2:
          row[key] = row[key].toISOString();
          break;
        case mysql.Types.JSON:
          if (typeof row[key] === 'string') {
            row[key] = new JsonContainer(row[key]);
          }
          break;
        case mysql.Types.BIT:
        case mysql.Types.BLOB:
        case mysql.Types.TINY_BLOB:
        case mysql.Types.MEDIUM_BLOB:
        case mysql.Types.LONG_BLOB:
        case ADDITIONAL_MYSQL_TYPES.BINARY:
        case ADDITIONAL_MYSQL_TYPES.VARBINARY:
          row[key] = new Uint8Array(Object.values(row[key]));
          break;
        case mysql.Types.LONGLONG:
          if (typeof row[key] === 'string') {
            row[key] = BigInt(row[key]);
          }
          break;
        // case ADDITIONAL_MYSQL_TYPES.TEXT:
        //   row[key] = row[key].toString();
        //   break;
      }
    }
  }
  return sync_rules.toSyncRulesRow(row);
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
