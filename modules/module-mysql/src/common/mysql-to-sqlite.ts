import * as sync_rules from '@powersync/service-sync-rules';
import { ExpressionType } from '@powersync/service-sync-rules';
import { ColumnDescriptor } from '@powersync/service-core';
import mysql from 'mysql2';

export function toSQLiteRow(row: Record<string, any>, columns?: Map<string, ColumnDescriptor>): sync_rules.SqliteRow {
  for (let key in row) {
    if (row[key] instanceof Date) {
      const column = columns?.get(key);
      if (column?.typeId == mysql.Types.DATE) {
        // Only parse the date part
        row[key] = row[key].toISOString().split('T')[0];
      } else {
        row[key] = row[key].toISOString();
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
