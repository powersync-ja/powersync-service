import sql from 'mssql';
import {
  DatabaseInputRow,
  ExpressionType,
  SQLITE_FALSE,
  SQLITE_TRUE,
  SqliteInputRow,
  toSyncRulesRow
} from '@powersync/service-sync-rules';
import { MSSQLUserDefinedType } from '../types/mssql-data-types.js';

export function toSqliteInputRow(row: any, columns: sql.IColumnMetadata): SqliteInputRow {
  let result: DatabaseInputRow = {};
  for (const key in row) {
    // We are very much expecting the column to be there
    const columnMetadata = columns[key];

    if (row[key] !== null) {
      switch (columnMetadata.type) {
        case sql.TYPES.BigInt:
          // MSSQL returns BIGINT as a string to avoid precision loss
          if (typeof row[key] === 'string') {
            result[key] = BigInt(row[key]);
          }
          break;
        case sql.TYPES.Bit:
          // MSSQL returns BIT as boolean
          result[key] = row[key] ? SQLITE_TRUE : SQLITE_FALSE;
          break;
        // Convert Dates to string
        case sql.TYPES.Date:
          result[key] = toISODateString(row[key] as Date);
          break;
        case sql.TYPES.Time:
          result[key] = toISOTimeString(row[key] as Date);
          break;
        case sql.TYPES.DateTime:
        case sql.TYPES.DateTime2:
        case sql.TYPES.SmallDateTime:
        case sql.TYPES.DateTimeOffset: // The offset is lost when the driver converts to Date. This needs to be handled in the sql query.
          const date = row[key] as Date;
          result[key] = isNaN(date.getTime()) ? null : date.toISOString();
          break;
        case sql.TYPES.Binary:
        case sql.TYPES.VarBinary:
        case sql.TYPES.Image:
          result[key] = new Uint8Array(row[key]);
          break;
        // TODO: Spatial types need to be converted to binary WKB, they are returned as a non standard object currently
        case sql.TYPES.Geometry:
        case sql.TYPES.Geography:
          result[key] = JSON.stringify(row[key]);
          break;
        case sql.TYPES.UDT:
          if (columnMetadata.udt.name === MSSQLUserDefinedType.HIERARCHYID) {
            result[key] = new Uint8Array(row[key]);
            break;
          } else {
            result[key] = row[key];
          }
          break;
        default:
          result[key] = row[key];
      }
    } else {
      // If the value is null, we just set it to null
      result[key] = null;
    }
  }
  return toSyncRulesRow(result);
}

function toISODateString(date: Date): string | null {
  return isNaN(date.getTime()) ? null : date.toISOString().split('T')[0];
}

/**
 *  MSSQL time format is HH:mm:ss[.nnnnnnn]
 * @param date
 * @returns
 */
function toISOTimeString(date: Date): string | null {
  return isNaN(date.getTime()) ? null : date.toISOString().split('T')[1].replace('Z', '');
}

/**
 * Converts MSSQL type names to SQLite ExpressionType
 * @param mssqlType - The MSSQL type name (e.g., 'int', 'varchar', 'datetime2')
 */
export function toExpressionTypeFromMSSQLType(mssqlType: string | undefined): ExpressionType {
  if (!mssqlType) {
    return ExpressionType.TEXT;
  }

  const baseType = mssqlType.toUpperCase();
  switch (baseType) {
    case 'BIT':
    case 'TINYINT':
    case 'SMALLINT':
    case 'INT':
    case 'INTEGER':
    case 'BIGINT':
      return ExpressionType.INTEGER;
    case 'BINARY':
    case 'VARBINARY':
    case 'IMAGE':
    case 'TIMESTAMP':
      return ExpressionType.BLOB;
    case 'FLOAT':
    case 'REAL':
    case 'MONEY':
    case 'SMALLMONEY':
    case 'DECIMAL':
    case 'NUMERIC':
      return ExpressionType.REAL;
    case 'JSON':
      return ExpressionType.TEXT;
    // System and extended types
    case 'SYSNAME':
      // SYSNAME is essentially NVARCHAR(128), map to TEXT
      return ExpressionType.TEXT;
    case 'HIERARCHYID':
      // HIERARCHYID is a CLR UDT representing hierarchical data, stored as string representation
      return ExpressionType.TEXT;
    case 'GEOMETRY':
    case 'GEOGRAPHY':
      // Spatial CLR UDT types, typically stored as WKT (Well-Known Text) strings
      return ExpressionType.TEXT;
    case 'VECTOR':
      // Vector type (SQL Server 2022+), stored as binary data
      return ExpressionType.BLOB;
    default:
      // In addition to the normal text types, includes: VARCHAR, NVARCHAR, CHAR, NCHAR, TEXT, NTEXT, DATE, TIME, DATETIME, DATETIME2, SMALLDATETIME, DATETIMEOFFSET, XML, UNIQUEIDENTIFIER, SQL_VARIANT
      return ExpressionType.TEXT;
  }
}

export interface CDCRowToSqliteRowOptions {
  row: any;
  columns: sql.IColumnMetadata;
}
// CDC metadata columns in CDCS rows that should be excluded
const CDC_METADATA_COLUMNS = ['__$operation', '__$start_lsn', '__$end_lsn', '__$seqval', '__$update_mask'];
/**
 * Convert CDC row data to SqliteRow format.
 * CDC rows include table columns plus CDC metadata columns (__$operation, __$start_lsn, etc.)
 * which we filter out.
 */
export function CDCToSqliteRow(options: CDCRowToSqliteRowOptions): SqliteInputRow {
  const { row, columns } = options;
  const filteredRow: DatabaseInputRow = {};
  for (const key in row) {
    if (!CDC_METADATA_COLUMNS.includes(key)) {
      filteredRow[key] = row[key];
    }
  }
  return toSqliteInputRow(filteredRow, columns);
}
