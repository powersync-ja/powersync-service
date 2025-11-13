import sql from 'mssql';
import { DatabaseInputRow, SqliteInputRow, toSyncRulesRow } from '@powersync/service-sync-rules';

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
        // TODO: Confirm case sql.TYPES.UDT
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

function toISOTimeString(date: Date): string | null {
  return isNaN(date.getTime()) ? null : date.toISOString().split('T')[1];
}
