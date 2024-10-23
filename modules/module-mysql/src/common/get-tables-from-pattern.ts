import * as sync_rules from '@powersync/service-sync-rules';
import mysql from 'mysql2/promise';

export type GetDebugTablesInfoOptions = {
  connection: mysql.Connection;
  tablePattern: sync_rules.TablePattern;
};

export async function getTablesFromPattern(options: GetDebugTablesInfoOptions): Promise<Set<string>> {
  const { connection, tablePattern } = options;
  const schema = tablePattern.schema;

  if (tablePattern.isWildcard) {
    const [results] = await connection.query<mysql.RowDataPacket[]>(
      `SELECT
            TABLE_NAME AS table_name
           FROM 
            INFORMATION_SCHEMA.TABLES
           WHERE 
            TABLE_SCHEMA = ?
            AND TABLE_NAME LIKE ?`,
      [schema, tablePattern.tablePattern]
    );

    return new Set(
      results
        .filter((result) => result.table_name.startsWith(tablePattern.tablePrefix))
        .map((result) => result.table_name)
    );
  } else {
    const [[match]] = await connection.query<mysql.RowDataPacket[]>(
      `SELECT
            TABLE_NAME AS table_name
           FROM 
            INFORMATION_SCHEMA.TABLES
           WHERE 
            TABLE_SCHEMA = ?
            AND TABLE_NAME = ?`,
      [tablePattern.schema, tablePattern.tablePattern]
    );
    // Only return the first result
    return new Set([match.table_name]);
  }
}
