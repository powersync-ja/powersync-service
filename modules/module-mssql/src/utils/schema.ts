import { SourceEntityDescriptor } from '@powersync/service-core';
import { TablePattern } from '@powersync/service-sync-rules';
import { MSSQLConnectionManager } from '../replication/MSSQLConnectionManager.js';
import { MSSQLColumnDescriptor } from '../types/mssql-data-types.js';
import sql from 'mssql';

export interface GetColumnsOptions {
  connectionManager: MSSQLConnectionManager;
  schema: string;
  tableName: string;
}

async function getColumns(options: GetColumnsOptions): Promise<MSSQLColumnDescriptor[]> {
  const { connectionManager, schema, tableName } = options;

  const { recordset: columnResults } = await connectionManager.query(`
      SELECT
        col.name AS [name],
        typ.name AS [type],
        typ.system_type_id AS type_id,
        typ.user_type_id AS user_type_id
      FROM sys.columns AS col
        JOIN sys.tables AS tbl ON tbl.object_id = col.object_id
        JOIN sys.schemas AS sch ON sch.schema_id = tbl.schema_id
        JOIN sys.types AS typ ON typ.user_type_id = col.user_type_id
      WHERE sch.name = @schema
        AND tbl.name = @tableName
      ORDER BY col.column_id;
      `, [
        { name: 'schema', type: sql.VarChar(sql.MAX), value: schema },
        { name: 'tableName', type: sql.VarChar(sql.MAX), value: tableName },
      ]);

  return columnResults.map((row) => {
    return {
      name: row.name,
      type: row.type,
      typeId: row.type_id,
      userTypeId: row.user_type_id
    };
  });
}

export interface GetReplicationIdentityColumnsOptions {
  connectionManager: MSSQLConnectionManager;
  schema: string;
  tableName: string;
}

export interface ReplicationIdentityColumnsResult {
  columns: MSSQLColumnDescriptor[];
  identity: 'default' | 'nothing' | 'full' | 'index';
}

export async function getReplicationIdentityColumns(
  options: GetReplicationIdentityColumnsOptions
): Promise<ReplicationIdentityColumnsResult> {
  const { connectionManager, schema, tableName } = options;
  const { recordset: primaryKeyColumns } = await connectionManager.query(`
      SELECT
        col.name AS [name],
        typ.name AS [type],
        typ.system_type_id AS type_id,
        typ.user_type_id AS user_type_id
      FROM sys.tables           AS tbl
        JOIN sys.schemas        AS sch ON sch.schema_id = tbl.schema_id
        JOIN sys.indexes        AS idx ON idx.object_id = tbl.object_id AND idx.is_primary_key = 1
        JOIN sys.index_columns  AS idx_col ON idx_col.object_id = idx.object_id AND idx_col.index_id = idx.index_id
        JOIN sys.columns        AS col ON col.object_id  = idx_col.object_id AND col.column_id  = idx_col.column_id
        JOIN sys.types          AS typ ON typ.user_type_id = col.user_type_id
      WHERE sch.name = @schema
        AND tbl.name = @tableName
      ORDER BY idx_col.key_ordinal;
      `, [
        { name: 'schema', type: sql.VarChar(sql.MAX), value: schema },
        { name: 'tableName', type: sql.VarChar(sql.MAX), value: tableName },
      ]);

  if (primaryKeyColumns.length > 0) {
    return {
      columns: primaryKeyColumns.map((row) => ({
        name: row.name,
        type: row.type,
        typeId: row.type_id,
        userTypeId: row.user_type_id
      })),
      identity: 'default'
    };
  }

  // No primary key, check if any of the columns have a unique constraint we can use
  const { recordset: uniqueKeyColumns } = await connectionManager.query(`
      SELECT
        col.name AS [name],
        typ.name AS [type],
        typ.system_type_id AS type_id,
        typ.user_type_id AS user_type_id
      FROM sys.tables            AS tbl
        JOIN sys.schemas         AS sch ON sch.schema_id = tbl.schema_id
        JOIN sys.indexes         AS idx ON idx.object_id = tbl.object_id AND idx.is_unique_constraint = 1
        JOIN sys.index_columns   AS idx_col ON idx_col.object_id = idx.object_id AND idx_col.index_id = idx.index_id
        JOIN sys.columns         AS col ON col.object_id  = idx_col.object_id AND col.column_id  = idx_col.column_id
        JOIN sys.types           AS typ ON typ.user_type_id = col.user_type_id
      WHERE sch.name = @schema
        AND tbl.name = @tableName
      ORDER BY idx_col.key_ordinal;
      `, [
        { name: 'schema', type: sql.VarChar(sql.MAX), value: schema },
        { name: 'tableName', type: sql.VarChar(sql.MAX), value: tableName },
      ]);

  if (uniqueKeyColumns.length > 0) {
    return {
      columns: uniqueKeyColumns.map((row) => ({
        name: row.name,
        type: row.type,
        typeId: row.type_id,
        userTypeId: row.user_type_id
      })),
      identity: 'index'
    };
  }

  const allColumns = await getColumns(options);

  return {
    columns: allColumns,
    identity: 'full'
  };
}

export type ResolvedTable = Omit<SourceEntityDescriptor, 'replicaIdColumns'>;

export async function getTablesFromPattern(
  connectionManager: MSSQLConnectionManager,
  tablePattern: TablePattern
): Promise<ResolvedTable[]> {
  if (tablePattern.isWildcard) {
    const { recordset: tableResults } = await connectionManager.query(`
        SELECT
          tbl.name      AS [table],
          sch.name      AS [schema],
          tbl.object_id AS object_id
        FROM sys.tables tbl
          JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
        WHERE sch.name = @schema
          AND tbl.name LIKE @tablePattern
      `, [
        { name: 'schema', type: sql.VarChar(sql.MAX), value: tablePattern.schema },
        { name: 'tablePattern', type: sql.VarChar(sql.MAX), value: tablePattern.tablePattern },
      ]);

    return tableResults
      .map((row) => {
        return {
          objectId: row.object_id,
          schema: row.schema,
          name: row.table
        };
      })
      .filter((table: ResolvedTable) => table.name.startsWith(tablePattern.tablePrefix));
  } else {
    const { recordset: tableResults } = await connectionManager.query(
      `
        SELECT
          tbl.name      AS [table],
          sch.name      AS [schema],
          tbl.object_id AS object_id
        FROM sys.tables tbl
          JOIN sys.schemas sch ON tbl.schema_id = sch.schema_id
          WHERE sch.name = @schema
          AND tbl.name = @tablePattern
      `, [
        { name: 'schema', type: sql.VarChar(sql.MAX), value: tablePattern.schema },
        { name: 'tablePattern', type: sql.VarChar(sql.MAX), value: tablePattern.tablePattern },
      ]);

    return tableResults.map((row) => {
      return {
        objectId: row.object_id,
        schema: row.schema,
        name: row.table
      };
    });
  }
}
