import { storage } from '@powersync/service-core';
import mysql from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql_utils.js';

export type GetReplicationColumnsOptions = {
  db: mysql.Connection;
  schema: string;
  table_name: string;
};

export type ReplicationIdentityColumnsResult = {
  columns: storage.ColumnDescriptor[];
  //   TODO maybe export an enum from the core package
  identity: string;
};

export async function getReplicationIdentityColumns(
  options: GetReplicationColumnsOptions
): Promise<ReplicationIdentityColumnsResult> {
  const { db, schema, table_name } = options;
  const [primaryKeyColumns] = await mysql_utils.retriedQuery({
    connection: db,
    query: `
      SELECT 
        s.COLUMN_NAME AS name,
        c.DATA_TYPE AS type
      FROM 
        INFORMATION_SCHEMA.STATISTICS s
        JOIN 
          INFORMATION_SCHEMA.COLUMNS c 
            ON 
              s.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND s.TABLE_NAME = c.TABLE_NAME
              AND s.COLUMN_NAME = c.COLUMN_NAME
      WHERE 
        s.TABLE_SCHEMA = ?
        AND s.TABLE_NAME = ?
        AND s.INDEX_NAME = 'PRIMARY'
      ORDER BY 
        s.SEQ_IN_INDEX;
      `,
    params: [schema, table_name]
  });

  if (primaryKeyColumns.length) {
    return {
      columns: primaryKeyColumns.map((row) => ({
        name: row.name,
        type: row.type
      })),
      identity: 'default'
    };
  }

  // TODO: test code with tables with unique keys, compound key etc.
  // No primary key, find the first valid unique key
  const [uniqueKeyColumns] = await mysql_utils.retriedQuery({
    connection: db,
    query: `
      SELECT 
        s.INDEX_NAME,
        s.COLUMN_NAME,
        c.DATA_TYPE,
        s.NON_UNIQUE,
        s.NULLABLE
      FROM 
        INFORMATION_SCHEMA.STATISTICS s
      JOIN 
        INFORMATION_SCHEMA.COLUMNS c
          ON 
            s.TABLE_SCHEMA = c.TABLE_SCHEMA
            AND s.TABLE_NAME = c.TABLE_NAME
            AND s.COLUMN_NAME = c.COLUMN_NAME
      WHERE 
        s.TABLE_SCHEMA = ?
        AND s.TABLE_NAME = ?
        AND s.INDEX_NAME != 'PRIMARY'
        AND s.NON_UNIQUE = 0
      ORDER BY s.SEQ_IN_INDEX;
      `,
    params: [schema, table_name]
  });

  if (uniqueKeyColumns.length > 0) {
    return {
      columns: uniqueKeyColumns.map((col) => ({
        name: col.COLUMN_NAME,
        type: col.DATA_TYPE
      })),
      identity: 'index'
    };
  }

  const [allColumns] = await mysql_utils.retriedQuery({
    connection: db,
    query: `
      SELECT 
        s.COLUMN_NAME AS name,
        c.DATA_TYPE as type
      FROM 
        INFORMATION_SCHEMA.COLUMNS s
        JOIN 
          INFORMATION_SCHEMA.COLUMNS c
            ON 
              s.TABLE_SCHEMA = c.TABLE_SCHEMA
              AND s.TABLE_NAME = c.TABLE_NAME
              AND s.COLUMN_NAME = c.COLUMN_NAME
      WHERE 
        s.TABLE_SCHEMA = ?
        AND s.TABLE_NAME = ?
      ORDER BY 
        s.ORDINAL_POSITION;
      `,
    params: [schema, table_name]
  });

  return {
    columns: allColumns.map((row) => ({
      name: row.name,
      type: row.type
    })),
    identity: 'full'
  };
}
