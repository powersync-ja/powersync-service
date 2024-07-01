import type * as pgwire from '@powersync/service-jpgwire';
import { pgwireRows } from '@powersync/service-jpgwire';
import { DatabaseSchema, internal_routes } from '@powersync/service-types';

import * as util from '../util/util-index.js';
import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';

export async function getConnectionsSchema(system: CorePowerSyncSystem): Promise<internal_routes.GetSchemaResponse> {
  if (system.config.connection == null) {
    return { connections: [] };
  }
  const schemas = await getConnectionSchema(system.requirePgPool());
  return {
    connections: [
      {
        schemas,
        tag: system.config.connection!.tag,
        id: system.config.connection!.id
      }
    ]
  };
}

export async function getConnectionSchema(db: pgwire.PgClient): Promise<DatabaseSchema[]> {
  // https://github.com/Borvik/vscode-postgres/blob/88ec5ed061a0c9bced6c5d4ec122d0759c3f3247/src/language/server.ts
  const results = await util.retriedQuery(
    db,
    `SELECT
  tbl.schemaname,
  tbl.tablename,
  tbl.quoted_name,
  json_agg(a ORDER BY attnum) as columns
FROM
  (
    SELECT
      n.nspname as schemaname,
      c.relname as tablename,
      (quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as quoted_name
    FROM
      pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE
      c.relkind = 'r'
      AND n.nspname not in ('information_schema', 'pg_catalog', 'pg_toast')
      AND n.nspname not like 'pg_temp_%'
      AND n.nspname not like 'pg_toast_temp_%'
      AND c.relnatts > 0
      AND has_schema_privilege(n.oid, 'USAGE') = true
      AND has_table_privilege(quote_ident(n.nspname) || '.' || quote_ident(c.relname), 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') = true
  ) as tbl
  LEFT JOIN (
    SELECT
      attrelid,
      attname,
      format_type(atttypid, atttypmod) as data_type,
      (SELECT typname FROM pg_catalog.pg_type WHERE oid = atttypid) as pg_type,
      attnum,
      attisdropped
    FROM
      pg_attribute
  ) as a ON (
    a.attrelid = tbl.quoted_name::regclass
    AND a.attnum > 0
    AND NOT a.attisdropped
    AND has_column_privilege(tbl.quoted_name, a.attname, 'SELECT, INSERT, UPDATE, REFERENCES')
  )
GROUP BY schemaname, tablename, quoted_name`
  );
  const rows = pgwireRows(results);

  let schemas: Record<string, any> = {};

  for (let row of rows) {
    const schema = (schemas[row.schemaname] ??= {
      name: row.schemaname,
      tables: []
    });
    const table = {
      name: row.tablename,
      columns: [] as any[]
    };
    schema.tables.push(table);

    const columnInfo = JSON.parse(row.columns);
    for (let column of columnInfo) {
      let pg_type = column.pg_type as string;
      if (pg_type.startsWith('_')) {
        pg_type = `${pg_type.substring(1)}[]`;
      }
      table.columns.push({
        name: column.attname,
        type: column.data_type,
        pg_type: pg_type
      });
    }
  }

  return Object.values(schemas);
}
