import { api, storage } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';

import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import * as replication_utils from '../replication/replication-utils.js';
import * as types from '../types/types.js';
import * as pg_utils from '../utils/pgwire_utils.js';

export class PostgresRouteAPIAdapter implements api.RouteAPI {
  protected pool: pgwire.PgClient;

  connectionTag: string;
  // TODO this should probably be configurable one day
  publication_name = 'powersync';

  constructor(protected config: types.ResolvedConnectionConfig) {
    this.pool = pgwire.connectPgWirePool(config, {
      idleTimeout: 30_000
    });
    this.connectionTag = config.tag ?? sync_rules.DEFAULT_TAG;
  }

  async shutdown(): Promise<void> {
    await this.pool.end();
  }

  async getSourceConfig(): Promise<service_types.configFile.ResolvedDataSourceConfig> {
    return this.config;
  }

  async getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    const base = {
      id: this.config.id,
      uri: types.baseUri(this.config)
    };

    try {
      await pg_utils.retriedQuery(this.pool, `SELECT 'PowerSync connection test'`);
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: e.message }]
      };
    }

    try {
      await replication_utils.checkSourceConfiguration(this.pool);
    } catch (e) {
      return {
        ...base,
        connected: true,
        errors: [{ level: 'fatal', message: e.message }]
      };
    }

    return {
      ...base,
      connected: true,
      errors: []
    };
  }

  async executeQuery(query: string, params: any[]): Promise<service_types.internal_routes.ExecuteSqlResponse> {
    if (!this.config.debug_api) {
      return service_types.internal_routes.ExecuteSqlResponse.encode({
        results: {
          columns: [],
          rows: []
        },
        success: false,
        error: 'SQL querying is not enabled'
      });
    }

    try {
      const result = await this.pool.query({
        statement: query,
        params: params.map(pg_utils.autoParameter)
      });

      return service_types.internal_routes.ExecuteSqlResponse.encode({
        success: true,
        results: {
          columns: result.columns.map((c) => c.name),
          rows: result.rows.map((row) => {
            return row.map((value) => {
              const sqlValue = sync_rules.toSyncRulesValue(value);
              if (typeof sqlValue == 'bigint') {
                return Number(value);
              } else if (sync_rules.isJsonValue(sqlValue)) {
                return sqlValue;
              } else {
                return null;
              }
            });
          })
        }
      });
    } catch (e) {
      return service_types.internal_routes.ExecuteSqlResponse.encode({
        results: {
          columns: [],
          rows: []
        },
        success: false,
        error: e.message
      });
    }
  }

  async getDebugTablesInfo(
    tablePatterns: sync_rules.TablePattern[],
    sqlSyncRules: sync_rules.SqlSyncRules
  ): Promise<api.PatternResult[]> {
    let result: api.PatternResult[] = [];

    for (let tablePattern of tablePatterns) {
      const schema = tablePattern.schema;

      let patternResult: api.PatternResult = {
        schema: schema,
        pattern: tablePattern.tablePattern,
        wildcard: tablePattern.isWildcard
      };
      result.push(patternResult);

      if (tablePattern.isWildcard) {
        patternResult.tables = [];
        const prefix = tablePattern.tablePrefix;
        const results = await pg_utils.retriedQuery(this.pool, {
          statement: `SELECT c.oid AS relid, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        AND c.relkind = 'r'
        AND c.relname LIKE $2`,
          params: [
            { type: 'varchar', value: schema },
            { type: 'varchar', value: tablePattern.tablePattern }
          ]
        });

        for (let row of pgwire.pgwireRows(results)) {
          const name = row.table_name as string;
          const relationId = row.relid as number;
          if (!name.startsWith(prefix)) {
            continue;
          }
          const details = await this.getDebugTableInfo(tablePattern, name, relationId, sqlSyncRules);
          patternResult.tables.push(details);
        }
      } else {
        const results = await pg_utils.retriedQuery(this.pool, {
          statement: `SELECT c.oid AS relid, c.relname AS table_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        AND c.relkind = 'r'
        AND c.relname = $2`,
          params: [
            { type: 'varchar', value: schema },
            { type: 'varchar', value: tablePattern.tablePattern }
          ]
        });
        if (results.rows.length == 0) {
          // Table not found
          const details = await this.getDebugTableInfo(tablePattern, tablePattern.name, null, sqlSyncRules);
          patternResult.table = details;
        } else {
          const row = pgwire.pgwireRows(results)[0];
          const name = row.table_name as string;
          const relationId = row.relid as number;
          patternResult.table = await this.getDebugTableInfo(tablePattern, name, relationId, sqlSyncRules);
        }
      }
    }
    return result;
  }

  protected async getDebugTableInfo(
    tablePattern: sync_rules.TablePattern,
    name: string,
    relationId: number | null,
    syncRules: sync_rules.SqlSyncRules
  ): Promise<service_types.TableInfo> {
    const schema = tablePattern.schema;
    let id_columns_result: replication_utils.ReplicaIdentityResult | undefined = undefined;
    let id_columns_error = null;

    if (relationId != null) {
      try {
        id_columns_result = await replication_utils.getReplicationIdentityColumns(this.pool, relationId);
      } catch (e) {
        id_columns_error = { level: 'fatal', message: e.message };
      }
    }

    const id_columns = id_columns_result?.replicationColumns ?? [];

    const sourceTable = new storage.SourceTable(0, this.connectionTag, relationId ?? 0, schema, name, id_columns, true);

    const syncData = syncRules.tableSyncsData(sourceTable);
    const syncParameters = syncRules.tableSyncsParameters(sourceTable);

    if (relationId == null) {
      return {
        schema: schema,
        name: name,
        pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
        replication_id: [],
        data_queries: syncData,
        parameter_queries: syncParameters,
        // Also
        errors: [{ level: 'warning', message: `Table ${sourceTable.qualifiedName} not found.` }]
      };
    }
    if (id_columns.length == 0 && id_columns_error == null) {
      let message = `No replication id found for ${sourceTable.qualifiedName}. Replica identity: ${id_columns_result?.replicationIdentity}.`;
      if (id_columns_result?.replicationIdentity == 'default') {
        message += ' Configure a primary key on the table.';
      }
      id_columns_error = { level: 'fatal', message };
    }

    let selectError = null;
    try {
      await pg_utils.retriedQuery(this.pool, `SELECT * FROM ${sourceTable.escapedIdentifier} LIMIT 1`);
    } catch (e) {
      selectError = { level: 'fatal', message: e.message };
    }

    let replicateError = null;

    const publications = await pg_utils.retriedQuery(this.pool, {
      statement: `SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename = $3`,
      params: [
        { type: 'varchar', value: this.publication_name },
        { type: 'varchar', value: tablePattern.schema },
        { type: 'varchar', value: name }
      ]
    });
    if (publications.rows.length == 0) {
      replicateError = {
        level: 'fatal',
        message: `Table ${sourceTable.qualifiedName} is not part of publication '${this.publication_name}'. Run: \`ALTER PUBLICATION ${this.publication_name} ADD TABLE ${sourceTable.qualifiedName}\`.`
      };
    }

    return {
      schema: schema,
      name: name,
      pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
      replication_id: id_columns.map((c) => c.name),
      data_queries: syncData,
      parameter_queries: syncParameters,
      errors: [id_columns_error, selectError, replicateError].filter(
        (error) => error != null
      ) as service_types.ReplicationError[]
    };
  }

  async getReplicationLag(options: api.ReplicationLagOptions): Promise<number> {
    const { replication_identifier: slotName } = options;
    const results = await pg_utils.retriedQuery(this.pool, {
      statement: `SELECT
  slot_name,
  confirmed_flush_lsn, 
  pg_current_wal_lsn(), 
  (pg_current_wal_lsn() - confirmed_flush_lsn) AS lsn_distance
FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1;`,
      params: [{ type: 'varchar', value: slotName }]
    });
    const [row] = pgwire.pgwireRows(results);
    if (row) {
      return Number(row.lsn_distance);
    }

    throw new Error(`Could not determine replication lag for slot ${slotName}`);
  }

  async getReplicationHead(): Promise<string> {
    const [{ lsn }] = pgwire.pgwireRows(
      await pg_utils.retriedQuery(this.pool, `SELECT pg_logical_emit_message(false, 'powersync', 'ping') as lsn`)
    );
    return String(lsn);
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchemaV2[]> {
    // https://github.com/Borvik/vscode-postgres/blob/88ec5ed061a0c9bced6c5d4ec122d0759c3f3247/src/language/server.ts
    const results = await pg_utils.retriedQuery(
      this.pool,
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
    const rows = pgwire.pgwireRows(results);

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
          internal_type: pg_type
        });
      }
    }

    return Object.values(schemas);
  }
}