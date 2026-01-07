import * as lib_postgres from '@powersync/lib-service-postgres';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { api, ParseSyncRulesOptions, ReplicationHeadCallback } from '@powersync/service-core';
import * as pgwire from '@powersync/service-jpgwire';
import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import * as replication_utils from '../replication/replication-utils.js';
import { getDebugTableInfo } from '../replication/replication-utils.js';
import { KEEPALIVE_STATEMENT, PUBLICATION_NAME } from '../replication/WalStream.js';
import * as types from '../types/types.js';
import { getApplicationName } from '../utils/application-name.js';
import { CustomTypeRegistry } from '../types/registry.js';
import { PostgresTypeResolver } from '../types/resolver.js';

export class PostgresRouteAPIAdapter implements api.RouteAPI {
  private typeCache: PostgresTypeResolver;
  connectionTag: string;
  // TODO this should probably be configurable one day
  publicationName = PUBLICATION_NAME;

  static withConfig(config: types.ResolvedConnectionConfig) {
    const pool = pgwire.connectPgWirePool(config, {
      idleTimeout: 30_000,
      applicationName: getApplicationName()
    });
    return new PostgresRouteAPIAdapter(pool, config.tag, config);
  }

  /**
   * @param config - Required for the service; optional for tests.
   */
  constructor(
    protected pool: pgwire.PgClient,
    connectionTag?: string,
    private config?: types.ResolvedConnectionConfig
  ) {
    this.typeCache = new PostgresTypeResolver(pool);
    this.connectionTag = connectionTag ?? sync_rules.DEFAULT_TAG;
  }

  getParseSyncRulesOptions(): ParseSyncRulesOptions {
    return {
      defaultSchema: 'public'
    };
  }

  async shutdown(): Promise<void> {
    await this.pool.end();
  }

  async getSourceConfig(): Promise<service_types.configFile.ResolvedDataSourceConfig> {
    return this.config!;
  }

  async getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    const base = {
      id: this.config?.id ?? '',
      uri: this.config == null ? '' : types.baseUri(this.config)
    };

    try {
      await lib_postgres.retriedQuery(this.pool, `SELECT 'PowerSync connection test'`);
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: e.message }]
      };
    }

    try {
      await replication_utils.checkSourceConfiguration(this.pool, this.publicationName);
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
    if (!this.config?.debug_api) {
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
        params: params.map(lib_postgres.autoParameter)
      });

      return service_types.internal_routes.ExecuteSqlResponse.encode({
        success: true,
        results: {
          columns: result.columns.map((c) => c.name),
          rows: result.rows.map((row) => {
            return row.raw.map((raw, i) => {
              const value = pgwire.PgType.decode(raw, row.columns[i].typeOid);
              const sqlValue = sync_rules.applyValueContext(
                sync_rules.toSyncRulesValue(value),
                sync_rules.CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
              );
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
        const results = await lib_postgres.retriedQuery(this.pool, {
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
        const results = await lib_postgres.retriedQuery(this.pool, {
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
          patternResult.table = await this.getDebugTableInfo(tablePattern, tablePattern.name, null, sqlSyncRules);
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
    return getDebugTableInfo({
      db: this.pool,
      name: name,
      publicationName: this.publicationName,
      connectionTag: this.connectionTag,
      tablePattern: tablePattern,
      relationId: relationId,
      syncRules: syncRules
    });
  }

  async getReplicationLagBytes(options: api.ReplicationLagOptions): Promise<number | undefined> {
    const { bucketStorage } = options;
    const slotName = bucketStorage.slot_name;
    const results = await lib_postgres.retriedQuery(this.pool, {
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

    throw new ServiceError({
      status: 500,
      code: ErrorCode.PSYNC_S4001,
      description: `Could not determine replication lag for slot ${slotName}`
    });
  }

  async getReplicationHead(): Promise<string> {
    // On most Postgres versions, pg_logical_emit_message() returns the correct LSN.
    // However, on Aurora (Postgres compatible), it can return an entirely different LSN,
    // causing the write checkpoints to never be replicated back to the client.
    // For those, we need to use pg_current_wal_lsn() instead.
    const { results } = await lib_postgres.retriedQuery(this.pool, `SELECT pg_current_wal_lsn() as lsn`);

    const lsn = results[0].rows[0].decodeWithoutCustomTypes(0);
    return String(lsn);
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const currentLsn = await this.getReplicationHead();

    const r = await callback(currentLsn);

    // Note: This may not reliably trigger a new replication message on Postgres 11 or 12,
    // in which case there could be a delay in the client receiving the write checkpoint acknowledgement.
    // Postgres 12 already reached EOL, and this is not a critical issue, so we're not fixing it.
    // On postgres 13+, this works reliably.
    await lib_postgres.retriedQuery(this.pool, KEEPALIVE_STATEMENT);

    return r;
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    // https://github.com/Borvik/vscode-postgres/blob/88ec5ed061a0c9bced6c5d4ec122d0759c3f3247/src/language/server.ts
    const results = await lib_postgres.retriedQuery(
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
    atttypid,
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
GROUP BY schemaname, tablename, quoted_name
ORDER BY schemaname, tablename;`
    );
    await this.typeCache.fetchTypesForSchema();
    const rows = pgwire.pgwireRows(results);

    let schemas: Record<string, service_types.DatabaseSchema> = {};

    for (let row of rows) {
      const schema = (schemas[row.schemaname] ??= {
        name: row.schemaname,
        tables: []
      });
      const table: service_types.TableSchema = {
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

        const knownType = this.typeCache.registry.lookupType(Number(column.atttypid));
        table.columns.push({
          name: column.attname,
          sqlite_type: sync_rules.ExpressionType.fromTypeText(knownType.sqliteType()).typeFlags,
          type: column.data_type,
          internal_type: column.data_type,
          pg_type: pg_type
        });
      }
    }

    return Object.values(schemas);
  }
}
