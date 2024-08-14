import { api, storage } from '@powersync/service-core';

import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import mysql from 'mysql2/promise';
import * as types from '../types/types.js';
import { checkSourceConfiguration, readMasterComparableGtid, retriedQuery } from '../utils/mysql_utils.js';
import { getReplicationIdentityColumns, ReplicationIdentityColumnsResult } from '../utils/replication/schema.js';

export class MySQLAPIAdapter implements api.RouteAPI {
  protected pool: mysql.Pool;

  constructor(protected config: types.ResolvedConnectionConfig) {
    this.pool = mysql.createPool({
      host: config.hostname,
      user: config.username,
      password: config.password,
      database: config.database
    });
  }

  async shutdown(): Promise<void> {
    return this.pool.end();
  }

  async getSourceConfig(): Promise<service_types.configFile.DataSourceConfig> {
    return this.config;
  }

  async getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    const base = {
      id: this.config.id,
      uri: `mysql://${this.config.hostname}:${this.config.port}/${this.config.database}`
    };
    try {
      await retriedQuery({
        db: this.pool,
        query: `SELECT 'PowerSync connection test'`
      });
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: `${e.code} - message: ${e.message}` }]
      };
    }
    try {
      const errors = await checkSourceConfiguration(this.pool);
      if (errors.length) {
        return {
          ...base,
          connected: true,
          errors: errors.map((e) => ({ level: 'fatal', message: e }))
        };
      }
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
    if (!this.config.debug_enabled) {
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
      const [results, fields] = await this.pool.query<mysql.RowDataPacket[]>(query, params);
      return service_types.internal_routes.ExecuteSqlResponse.encode({
        success: true,
        results: {
          columns: fields.map((c) => c.name),
          rows: results.map((row) => {
            /**
             * Row will be in the format:
             * @rows: [ { test: 2 } ]
             */
            return fields.map((c) => {
              const value = row[c.name];
              const sqlValue = sync_rules.toSyncRulesValue(value);
              if (typeof sqlValue == 'bigint') {
                return Number(value);
              } else if (value instanceof Date) {
                return value.toISOString();
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

    /**
     * This is a hack. The schema should always be the database name in MySQL.
     * The default value of `public` is not valid.
     * We might need to implement this better where the original table patterns are created.
     */
    const mappedPatterns = tablePatterns.map((t) => new sync_rules.TablePattern(this.config.database, t.tablePattern));

    for (let tablePattern of mappedPatterns) {
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

        const [results] = await this.pool.query<mysql.RowDataPacket[]>(
          `SELECT TABLE_NAME AS table_name
           FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_SCHEMA = ?
           AND TABLE_NAME LIKE ?`,
          [schema, tablePattern.tablePattern]
        );

        for (let row of results) {
          const name = row.table_name as string;

          if (!name.startsWith(prefix)) {
            continue;
          }

          const details = await this.getDebugTableInfo(tablePattern, name, sqlSyncRules);
          patternResult.tables.push(details);
        }
      } else {
        const [results] = await this.pool.query<mysql.RowDataPacket[]>(
          `SELECT TABLE_NAME AS table_name
           FROM INFORMATION_SCHEMA.TABLES
           WHERE TABLE_SCHEMA = ?
           AND TABLE_NAME = ?`,
          [
            /**
             * TODO:!!!! The Schema here is the default Postgres `public`
             * which is not a thing in MySQL. The TABLE_SCHEMA in MySQL is the database name.
             */
            // schema
            this.config.database,
            tablePattern.tablePattern
          ]
        );

        if (results.length == 0) {
          // Table not found
          const details = await this.getDebugTableInfo(tablePattern, tablePattern.name, sqlSyncRules);
          patternResult.table = details;
        } else {
          const row = results[0];
          patternResult.table = await this.getDebugTableInfo(tablePattern, row.table_name, sqlSyncRules);
        }
      }
    }

    return result;
  }

  protected async getDebugTableInfo(
    tablePattern: sync_rules.TablePattern,
    tableName: string,
    syncRules: sync_rules.SqlSyncRules
  ): Promise<service_types.TableInfo> {
    const { schema } = tablePattern;

    let idColumnsResult: ReplicationIdentityColumnsResult | null = null;
    let idColumnsError: service_types.ReplicationError | null = null;
    try {
      idColumnsResult = await getReplicationIdentityColumns({
        db: this.pool,
        schema,
        table_name: tableName
      });
    } catch (ex) {
      idColumnsError = { level: 'fatal', message: ex.message };
    }

    const idColumns = idColumnsResult?.columns ?? [];
    const sourceTable = new storage.SourceTable(0, this.config.tag, tableName, schema, tableName, idColumns, true);
    const syncData = syncRules.tableSyncsData(sourceTable);
    const syncParameters = syncRules.tableSyncsParameters(sourceTable);

    if (idColumns.length == 0 && idColumnsError == null) {
      let message = `No replication id found for ${sourceTable.qualifiedName}. Replica identity: ${idColumnsResult?.identity}.`;
      if (idColumnsResult?.identity == 'default') {
        message += ' Configure a primary key on the table.';
      }
      idColumnsError = { level: 'fatal', message };
    }

    let selectError: service_types.ReplicationError | null = null;
    try {
      await retriedQuery({
        db: this.pool,
        query: `SELECT * FROM ${sourceTable.table} LIMIT 1`
      });
    } catch (e) {
      selectError = { level: 'fatal', message: e.message };
    }

    // Not sure if table level checks are possible yet
    return {
      schema: schema,
      name: tableName,
      pattern: tablePattern.isWildcard ? tablePattern.tablePattern : undefined,
      replication_id: idColumns.map((c) => c.name),
      data_queries: syncData,
      parameter_queries: syncParameters,
      errors: [idColumnsError, selectError].filter((error) => error != null) as service_types.ReplicationError[]
    };
  }

  async getReplicationLag(syncRulesId: string): Promise<number> {
    const [binLogFiles] = await retriedQuery({
      db: this.pool,
      query: `SHOW MASTER STATUS`
    });

    /**
     * The format will be something like:
     *  Array<{
     *     File: 'binlog.000002',
     *     Position: 197,
     *     Binlog_Do_DB: '',
     *     Binlog_Ignore_DB: '',
     *     Executed_Gtid_Set: 'a7b95c22-5987-11ef-ac5e-0242ac190003:1-12'
     *   }>
     * The file should be the syncRulesId. The Position should be the current head in bytes.
     */

    const matchingFile = binLogFiles.find((f) => f.File == syncRulesId);
    if (!matchingFile) {
      throw new Error(`Could not determine replication lag for file ${syncRulesId}: File not found.`);
    }

    //  TODO, it seems that the lag needs to be tracked manually

    throw new Error(`Could not determine replication lag for slot ${syncRulesId}`);
  }

  async getReplicationHead(): Promise<string> {
    return readMasterComparableGtid(this.pool);
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    throw new Error('not implemented');

    // https://github.com/Borvik/vscode-postgres/blob/88ec5ed061a0c9bced6c5d4ec122d0759c3f3247/src/language/server.ts
    //     const results = await pg_utils.retriedQuery(
    //       this.pool,
    //       `SELECT
    // tbl.schemaname,
    // tbl.tablename,
    // tbl.quoted_name,
    // json_agg(a ORDER BY attnum) as columns
    // FROM
    // (
    //   SELECT
    //     n.nspname as schemaname,
    //     c.relname as tablename,
    //     (quote_ident(n.nspname) || '.' || quote_ident(c.relname)) as quoted_name
    //   FROM
    //     pg_catalog.pg_class c
    //     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    //   WHERE
    //     c.relkind = 'r'
    //     AND n.nspname not in ('information_schema', 'pg_catalog', 'pg_toast')
    //     AND n.nspname not like 'pg_temp_%'
    //     AND n.nspname not like 'pg_toast_temp_%'
    //     AND c.relnatts > 0
    //     AND has_schema_privilege(n.oid, 'USAGE') = true
    //     AND has_table_privilege(quote_ident(n.nspname) || '.' || quote_ident(c.relname), 'SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') = true
    // ) as tbl
    // LEFT JOIN (
    //   SELECT
    //     attrelid,
    //     attname,
    //     format_type(atttypid, atttypmod) as data_type,
    //     (SELECT typname FROM pg_catalog.pg_type WHERE oid = atttypid) as pg_type,
    //     attnum,
    //     attisdropped
    //   FROM
    //     pg_attribute
    // ) as a ON (
    //   a.attrelid = tbl.quoted_name::regclass
    //   AND a.attnum > 0
    //   AND NOT a.attisdropped
    //   AND has_column_privilege(tbl.quoted_name, a.attname, 'SELECT, INSERT, UPDATE, REFERENCES')
    // )
    // GROUP BY schemaname, tablename, quoted_name`
    //     );
    //     const rows = pgwire.pgwireRows(results);
    //     let schemas: Record<string, any> = {};
    //     for (let row of rows) {
    //       const schema = (schemas[row.schemaname] ??= {
    //         name: row.schemaname,
    //         tables: []
    //       });
    //       const table = {
    //         name: row.tablename,
    //         columns: [] as any[]
    //       };
    //       schema.tables.push(table);
    //       const columnInfo = JSON.parse(row.columns);
    //       for (let column of columnInfo) {
    //         let pg_type = column.pg_type as string;
    //         if (pg_type.startsWith('_')) {
    //           pg_type = `${pg_type.substring(1)}[]`;
    //         }
    //         table.columns.push({
    //           name: column.attname,
    //           type: column.data_type,
    //           pg_type: pg_type
    //         });
    //       }
    //     }
    //     return Object.values(schemas);
  }
}
