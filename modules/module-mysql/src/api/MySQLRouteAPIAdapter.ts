import { api, storage } from '@powersync/service-core';

import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import mysql from 'mysql2/promise';
import * as types from '../types/types.js';
import { checkSourceConfiguration, readMasterComparableGtid, retriedQuery } from '../utils/mysql_utils.js';
import { getReplicationIdentityColumns, ReplicationIdentityColumnsResult } from '../utils/replication/schema.js';

type SchemaResult = {
  schemaname: string;
  tablename: string;
  columns: Array<{ data_type: string; column_name: string }>;
};

export class MySQLRouteAPIAdapter implements api.RouteAPI {
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
          `SELECT
            TABLE_NAME AS table_name
           FROM 
            INFORMATION_SCHEMA.TABLES
           WHERE 
            TABLE_SCHEMA = ?
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
          `SELECT
            TABLE_NAME AS table_name
           FROM 
            INFORMATION_SCHEMA.TABLES
           WHERE 
            TABLE_SCHEMA = ?
            AND TABLE_NAME = ?`,
          [tablePattern.schema, tablePattern.tablePattern]
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

  async getReplicationLag(options: api.ReplicationLagOptions): Promise<number> {
    const { replication_identifier: binLogFilename, last_checkpoint_identifier } = options;

    const [[gtidEvent]] = await retriedQuery({
      db: this.pool,
      query: `
      SHOW 
        BINLOG EVENTS IN ?
      WHERE
        Event_type = 'Previous_gtids'
        AND Info = ?
      LIMIT 
        1
        `,
      params: [binLogFilename, last_checkpoint_identifier]
    });

    if (!gtidEvent) {
      throw new Error(`Could not find binlog event for GTID`);
    }

    // This is the BinLog file position at the last replicated GTID.
    // The position is the file offset in bytes.
    const headBinlogPosition = gtidEvent.Pos;

    // Get the position of the latest event in the BinLog file
    const [[binLogEntry]] = await retriedQuery({
      db: this.pool,
      query: `
      SHOW 
        master status
      WHERE
        File = ?
        AND Info = ?
      LIMIT 
        1
        `,
      params: [binLogFilename]
    });

    if (!binLogEntry) {
      throw new Error(`Could not find master status for binlog file`);
    }

    return binLogEntry.Position - headBinlogPosition;
  }

  async getReplicationHead(): Promise<string> {
    return readMasterComparableGtid(this.pool);
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    const [results] = await retriedQuery({
      db: this.pool,
      query: `
        SELECT 
          tbl.schemaname,
          tbl.tablename,
          tbl.quoted_name,
          JSON_ARRAYAGG(JSON_OBJECT('column_name', a.column_name, 'data_type', a.data_type)) AS columns
        FROM
          (
            SELECT 
              TABLE_SCHEMA AS schemaname,
              TABLE_NAME AS tablename,
              CONCAT('\`', TABLE_SCHEMA, '\`.\`', TABLE_NAME, '\`') AS quoted_name
            FROM 
              INFORMATION_SCHEMA.TABLES
            WHERE 
              TABLE_TYPE = 'BASE TABLE'
              AND TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
          ) AS tbl
          LEFT JOIN 
            (
              SELECT 
                TABLE_SCHEMA AS schemaname,
                TABLE_NAME AS tablename,
                COLUMN_NAME AS column_name,
                COLUMN_TYPE AS data_type
              FROM 
                INFORMATION_SCHEMA.COLUMNS
            ) AS a 
            ON 
              tbl.schemaname = a.schemaname 
              AND tbl.tablename = a.tablename
        GROUP BY 
          tbl.schemaname, tbl.tablename, tbl.quoted_name;
      `
    });

    /**
     * Reduces the SQL results into a Record of {@link DatabaseSchema}
     * then returns the values as an array.
     */

    return Object.values(
      (results as SchemaResult[]).reduce((hash: Record<string, service_types.DatabaseSchema>, result) => {
        const schema =
          hash[result.schemaname] ||
          (hash[result.schemaname] = {
            name: result.schemaname,
            tables: []
          });

        schema.tables.push({
          name: result.tablename,
          columns: result.columns.map((column) => ({
            name: column.column_name,
            type: column.data_type,
            // FIXME remove this
            pg_type: column.data_type
          }))
        });

        return hash;
      }, {})
    );
  }
}
