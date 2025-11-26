import { api, ParseSyncRulesOptions, ReplicationHeadCallback, storage } from '@powersync/service-core';

import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import mysql from 'mysql2/promise';
import * as common from '../common/common-index.js';
import * as mysql_utils from '../utils/mysql-utils.js';
import * as types from '../types/types.js';
import { toExpressionTypeFromMySQLType } from '../common/common-index.js';

type SchemaResult = {
  schema_name: string;
  table_name: string;
  columns: string;
};

export class MySQLRouteAPIAdapter implements api.RouteAPI {
  protected pool: mysql.Pool;

  constructor(protected config: types.ResolvedConnectionConfig) {
    this.pool = mysql_utils.createPool(config).promise();
  }

  async shutdown(): Promise<void> {
    await this.pool.end();
  }

  async getSourceConfig(): Promise<service_types.configFile.ResolvedDataSourceConfig> {
    return this.config;
  }

  getParseSyncRulesOptions(): ParseSyncRulesOptions {
    return {
      // In MySQL Schema and Database are the same thing. There is no default database
      defaultSchema: this.config.database
    };
  }

  async getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    const base = {
      id: this.config.id,
      uri: `mysql://${this.config.hostname}:${this.config.port}/${this.config.database}`
    };
    try {
      await this.retriedQuery({
        query: `SELECT 'PowerSync connection test'`
      });
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: `${e.code} - message: ${e.message}` }]
      };
    }
    const connection = await this.pool.getConnection();
    try {
      const errors = await common.checkSourceConfiguration(connection);
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
    } finally {
      connection.release();
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
              const sqlValue = sync_rules.applyValueContext(
                sync_rules.toSyncRulesValue(value),
                sync_rules.CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY
              );
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
          patternResult.table = await this.getDebugTableInfo(tablePattern, tablePattern.name, sqlSyncRules);
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

    let idColumnsResult: common.ReplicationIdentityColumnsResult | null = null;
    let idColumnsError: service_types.ReplicationError | null = null;
    let connection: mysql.PoolConnection | null = null;
    try {
      connection = await this.pool.getConnection();
      idColumnsResult = await common.getReplicationIdentityColumns({
        connection: connection,
        schema,
        tableName: tableName
      });
    } catch (ex) {
      idColumnsError = { level: 'fatal', message: ex.message };
    } finally {
      connection?.release();
    }

    const idColumns = idColumnsResult?.columns ?? [];
    const sourceTable = new storage.SourceTable({
      id: 0,
      connectionTag: this.config.tag,
      objectId: tableName,
      schema: schema,
      name: tableName,
      replicaIdColumns: idColumns,
      snapshotComplete: true
    });
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
      await this.retriedQuery({
        query: `SELECT * FROM ${sourceTable.name} LIMIT 1`
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

  async getReplicationLagBytes(options: api.ReplicationLagOptions): Promise<number | undefined> {
    const { bucketStorage } = options;
    const lastCheckpoint = await bucketStorage.getCheckpoint();

    const current = lastCheckpoint.lsn
      ? common.ReplicatedGTID.fromSerialized(lastCheckpoint.lsn)
      : common.ReplicatedGTID.ZERO;

    const connection = await this.pool.getConnection();
    const head = await common.readExecutedGtid(connection);
    const lag = await current.distanceTo(connection, head);
    connection.release();
    if (lag == null) {
      throw new Error(`Could not determine replication lag`);
    }

    return lag;
  }

  async getReplicationHead(): Promise<string> {
    const connection = await this.pool.getConnection();
    const result = await common.readExecutedGtid(connection);
    connection.release();
    return result.comparable;
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const head = await this.getReplicationHead();
    const r = await callback(head);

    // TODO: make sure another message is replicated

    return r;
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    const [results] = await this.retriedQuery({
      query: `
        SELECT 
          tbl.schema_name,
          tbl.table_name,
          tbl.quoted_name,
          JSON_ARRAYAGG(JSON_OBJECT('column_name', a.column_name, 'data_type', a.data_type)) AS columns
        FROM
          (
            SELECT 
              TABLE_SCHEMA AS schema_name,
              TABLE_NAME AS table_name,
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
                TABLE_SCHEMA AS schema_name,
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name,
                COLUMN_TYPE AS data_type
              FROM 
                INFORMATION_SCHEMA.COLUMNS
            ) AS a 
            ON 
              tbl.schema_name = a.schema_name 
              AND tbl.table_name = a.table_name
        GROUP BY 
          tbl.schema_name, tbl.table_name, tbl.quoted_name;
      `
    });

    /**
     * Reduces the SQL results into a Record of {@link DatabaseSchema}
     * then returns the values as an array.
     */

    return Object.values(
      (results as SchemaResult[]).reduce((hash: Record<string, service_types.DatabaseSchema>, result) => {
        const schema =
          hash[result.schema_name] ||
          (hash[result.schema_name] = {
            name: result.schema_name,
            tables: []
          });

        const columns = JSON.parse(result.columns).map((column: { data_type: string; column_name: string }) => ({
          name: column.column_name,
          type: column.data_type,
          sqlite_type: toExpressionTypeFromMySQLType(column.data_type).typeFlags,
          internal_type: column.data_type,
          pg_type: column.data_type
        }));

        schema.tables.push({
          name: result.table_name,
          columns: columns
        });

        return hash;
      }, {})
    );
  }

  protected async retriedQuery(options: { query: string; params?: any[] }) {
    const connection = await this.pool.getConnection();

    return mysql_utils
      .retriedQuery({
        connection: connection,
        query: options.query,
        params: options.params
      })
      .finally(() => connection.release());
  }
}
