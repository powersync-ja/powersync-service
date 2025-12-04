import {
  api,
  ParseSyncRulesOptions,
  PatternResult,
  ReplicationHeadCallback,
  ReplicationLagOptions
} from '@powersync/service-core';
import * as service_types from '@powersync/service-types';
import { SqlSyncRules, TablePattern } from '@powersync/service-sync-rules';
import * as types from '../types/types.js';
import { ExecuteSqlResponse } from '@powersync/service-types/dist/routes.js';
import { MSSQLConnectionManager } from '../replication/MSSQLConnectionManager.js';
import {
  checkSourceConfiguration,
  createCheckpoint,
  getDebugTableInfo,
  getLatestLSN,
  POWERSYNC_CHECKPOINTS_TABLE
} from '../utils/mssql.js';
import { getTablesFromPattern, ResolvedTable } from '../utils/schema.js';
import { toExpressionTypeFromMSSQLType } from '../common/mssqls-to-sqlite.js';
import sql from 'mssql';

export class MSSQLRouteAPIAdapter implements api.RouteAPI {
  protected connectionManager: MSSQLConnectionManager;

  constructor(protected config: types.ResolvedMSSQLConnectionConfig) {
    this.connectionManager = new MSSQLConnectionManager(config, {});
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const currentLSN = await getLatestLSN(this.connectionManager);
    const result = await callback(currentLSN.toString());

    // Updates the powersync checkpoints table on the source database, ensuring that an update with a newer LSN will be captured by the CDC.
    await createCheckpoint(this.connectionManager);

    return result;
  }

  async executeQuery(query: string, params: any[]): Promise<ExecuteSqlResponse> {
    return service_types.internal_routes.ExecuteSqlResponse.encode({
      results: {
        columns: [],
        rows: []
      },
      success: false,
      error: 'SQL querying is not supported for SQL Server'
    });
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    const { recordset: results } = await this.connectionManager.query(`
      SELECT 
        sch.name AS schema_name,
        tbl.name AS table_name,
        col.name AS column_name,
        typ.name AS data_type,
        CASE 
          WHEN typ.name IN ('nvarchar', 'nchar') 
            AND col.max_length > 0 
            AND col.max_length != -1
          THEN typ.name + '(' + CAST(col.max_length / 2 AS VARCHAR) + ')'
          WHEN typ.name IN ('varchar', 'char', 'varbinary', 'binary') 
            AND col.max_length > 0 
            AND col.max_length != -1
          THEN typ.name + '(' + CAST(col.max_length AS VARCHAR) + ')'
          WHEN typ.name IN ('varchar', 'nvarchar', 'char', 'nchar') 
            AND col.max_length = -1
          THEN typ.name + '(MAX)'
          WHEN typ.name IN ('decimal', 'numeric')
            AND col.precision > 0
          THEN typ.name + '(' + CAST(col.precision AS VARCHAR) + ',' + CAST(col.scale AS VARCHAR) + ')'
          WHEN typ.name IN ('float', 'real')
            AND col.precision > 0
          THEN typ.name + '(' + CAST(col.precision AS VARCHAR) + ')'
          ELSE typ.name
        END AS formatted_type
      FROM sys.tables AS tbl
        JOIN sys.schemas AS sch ON sch.schema_id = tbl.schema_id
        JOIN sys.columns AS col ON col.object_id = tbl.object_id
        JOIN sys.types AS typ ON typ.user_type_id = col.user_type_id
      WHERE sch.name = @schema
        AND sch.name NOT IN ('sys', 'INFORMATION_SCHEMA', 'cdc')
        AND tbl.name NOT IN ('systranschemas', @checkpointsTable)
        AND tbl.type = 'U'
        AND col.is_computed = 0
      ORDER BY sch.name, tbl.name, col.column_id
    `, [
      { name: 'schema', type: sql.VarChar(sql.MAX), value: this.connectionManager.schema },
      { name: 'checkpointsTable', type: sql.VarChar(sql.MAX), value: POWERSYNC_CHECKPOINTS_TABLE },
    ]);

    /**
     * Reduces the SQL results into a Record of {@link DatabaseSchema}
     * then returns the values as an array.
     */
    const schemas: Record<string, service_types.DatabaseSchema> = {};

    for (const row of results) {
      const schemaName = row.schema_name as string;
      const tableName = row.table_name as string;
      const columnName = row.column_name as string;
      const dataType = row.data_type as string;
      const formattedType = (row.formatted_type as string) || dataType;

      const schema =
        schemas[schemaName] ||
        (schemas[schemaName] = {
          name: schemaName,
          tables: []
        });

      let table = schema.tables.find((t) => t.name === tableName);
      if (!table) {
        table = {
          name: tableName,
          columns: []
        };
        schema.tables.push(table);
      }

      table.columns.push({
        name: columnName,
        type: formattedType,
        sqlite_type: toExpressionTypeFromMSSQLType(dataType).typeFlags,
        internal_type: formattedType,
        pg_type: formattedType
      });
    }

    return Object.values(schemas);
  }

  async getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    const base = {
      id: this.config?.id ?? '',
      uri: this.config == null ? '' : types.baseUri(this.config)
    };

    try {
      await this.connectionManager.query(`SELECT 'PowerSync connection test'`);
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: `${e.code} - message: ${e.message}` }]
      };
    }

    try {
      const errors = await checkSourceConfiguration(this.connectionManager);
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

  async getDebugTablesInfo(tablePatterns: TablePattern[], sqlSyncRules: SqlSyncRules): Promise<PatternResult[]> {
    const result: PatternResult[] = [];

    for (const tablePattern of tablePatterns) {
      const schema = tablePattern.schema;
      const patternResult: PatternResult = {
        schema: schema,
        pattern: tablePattern.tablePattern,
        wildcard: tablePattern.isWildcard
      };
      result.push(patternResult);

      const tables = await getTablesFromPattern(this.connectionManager, tablePattern);
      if (tablePattern.isWildcard) {
        patternResult.tables = [];
        for (const table of tables) {
          const details = await getDebugTableInfo({
            connectionManager: this.connectionManager,
            tablePattern,
            table,
            syncRules: sqlSyncRules
          });
          patternResult.tables.push(details);
        }
      } else {
        if (tables.length == 0) {
          // This should technically never happen, but we'll handle it anyway.
          const resolvedTable: ResolvedTable = {
            objectId: 0,
            schema: schema,
            name: tablePattern.name
          };
          patternResult.table = await getDebugTableInfo({
            connectionManager: this.connectionManager,
            tablePattern,
            table: resolvedTable,
            syncRules: sqlSyncRules
          });
        } else {
          patternResult.table = await getDebugTableInfo({
            connectionManager: this.connectionManager,
            tablePattern,
            table: tables[0],
            syncRules: sqlSyncRules
          });
        }
      }
    }

    return result;
  }

  getParseSyncRulesOptions(): ParseSyncRulesOptions {
    return {
      defaultSchema: this.connectionManager.schema
    };
  }

  async getReplicationLagBytes(options: ReplicationLagOptions): Promise<number | undefined> {
    return undefined;
  }

  async getSourceConfig(): Promise<service_types.configFile.ResolvedDataSourceConfig> {
    return this.config;
  }

  async [Symbol.asyncDispose]() {
    await this.shutdown();
  }

  async shutdown(): Promise<void> {
    await this.connectionManager.end();
  }
}
