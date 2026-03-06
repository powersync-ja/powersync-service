import { api, ParseSyncRulesOptions, ReplicationHeadCallback, ReplicationLagOptions, SourceTable } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import { toConvexLsn } from '../common/ConvexLSN.js';
import { isConvexCheckpointTable } from '../common/ConvexCheckpoints.js';
import { extractProperties, readConvexFieldType, toExpressionTypeFromConvexType } from '../common/convex-to-sqlite.js';
import { ConvexConnectionManager } from '../replication/ConvexConnectionManager.js';
import * as types from '../types/types.js';

export class ConvexRouteAPIAdapter implements api.RouteAPI {
  protected connectionManager: ConvexConnectionManager;

  constructor(protected config: types.ResolvedConvexConnectionConfig) {
    this.connectionManager = new ConvexConnectionManager(config);
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
      await this.connectionManager.client.getJsonSchemas();
      return {
        ...base,
        connected: true,
        errors: []
      };
    } catch (error) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: error instanceof Error ? error.message : `${error}` }]
      };
    }
  }

  async getDebugTablesInfo(
    tablePatterns: sync_rules.TablePattern[],
    sqlSyncRules: sync_rules.SqlSyncRules
  ): Promise<api.PatternResult[]> {
    const schema = await this.connectionManager.client.getJsonSchemas();
    const tablesByName = new Map(schema.tables.map((table) => [table.tableName, table]));

    const result: api.PatternResult[] = [];

    for (const tablePattern of tablePatterns) {
      const patternResult: api.PatternResult = {
        schema: tablePattern.schema,
        pattern: tablePattern.tablePattern,
        wildcard: tablePattern.isWildcard
      };

      result.push(patternResult);

      if (tablePattern.connectionTag != this.connectionManager.connectionTag) {
        if (tablePattern.isWildcard) {
          patternResult.tables = [];
        } else {
          patternResult.table = createTableInfo({
            tablePattern,
            connectionTag: this.connectionManager.connectionTag,
            syncRules: sqlSyncRules,
            errors: [{ level: 'warning', message: 'Skipped: connection tag does not match Convex connection tag' }]
          });
        }
        continue;
      }

      const matchedTableNames = [...tablesByName.keys()]
        .filter((name) => {
          //Convex doesn't support user-defined schemas, so this is more a forwards compatibility check for when multiple connections are supported
          if (tablePattern.schema != this.connectionManager.schema) {
            return false;
          }
          if (isConvexCheckpointTable(name)) {
            return false;
          }
          if (tablePattern.isWildcard) {
            return name.startsWith(tablePattern.tablePrefix);
          }
          return name == tablePattern.name;
        })
        .sort();

      if (tablePattern.isWildcard) {
        patternResult.tables = matchedTableNames.map((tableName) =>
          createTableInfo({
            tablePattern,
            connectionTag: this.connectionManager.connectionTag,
            syncRules: sqlSyncRules,
            tableName
          })
        );
      } else {
        const tableName = matchedTableNames[0] ?? tablePattern.name;
        patternResult.table = createTableInfo({
          tablePattern,
          connectionTag: this.connectionManager.connectionTag,
          syncRules: sqlSyncRules,
          tableName,
          errors:
            matchedTableNames.length == 0
              ? [{ level: 'warning', message: `Table ${tablePattern.schema}.${tablePattern.name} not found` }]
              : []
        });
      }
    }

    return result;
  }

  //for convex we can calculate time-based lag, but not byte-based lag
  async getReplicationLagBytes(options: ReplicationLagOptions): Promise<number | undefined> {
    return undefined;
  }

  async createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    const head = await this.connectionManager.client.getHeadCursor();
    await this.connectionManager.client.createWriteCheckpointMarker();
    return await callback(toConvexLsn(head));
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    const schema = await this.connectionManager.client.getJsonSchemas();

    return [
      {
        name: this.connectionManager.schema,
        tables: schema.tables
          .filter((table) => !isConvexCheckpointTable(table.tableName))
          .map((table) => ({
            name: table.tableName,
            columns: Object.entries({
              ...extractProperties(table.schema),
              _id: { type: 'id' }
            })
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([columnName, property]) => {
                const jsonType = readConvexFieldType(property);
                const sqliteType = toExpressionTypeFromConvexType(jsonType);

                return {
                  name: columnName,
                  type: jsonType,
                  sqlite_type: sqliteType.typeFlags,
                  internal_type: jsonType,
                  pg_type: jsonType
                };
              })
          }))
      }
    ];
  }

  async executeQuery(query: string, params: any[]): Promise<service_types.internal_routes.ExecuteSqlResponse> {
    return service_types.internal_routes.ExecuteSqlResponse.encode({
      results: {
        columns: [],
        rows: []
      },
      success: false,
      error: 'SQL querying is not supported for Convex'
    });
  }

  async shutdown(): Promise<void> {
    await this.connectionManager.end();
  }

  async [Symbol.asyncDispose]() {
    await this.shutdown();
  }

  getParseSyncRulesOptions(): ParseSyncRulesOptions {
    return {
      defaultSchema: this.connectionManager.schema
    };
  }
}

function createTableInfo(options: {
  tablePattern: sync_rules.TablePattern;
  connectionTag: string;
  syncRules: sync_rules.SqlSyncRules;
  tableName?: string;
  errors?: service_types.ReplicationError[];
}) {
  const tableName =
    options.tableName ?? (options.tablePattern.isWildcard ? options.tablePattern.tablePrefix : options.tablePattern.name);
  const sourceTable = new SourceTable({
    id: 0,
    connectionTag: options.connectionTag,
    objectId: tableName,
    schema: options.tablePattern.schema,
    name: tableName,
    replicaIdColumns: [{ name: '_id' }],
    snapshotComplete: true
  });

  return {
    schema: options.tablePattern.schema,
    name: tableName,
    pattern: options.tablePattern.isWildcard ? options.tablePattern.tablePattern : undefined,
    replication_id: ['_id'],
    data_queries: options.syncRules.tableSyncsData(sourceTable),
    parameter_queries: options.syncRules.tableSyncsParameters(sourceTable),
    errors: options.errors ?? []
  };
}
