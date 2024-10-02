import { api, ParseSyncRulesOptions, SourceTable } from '@powersync/service-core';
import * as mongo from 'mongodb';

import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import * as types from '../types/types.js';
import { MongoManager } from '../replication/MongoManager.js';
import { createCheckpoint, getMongoLsn } from '../replication/MongoRelation.js';
import { escapeRegExp } from '../utils.js';

export class MongoRouteAPIAdapter implements api.RouteAPI {
  protected client: mongo.MongoClient;
  private db: mongo.Db;

  connectionTag: string;
  defaultSchema: string;

  constructor(protected config: types.ResolvedConnectionConfig) {
    const manager = new MongoManager(config);
    this.client = manager.client;
    this.db = manager.db;
    this.defaultSchema = manager.db.databaseName;
    this.connectionTag = config.tag ?? sync_rules.DEFAULT_TAG;
  }

  getParseSyncRulesOptions(): ParseSyncRulesOptions {
    return {
      defaultSchema: this.defaultSchema
    };
  }

  async shutdown(): Promise<void> {
    await this.client.close();
  }

  async getSourceConfig(): Promise<service_types.configFile.DataSourceConfig> {
    return this.config;
  }

  async getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    const base = {
      id: this.config.id,
      uri: types.baseUri(this.config)
    };

    try {
      await this.client.connect();
      await this.db.command({ hello: 1 });
    } catch (e) {
      return {
        ...base,
        connected: false,
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
    return service_types.internal_routes.ExecuteSqlResponse.encode({
      results: {
        columns: [],
        rows: []
      },
      success: false,
      error: 'SQL querying is not supported for MongoDB'
    });
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

      let nameFilter: RegExp | string;
      if (tablePattern.isWildcard) {
        nameFilter = new RegExp('^' + escapeRegExp(tablePattern.tablePrefix));
      } else {
        nameFilter = tablePattern.name;
      }

      // Check if the collection exists
      const collections = await this.client
        .db(schema)
        .listCollections(
          {
            name: nameFilter
          },
          { nameOnly: true }
        )
        .toArray();

      if (tablePattern.isWildcard) {
        patternResult.tables = [];
        for (let collection of collections) {
          const sourceTable = new SourceTable(
            0,
            this.connectionTag,
            collection.name,
            schema,
            collection.name,
            [],
            true
          );
          const syncData = sqlSyncRules.tableSyncsData(sourceTable);
          const syncParameters = sqlSyncRules.tableSyncsParameters(sourceTable);
          patternResult.tables.push({
            schema,
            name: collection.name,
            replication_id: ['_id'],
            data_queries: syncData,
            parameter_queries: syncParameters,
            errors: []
          });
        }
      } else {
        const sourceTable = new SourceTable(
          0,
          this.connectionTag,
          tablePattern.name,
          schema,
          tablePattern.name,
          [],
          true
        );

        const syncData = sqlSyncRules.tableSyncsData(sourceTable);
        const syncParameters = sqlSyncRules.tableSyncsParameters(sourceTable);

        if (collections.length == 1) {
          patternResult.table = {
            schema,
            name: tablePattern.name,
            replication_id: ['_id'],
            data_queries: syncData,
            parameter_queries: syncParameters,
            errors: []
          };
        } else {
          patternResult.table = {
            schema,
            name: tablePattern.name,
            replication_id: ['_id'],
            data_queries: syncData,
            parameter_queries: syncParameters,
            errors: [{ level: 'warning', message: `Collection ${schema}.${tablePattern.name} not found` }]
          };
        }
      }
    }
    return result;
  }

  async getReplicationLag(syncRulesId: string): Promise<number | undefined> {
    // There is no fast way to get replication lag in bytes in MongoDB.
    // We can get replication lag in seconds, but need a different API for that.
    return undefined;
  }

  async getReplicationHead(): Promise<string> {
    return createCheckpoint(this.client, this.db);
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    // TODO: Implement

    return [];
  }
}
