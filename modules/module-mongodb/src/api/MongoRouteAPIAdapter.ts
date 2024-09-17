import { api, ParseSyncRulesOptions } from '@powersync/service-core';
import * as mongo from 'mongodb';

import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import * as types from '../types/types.js';
import { MongoManager } from '../replication/MongoManager.js';
import { createCheckpoint, getMongoLsn } from '../replication/MongoRelation.js';

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
    // TODO: Implement
    const base = {
      id: this.config.id,
      uri: types.baseUri(this.config)
    };
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
    // TODO: Implement
    return [];
  }

  async getReplicationLag(syncRulesId: string): Promise<number> {
    // TODO: Implement

    return 0;
  }

  async getReplicationHead(): Promise<string> {
    return createCheckpoint(this.db);
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    // TODO: Implement

    return [];
  }
}
