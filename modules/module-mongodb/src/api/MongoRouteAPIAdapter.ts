import { api } from '@powersync/service-core';
import * as mongo from 'mongodb';

import * as sync_rules from '@powersync/service-sync-rules';
import * as service_types from '@powersync/service-types';
import * as types from '../types/types.js';
import { MongoManager } from '../replication/MongoManager.js';

export class MongoRouteAPIAdapter implements api.RouteAPI {
  protected client: mongo.MongoClient;

  connectionTag: string;

  constructor(protected config: types.ResolvedConnectionConfig) {
    this.client = new MongoManager(config).client;
    this.connectionTag = config.tag ?? sync_rules.DEFAULT_TAG;
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
    // TODO: implement
    return '';
  }

  async getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    // TODO: Implement

    return [];
  }
}
