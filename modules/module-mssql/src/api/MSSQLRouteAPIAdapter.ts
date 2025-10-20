import {
  api,
  ParseSyncRulesOptions,
  PatternResult,
  ReplicationHeadCallback,
  ReplicationLagOptions
} from '@powersync/service-core';
import { Promise } from 'mssql';
import * as service_types from '@powersync/service-types';
import { SqlSyncRules, TablePattern } from '@powersync/service-sync-rules';
import { ResolvedConnectionConfig } from '../types/types.js';
import { ExecuteSqlResponse } from '@powersync/service-types/dist/routes.js';
import { MSSQLConnectionManager } from '../replication/MSSQLConnectionManager.js';

export class MSSQLRouteAPIAdapter implements api.RouteAPI {
  protected connectionManager: MSSQLConnectionManager;

  constructor(protected config: ResolvedConnectionConfig) {
    this.connectionManager = new MSSQLConnectionManager(config, {});
  }

  createReplicationHead<T>(callback: ReplicationHeadCallback<T>): Promise<T> {
    return Promise.resolve(undefined);
  }

  executeQuery(query: string, params: any[]): Promise<ExecuteSqlResponse> {
    return Promise.resolve(undefined);
  }

  getConnectionSchema(): Promise<service_types.DatabaseSchema[]> {
    return Promise.resolve([]);
  }

  getConnectionStatus(): Promise<service_types.ConnectionStatusV2> {
    return Promise.resolve(undefined);
  }

  getDebugTablesInfo(tablePatterns: TablePattern[], sqlSyncRules: SqlSyncRules): Promise<PatternResult[]> {
    return Promise.resolve([]);
  }

  getParseSyncRulesOptions(): ParseSyncRulesOptions {
    return {
      defaultSchema: this.connectionManager.schema
    };
  }

  getReplicationLagBytes(options: ReplicationLagOptions): Promise<number | undefined> {
    return Promise.resolve(undefined);
  }

  getSourceConfig(): Promise<service_types.configFile.ResolvedDataSourceConfig> {
    return Promise.resolve(undefined);
  }

  shutdown(): Promise<void> {
    return Promise.resolve(undefined);
  }
}
