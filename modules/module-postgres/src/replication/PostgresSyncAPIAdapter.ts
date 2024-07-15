import { api, replication } from '@powersync/service-core';

import { TablePattern } from '@powersync/service-sync-rules';
import { DatabaseSchema } from '@powersync/service-types';

// TODO
import { ExecuteSqlResponse } from '@powersync/service-types/src/routes.js';

import { PostgresConnectionConfig } from '../types/types.js';

export class PostgresSyncAPIAdapter implements api.SyncAPI {
  constructor(protected config: PostgresConnectionConfig) {}

  getDiagnostics(): Promise<{ connected: boolean; errors?: Array<{ level: string; message: string }> }> {
    throw new Error('Method not implemented.');
  }

  getDebugTablesInfo(tablePatterns: TablePattern[]): Promise<replication.PatternResult[]> {
    throw new Error('Method not implemented.');
  }

  getReplicationLag(): Promise<number> {
    throw new Error('Method not implemented.');
  }

  getCheckpoint(): Promise<bigint> {
    throw new Error('Method not implemented.');
  }

  getConnectionSchema(): Promise<DatabaseSchema[]> {
    throw new Error('Method not implemented.');
  }

  executeSQL(sql: string, params: any[]): Promise<ExecuteSqlResponse> {
    throw new Error('Method not implemented.');
  }
}
