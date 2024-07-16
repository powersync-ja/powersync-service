import * as pgwire from '@powersync/service-jpgwire';
import { api, replication } from '@powersync/service-core';

import { TablePattern } from '@powersync/service-sync-rules';
import { DatabaseSchema, internal_routes } from '@powersync/service-types';

// TODO
import { ExecuteSqlResponse } from '@powersync/service-types/src/routes.js';

import * as pg_utils from '../utils/pgwire_utils.js';
import * as replication_utils from '../replication/replication-utils.js';
import { baseUri, ResolvedConnectionConfig } from '../types/types.js';
import { DataSourceConfig } from '@powersync/service-types/src/config/PowerSyncConfig.js';

export class PostgresSyncAPIAdapter implements api.SyncAPI {
  // TODO manage lifecycle of this
  protected pool: pgwire.PgClient;

  constructor(protected config: ResolvedConnectionConfig) {
    this.pool = pgwire.connectPgWirePool(config, {
      idleTimeout: 30_000
    });
  }

  async getSourceConfig(): Promise<DataSourceConfig> {
    return this.config;
  }

  async getConnectionStatus(): Promise<api.ConnectionStatusResponse> {
    const base = {
      id: this.config.id,
      postgres_uri: baseUri(this.config)
    };

    try {
      await pg_utils.retriedQuery(this.pool, `SELECT 'PowerSync connection test'`);
    } catch (e) {
      return {
        ...base,
        connected: false,
        errors: [{ level: 'fatal', message: e.message }]
      };
    }

    try {
      await replication_utils.checkSourceConfiguration(this.pool);
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

  executeQuery(query: string, params: any[]): Promise<internal_routes.ExecuteSqlResponse> {
    throw new Error('Method not implemented.');
  }

  getDemoCredentials(): Promise<api.DemoCredentials> {
    throw new Error('Method not implemented.');
  }

  getDiagnostics(): Promise<{ connected: boolean; errors?: Array<{ level: string; message: string }> }> {
    throw new Error('Method not implemented.');
  }

  getDebugTablesInfo(tablePatterns: TablePattern[]): Promise<replication.PatternResult[]> {
    throw new Error('Method not implemented.');
  }

  async getReplicationLag(slotName: string): Promise<number> {
    const results = await pg_utils.retriedQuery(this.pool, {
      statement: `SELECT
  slot_name,
  confirmed_flush_lsn, 
  pg_current_wal_lsn(), 
  (pg_current_wal_lsn() - confirmed_flush_lsn) AS lsn_distance
FROM pg_replication_slots WHERE slot_name = $1 LIMIT 1;`,
      params: [{ type: 'varchar', value: slotName }]
    });
    const [row] = pgwire.pgwireRows(results);
    if (row) {
      return Number(row.lsn_distance);
    }

    throw new Error(`Could not determine replication lag for slot ${slotName}`);
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
