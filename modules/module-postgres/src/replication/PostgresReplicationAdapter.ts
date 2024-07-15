import { replication, storage } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';

import { PostgresConnection, PostgresConnectionManager } from '../connection/PostgresConnectionManager.js';
import {
  normalizeConnectionConfig,
  NormalizedPostgresConnectionConfig,
  PostgresConnectionConfig
} from '../types/types.js';

export class PostgresReplicationAdapter implements replication.ReplicationAdapter<PostgresConnection> {
  protected normalizedConfig: NormalizedPostgresConnectionConfig;

  constructor(config: PostgresConnectionConfig) {
    this.normalizedConfig = normalizeConnectionConfig(config);
  }

  name(): string {
    return 'postgres';
  }

  createConnectionManager(): PostgresConnectionManager {
    return new PostgresConnectionManager(this.normalizedConfig);
  }

  validateConfiguration(connection: PostgresConnection): void {
    throw new Error('Method not implemented.');
  }

  toReplicationEntities(
    connection: PostgresConnection,
    pattern: sync_rules.TablePattern
  ): Promise<replication.ReplicationEntity<any>[]> {
    throw new Error('Method not implemented.');
  }

  startReplication(connection: PostgresConnection, changeListener: (change: storage.SaveOptions) => {}): Promise<void> {
    throw new Error('Method not implemented.');
  }

  terminateReplication(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
