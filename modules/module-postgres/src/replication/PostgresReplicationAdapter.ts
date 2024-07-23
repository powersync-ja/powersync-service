import { replication } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as pgwire from '@powersync/service-jpgwire';

import { ResolvedConnectionConfig } from '../types/types.js';

export type PostgresConnection = pgwire.PgClient;

export class PostgresReplicationAdapter implements replication.ReplicationAdapter {
  constructor(protected config: ResolvedConnectionConfig) {}

  name(): string {
    return 'postgres';
  }

  shutdown(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  checkPrerequisites(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  toReplicationEntities(pattern: sync_rules.TablePattern): Promise<replication.ReplicationEntity[]> {
    throw new Error('Method not implemented.');
  }

  count(entity: replication.ReplicationEntity): Promise<number> {
    throw new Error('Method not implemented.');
  }

  initializeData(options: replication.InitializeDataOptions): Promise<string> {
    throw new Error('Method not implemented.');
  }

  startReplication(options: replication.StartReplicationOptions): Promise<void> {
    throw new Error('Method not implemented.');
  }
  cleanupReplication(syncRuleId: number): Promise<void> {
    throw new Error('Method not implemented.');
  }

  terminateReplication(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
