import { SqlSyncRules } from '@powersync/service-sync-rules';
import { ReplicationLock } from './ReplicationLock.js';

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export interface PersistedSyncRulesContent {
  readonly id: number;
  readonly sync_rules_content: string;
  readonly slot_name: string;

  readonly last_fatal_error?: string | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules;

  lock(): Promise<ReplicationLock>;
}

export interface PersistedSyncRules {
  readonly id: number;
  readonly sync_rules: SqlSyncRules;
  readonly slot_name: string;
}
