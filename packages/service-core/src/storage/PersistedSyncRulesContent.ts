import { HydratedSyncRules, SyncConfig, SyncConfigWithErrors } from '@powersync/service-sync-rules';
import { ReplicationLock } from './ReplicationLock.js';
import { StorageVersionConfig } from './StorageVersionConfig.js';

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export interface PersistedSyncRulesContent {
  readonly id: number;
  readonly sync_rules_content: string;
  readonly slot_name: string;
  /**
   * True if this is the "active" copy of the sync rules.
   */
  readonly active: boolean;

  readonly storageVersion: number;

  readonly last_checkpoint_lsn: string | null;

  readonly last_fatal_error?: string | null;
  readonly last_fatal_error_ts?: Date | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules;

  lock(): Promise<ReplicationLock>;
  getStorageConfig(): StorageVersionConfig;
}

export interface PersistedSyncRules {
  readonly id: number;
  readonly sync_rules: SyncConfigWithErrors;
  readonly slot_name: string;

  hydratedSyncRules(): HydratedSyncRules;
}
