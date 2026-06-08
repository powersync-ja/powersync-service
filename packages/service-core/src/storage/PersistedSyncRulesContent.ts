import { HydratedSyncConfig, HydrationState, SyncConfigWithErrors } from '@powersync/service-sync-rules';
import { SyncRuleState } from './BucketStorage.js';

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export type PersistedSyncConfigId = string;

export interface PersistedSyncConfigStatus {
  readonly id: PersistedSyncConfigId;
  readonly replicationStreamId: number;
  readonly state: SyncRuleState;
  readonly snapshot_done?: boolean;
  readonly last_checkpoint_lsn: string | null;
  readonly last_fatal_error?: string | null;
  readonly last_fatal_error_ts?: Date | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;
}

export interface PersistedSyncRules {
  readonly id: number;
  readonly syncConfigs: SyncConfigWithErrors[];
  readonly slot_name: string;
  /**
   * For testing only.
   */
  readonly hydrationState: HydrationState;

  hydratedSyncConfig(): HydratedSyncConfig;
}
