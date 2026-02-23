import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import {
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  HydratedSyncRules,
  HydrationState,
  SqlSyncRules,
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { UpdateSyncRulesOptions } from './BucketStorageFactory.js';
import { ReplicationLock } from './ReplicationLock.js';
import { STORAGE_VERSION_CONFIG, StorageVersionConfig } from './StorageVersionConfig.js';

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export interface PersistedSyncRulesContentData {
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
}

export abstract class PersistedSyncRulesContent implements PersistedSyncRulesContentData {
  readonly id!: number;
  readonly sync_rules_content!: string;
  readonly slot_name!: string;
  readonly active!: boolean;
  readonly storageVersion!: number;

  readonly last_checkpoint_lsn!: string | null;

  readonly last_fatal_error?: string | null;
  readonly last_fatal_error_ts?: Date | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;

  abstract readonly current_lock: ReplicationLock | null;

  constructor(data: PersistedSyncRulesContentData) {
    Object.assign(this, data);
  }

  /**
   * Load the storage config.
   *
   * This may throw if the persisted storage version is not supported.
   */
  getStorageConfig(): StorageVersionConfig {
    const storageConfig = STORAGE_VERSION_CONFIG[this.storageVersion];
    if (storageConfig == null) {
      throw new ServiceError(
        ErrorCode.PSYNC_S1005,
        `Unsupported storage version ${this.storageVersion} for sync rules ${this.id}`
      );
    }
    return storageConfig;
  }

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules {
    let hydrationState: HydrationState;
    const syncRules = SqlSyncRules.fromYaml(this.sync_rules_content, options);
    const storageConfig = this.getStorageConfig();
    if (
      storageConfig.versionedBuckets ||
      syncRules.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)
    ) {
      hydrationState = versionedHydrationState(this.id);
    } else {
      hydrationState = DEFAULT_HYDRATION_STATE;
    }

    return {
      id: this.id,
      slot_name: this.slot_name,
      sync_rules: syncRules,
      hydratedSyncRules: () => {
        return syncRules.config.hydrate({ hydrationState });
      }
    };
  }

  asUpdateOptions(options?: Omit<UpdateSyncRulesOptions, 'config'>): UpdateSyncRulesOptions {
    return {
      config: { yaml: this.sync_rules_content },
      ...options
    };
  }

  abstract lock(): Promise<ReplicationLock>;
}

export interface PersistedSyncRules {
  readonly id: number;
  readonly sync_rules: SyncConfigWithErrors;
  readonly slot_name: string;

  hydratedSyncRules(): HydratedSyncRules;
}
