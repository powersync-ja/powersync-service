import { logger as defaultLogger, ErrorCode, Logger, ServiceError } from '@powersync/lib-services-framework';
import {
  CompatibilityContext,
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  ErrorLocation,
  HydratedSyncConfig,
  HydrationState,
  nodeSqlite,
  PrecompiledSyncConfig,
  SqlEventDescriptor,
  SqlSyncRules,
  SyncConfigWithErrors,
  versionedHydrationState,
  YamlError
} from '@powersync/service-sync-rules';
import * as sqlite from 'node:sqlite';
import { SyncRuleState } from './BucketStorage.js';
import { SerializedSyncPlan, UpdateSyncRulesOptions } from './BucketStorageFactory.js';
import { ReplicationLock } from './ReplicationLock.js';
import { STORAGE_VERSION_CONFIG, StorageVersionConfig } from './StorageVersionConfig.js';

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export type PersistedSyncConfigId = string | number | object;

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

export interface PersistedReplicationStream {
  readonly id: number;
  readonly slot_name: string;
  readonly state: SyncRuleState;
  readonly storageVersion: number;
  readonly current_lock: ReplicationLock | null;
  readonly logger: Logger;

  getStorageConfig(): StorageVersionConfig;
  lock(): Promise<ReplicationLock>;
}

export interface PersistedSyncConfigContent {
  readonly id: number;
  readonly syncConfigId: PersistedSyncConfigId | null;
  readonly replicationStreamId: number;
  readonly sync_rules_content: string;
  readonly compiled_plan: SerializedSyncPlan | null;
  readonly storageVersion: number;
  readonly logger: Logger;

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules;
  asUpdateOptions(options?: Omit<UpdateSyncRulesOptions, 'config'>): UpdateSyncRulesOptions;
}

export interface PersistedSyncRulesContentData {
  readonly id: number;
  readonly sync_rules_content: string;
  readonly compiled_plan: SerializedSyncPlan | null;
  readonly slot_name: string;
  /**
   * True if this is the "active" copy of the sync config.
   */
  readonly active: boolean;
  readonly storageVersion: number;

  readonly last_checkpoint_lsn: string | null;

  readonly last_fatal_error?: string | null;
  readonly last_fatal_error_ts?: Date | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;
  readonly state?: SyncRuleState;
  readonly syncConfigId?: PersistedSyncConfigId | null;
}

export abstract class PersistedSyncRulesContent
  implements PersistedSyncRulesContentData, PersistedReplicationStream, PersistedSyncConfigContent
{
  readonly id!: number;
  readonly replicationStreamId!: number;
  readonly sync_rules_content!: string;
  readonly compiled_plan!: SerializedSyncPlan | null;
  readonly slot_name!: string;
  readonly active!: boolean;
  readonly state!: SyncRuleState;
  readonly storageVersion!: number;
  readonly logger: Logger;
  readonly syncConfigId!: PersistedSyncConfigId | null;

  readonly last_checkpoint_lsn!: string | null;

  readonly last_fatal_error?: string | null;
  readonly last_fatal_error_ts?: Date | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;

  abstract readonly current_lock: ReplicationLock | null;

  constructor(data: PersistedSyncRulesContentData) {
    Object.assign(this, data);
    this.replicationStreamId = data.id;
    this.state = data.state ?? (data.active ? SyncRuleState.ACTIVE : SyncRuleState.PROCESSING);
    this.syncConfigId = data.syncConfigId ?? null;
    this.logger = defaultLogger.child({ prefix: `[${this.slot_name}] ` });
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
        `Unsupported storage version ${this.storageVersion} for replication stream ${this.id}`
      );
    }
    return storageConfig;
  }

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules {
    let hydrationState: HydrationState;

    // Do we have a compiled sync plan? If so, restore from there instead of parsing everything again.
    let config: SyncConfigWithErrors;
    if (this.compiled_plan != null) {
      const plan = deserializeSyncPlan(this.compiled_plan.plan);
      const compatibility = CompatibilityContext.deserialize(this.compiled_plan.compatibility);
      const eventDefinitions: SqlEventDescriptor[] = [];
      for (const [name, queries] of Object.entries(this.compiled_plan.eventDescriptors)) {
        const descriptor = new SqlEventDescriptor(name, compatibility);
        for (const query of queries) {
          descriptor.addSourceQuery(query, options);
        }

        eventDefinitions.push(descriptor);
      }

      const precompiled = new PrecompiledSyncConfig(plan, compatibility, eventDefinitions, {
        defaultSchema: options.defaultSchema,
        sourceText: this.sync_rules_content
      });

      // Note: If the original content did not define a storage version, this will still set the storage version.
      // This means asUpdateOptions will not change the storage version, even if the default changes.
      precompiled.storageVersion = this.storageVersion;

      const errors: YamlError[] = [];
      if (this.compiled_plan.errors) {
        for (const error of this.compiled_plan.errors) {
          const location: ErrorLocation | undefined = error.location && {
            start: error.location.start_offset,
            end: error.location.end_offset
          };
          const asYamlError = new YamlError(new Error(error.message), location);
          asYamlError.type = error.level;

          errors.push(asYamlError);
        }
      }

      config = { config: precompiled, errors };
    } else {
      config = SqlSyncRules.fromYaml(this.sync_rules_content, options);
    }

    const storageConfig = this.getStorageConfig();
    if (
      storageConfig.versionedBuckets ||
      config.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)
    ) {
      hydrationState = versionedHydrationState(this.id);
    } else {
      hydrationState = DEFAULT_HYDRATION_STATE;
    }

    return {
      id: this.id,
      slot_name: this.slot_name,
      syncConfigWithErrors: config,
      hydrationState,
      hydratedSyncConfig: () => {
        return config.config.hydrate({ hydrationState, sqlite: nodeSqlite(sqlite) });
      }
    };
  }

  asUpdateOptions(options?: Omit<UpdateSyncRulesOptions, 'config'>): UpdateSyncRulesOptions {
    // defaultSchema is not relevant for the parsed version here
    const parsed = this.parsed({ defaultSchema: 'not_applicable' });
    return {
      config: { yaml: this.sync_rules_content, plan: this.compiled_plan, parsed: parsed.syncConfigWithErrors },
      ...options
    };
  }

  getSyncConfigStatus(): PersistedSyncConfigStatus {
    return {
      id: this.syncConfigId ?? this.id,
      replicationStreamId: this.replicationStreamId,
      state: this.state,
      last_checkpoint_lsn: this.last_checkpoint_lsn,
      last_fatal_error: this.last_fatal_error,
      last_fatal_error_ts: this.last_fatal_error_ts,
      last_keepalive_ts: this.last_keepalive_ts,
      last_checkpoint_ts: this.last_checkpoint_ts
    };
  }

  abstract lock(): Promise<ReplicationLock>;
}

export interface PersistedSyncRules {
  readonly id: number;
  /**
   * @deprecated Will be updated to support multiple SyncConfigs.
   */
  readonly syncConfigWithErrors: SyncConfigWithErrors;
  readonly slot_name: string;
  /**
   * For testing only.
   */
  readonly hydrationState: HydrationState;

  hydratedSyncConfig(): HydratedSyncConfig;
}
