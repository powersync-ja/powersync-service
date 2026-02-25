import {
  CompatibilityContext,
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  HydratedSyncRules,
  HydrationState,
  javaScriptExpressionEngine,
  PrecompiledSyncConfig,
  SqlEventDescriptor,
  SqlSyncRules,
  SyncConfigWithErrors,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { ReplicationLock } from './ReplicationLock.js';
import { STORAGE_VERSION_CONFIG, StorageVersionConfig } from './StorageVersionConfig.js';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { SerializedSyncPlan, UpdateSyncRulesOptions } from './BucketStorageFactory.js';

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export interface PersistedSyncRulesContentData {
  readonly id: number;
  readonly sync_rules_content: string;
  readonly compiled_plan: SerializedSyncPlan | null;
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
  readonly compiled_plan!: SerializedSyncPlan | null;
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
        engine: javaScriptExpressionEngine(compatibility),
        sourceText: this.sync_rules_content
      });

      config = { config: precompiled, errors: [] };
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
      sync_rules: config,
      hydratedSyncRules: () => {
        return config.config.hydrate({ hydrationState });
      }
    };
  }

  asUpdateOptions(options?: Omit<UpdateSyncRulesOptions, 'config'>): UpdateSyncRulesOptions {
    return {
      config: { yaml: this.sync_rules_content, plan: this.compiled_plan },
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
