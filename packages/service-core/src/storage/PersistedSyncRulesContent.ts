import {
  CompatibilityContext,
  deserializeSyncPlan,
  HydratedSyncRules,
  javaScriptExpressionEngine,
  PrecompiledSyncConfig,
  SerializedCompatibilityContext,
  SqlSyncRules,
  SyncConfig,
  versionedHydrationState
} from '@powersync/service-sync-rules';
import { ReplicationLock } from './ReplicationLock.js';
import { UpdateSyncRulesOptions } from './BucketStorageFactory.js';

export interface ParseSyncRulesOptions {
  defaultSchema: string;
}

export abstract class PersistedSyncRulesContent {
  readonly id!: number;
  readonly sync_rules_content!: string;
  readonly sync_plan!: SerializedSyncPlan | null;
  readonly slot_name!: string;
  readonly active!: boolean;
  readonly last_checkpoint_lsn!: string | null;
  readonly last_fatal_error?: string | null | undefined;
  readonly last_fatal_error_ts?: Date | null | undefined;
  readonly last_keepalive_ts?: Date | null | undefined;
  readonly last_checkpoint_ts?: Date | null | undefined;

  constructor(data: PersistedSyncRulesContentData) {
    Object.assign(this, data);
  }

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules {
    let config: SyncConfig;
    if (this.sync_plan) {
      config = new PrecompiledSyncConfig(deserializeSyncPlan(this.sync_plan.plan), {
        engine: javaScriptExpressionEngine(CompatibilityContext.deserialize(this.sync_plan.compatibility)),
        sourceText: this.sync_rules_content
      });
    } else {
      config = SqlSyncRules.fromYaml(this.sync_rules_content, options);
    }

    return {
      id: this.id,
      slot_name: this.slot_name,
      sync_rules: config,
      hydratedSyncRules() {
        return this.sync_rules.hydrate({
          hydrationState: versionedHydrationState(this.id)
        });
      }
    };
  }

  asUpdateOptions(): UpdateSyncRulesOptions {
    return new UpdateSyncRulesOptions(this.sync_rules_content, this.sync_plan);
  }

  abstract lock(): Promise<ReplicationLock>;
}

export type PersistedSyncRulesContentData = {
  [K in keyof PersistedSyncRulesContent as PersistedSyncRulesContent[K] extends Function
    ? never
    : K]: PersistedSyncRulesContent[K];
};

export interface SerializedSyncPlan {
  plan: unknown;
  compatibility: SerializedCompatibilityContext;
}

export interface PersistedSyncRules {
  readonly id: number;
  readonly sync_rules: SyncConfig;
  readonly slot_name: string;

  hydratedSyncRules(): HydratedSyncRules;
}
