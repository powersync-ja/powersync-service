import {
  CompatibilityContext,
  deserializeSyncPlan,
  HydratedSyncRules,
  javaScriptExpressionEngine,
  PrecompiledSyncConfig,
  SerializedCompatibilityContext,
  SqlSyncRules,
  SyncConfigWithErrors,
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

  protected loadSyncConfig(options: ParseSyncRulesOptions): SyncConfigWithErrors {
    if (this.sync_plan) {
      const loaded = new PrecompiledSyncConfig(deserializeSyncPlan(this.sync_plan.plan), {
        engine: javaScriptExpressionEngine(CompatibilityContext.deserialize(this.sync_plan.compatibility)),
        sourceText: this.sync_rules_content,
        defaultSchema: options.defaultSchema
      });
      // We can't report errors for pre-compiled sync plans
      return { config: loaded, errors: [] };
    } else {
      return SqlSyncRules.fromYaml(this.sync_rules_content, options);
    }
  }

  parsed(options: ParseSyncRulesOptions): PersistedSyncRules {
    const config = this.loadSyncConfig(options);

    return {
      id: this.id,
      slot_name: this.slot_name,
      sync_rules: config,
      hydratedSyncRules() {
        return this.sync_rules.config.hydrate({
          hydrationState: versionedHydrationState(this.id)
        });
      },
      asUpdateOptions: (options: { lock: boolean } = { lock: false }): UpdateSyncRulesOptions => {
        return this.asUpdateOptions(options);
      }
    };
  }

  asUpdateOptions(options: { lock: boolean } = { lock: false }): UpdateSyncRulesOptions {
    return {
      content: this.sync_rules_content,
      compiledSyncPlan: this.sync_plan ?? undefined,
      lock: options.lock
    };
  }

  abstract lock(): Promise<ReplicationLock>;
}

// Fields in PersistedSyncRulesContent
export type PersistedSyncRulesContentData = {
  [K in keyof PersistedSyncRulesContent as PersistedSyncRulesContent[K] extends Function
    ? never
    : K]: PersistedSyncRulesContent[K];
};

export interface PersistedSyncRules {
  readonly id: number;
  readonly sync_rules: SyncConfigWithErrors;
  readonly slot_name: string;

  hydratedSyncRules(): HydratedSyncRules;
  asUpdateOptions(options?: { lock: boolean }): UpdateSyncRulesOptions;
}

export interface SerializedSyncPlan {
  plan: unknown;
  compatibility: SerializedCompatibilityContext;
}
