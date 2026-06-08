import { logger as defaultLogger, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import {
  CompatibilityContext,
  CompatibilityOption,
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  ErrorLocation,
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
import { Logger } from 'winston';
import { SyncRuleState } from './BucketStorage.js';
import { SerializedSyncPlan, UpdateSyncRulesOptions } from './BucketStorageFactory.js';
import { ParsedSyncConfigSet } from './ParsedSyncConfigSet.js';
import { PersistedSyncConfigStatus } from './PersistedSyncConfigStatus.js';
import { STORAGE_VERSION_CONFIG, StorageVersionConfig } from './StorageVersionConfig.js';

export interface ParsePersistedSyncConfigContentOptions {
  content: string;
  compiledPlan: SerializedSyncPlan | null;
  storageVersion: number;
  parseOptions: ParseSyncConfigOptions;
}

export function parsePersistedSyncConfigContent(options: ParsePersistedSyncConfigContentOptions): SyncConfigWithErrors {
  const { content, compiledPlan, storageVersion, parseOptions } = options;

  if (compiledPlan == null) {
    // Fallback: Only parse from YAML if no compiled plan is available.
    return SqlSyncRules.fromYaml(content, parseOptions);
  }

  const plan = deserializeSyncPlan(compiledPlan.plan);
  const compatibility = CompatibilityContext.deserialize(compiledPlan.compatibility);
  const eventDefinitions: SqlEventDescriptor[] = [];
  for (const [name, queries] of Object.entries(compiledPlan.eventDescriptors)) {
    const descriptor = new SqlEventDescriptor(name, compatibility);
    for (const query of queries) {
      descriptor.addSourceQuery(query, parseOptions);
    }

    eventDefinitions.push(descriptor);
  }

  const precompiled = new PrecompiledSyncConfig(plan, compatibility, eventDefinitions, {
    defaultSchema: parseOptions.defaultSchema,
    sourceText: content
  });

  // Note: If the original content did not define a storage version, this will still set the storage version.
  // This means asUpdateOptions will not change the storage version, even if the default changes.
  precompiled.storageVersion = storageVersion;

  const errors: YamlError[] = [];
  if (compiledPlan.errors) {
    for (const error of compiledPlan.errors) {
      const location: ErrorLocation | undefined = error.location && {
        start: error.location.start_offset,
        end: error.location.end_offset
      };
      const asYamlError = new YamlError(new Error(error.message), location);
      asYamlError.type = error.level;

      errors.push(asYamlError);
    }
  }

  return { config: precompiled, errors };
}

/**
 * Immutable sync config content for one sync config inside a replication stream.
 *
 * This represents the parsed/compiled config plus the per-config status, but
 * deliberately does NOT expose stream lifecycle concerns (locking, terminating).
 * Use {@link PersistedReplicationStream} for those.
 */

export abstract class PersistedSyncConfigContent implements PersistedSyncConfigContentData {
  readonly replicationJobId: string;
  readonly replicationStreamId: number;
  readonly sync_rules_content: string;
  readonly compiled_plan: SerializedSyncPlan | null;
  readonly replicationStreamName: string;
  readonly active: boolean;
  readonly state: SyncRuleState;
  readonly storageVersion: number;
  readonly logger: Logger;
  readonly syncConfigId: PersistedSyncConfigId | null;

  // FIXME: These are not immutable, unlike the rest of this class
  readonly last_checkpoint_lsn: string | null;

  readonly last_fatal_error?: string | null;
  readonly last_fatal_error_ts?: Date | null;
  readonly last_keepalive_ts?: Date | null;
  readonly last_checkpoint_ts?: Date | null;

  constructor(data: PersistedSyncConfigContentData) {
    Object.assign(this, data);
    this.replicationJobId = data.replicationJobId ?? String(data.replicationStreamId);
    this.replicationStreamId = data.replicationStreamId;
    this.sync_rules_content = data.sync_rules_content;
    this.compiled_plan = data.compiled_plan;
    this.replicationStreamName = data.replicationStreamName;
    this.active = data.active;
    this.state = data.state ?? (data.active ? SyncRuleState.ACTIVE : SyncRuleState.PROCESSING);
    this.storageVersion = data.storageVersion;
    this.syncConfigId = data.syncConfigId ?? null;
    this.last_checkpoint_lsn = data.last_checkpoint_lsn;
    this.logger = defaultLogger.child({ prefix: `[${this.replicationStreamName}] ` });
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
        `Unsupported storage version ${this.storageVersion} for replication stream ${this.replicationStreamId}`
      );
    }
    return storageConfig;
  }

  /**
   * Parse only this config's content into a single {@link SyncConfigWithErrors}.
   *
   * This does not depend on any other configs in the same replication stream.
   */
  protected parseSingleConfig(options: ParseSyncConfigOptions): SyncConfigWithErrors {
    return parsePersistedSyncConfigContent({
      content: this.sync_rules_content,
      compiledPlan: this.compiled_plan,
      storageVersion: this.storageVersion,
      parseOptions: options
    });
  }

  parsed(options: ParseSyncConfigOptions): ParsedSyncConfigSet {
    let hydrationState: HydrationState;
    const config = this.parseSingleConfig(options);

    const storageConfig = this.getStorageConfig();
    if (
      storageConfig.versionedBuckets ||
      config.config.compatibility.isEnabled(CompatibilityOption.versionedBucketIds)
    ) {
      hydrationState = versionedHydrationState(this.replicationStreamId);
    } else {
      hydrationState = DEFAULT_HYDRATION_STATE;
    }

    return {
      replicationStreamId: this.replicationStreamId,
      replicationStreamName: this.replicationStreamName,
      syncConfigs: [config],
      hydrationState,
      hydratedSyncConfig: () => {
        return config.config.hydrate({ hydrationState, sqlite: nodeSqlite(sqlite) });
      }
    };
  }

  asUpdateOptions(options?: Omit<UpdateSyncRulesOptions, 'config'>): UpdateSyncRulesOptions {
    // defaultSchema is not relevant for the parsed version here
    const parsed = this.parseSingleConfig({ defaultSchema: 'not_applicable' });
    return {
      config: { yaml: this.sync_rules_content, plan: this.compiled_plan, parsed },
      ...options
    };
  }

  getSyncConfigStatus(): PersistedSyncConfigStatus {
    return {
      id: this.syncConfigId ?? String(this.replicationStreamId),
      replicationStreamId: this.replicationStreamId,
      state: this.state,
      last_checkpoint_lsn: this.last_checkpoint_lsn,
      last_fatal_error: this.last_fatal_error,
      last_fatal_error_ts: this.last_fatal_error_ts,
      last_keepalive_ts: this.last_keepalive_ts,
      last_checkpoint_ts: this.last_checkpoint_ts
    };
  }
}
export interface PersistedSyncConfigContentData {
  readonly replicationStreamId: number;
  readonly sync_rules_content: string;
  readonly compiled_plan: SerializedSyncPlan | null;
  readonly replicationStreamName: string;
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
  readonly replicationJobId?: string;
}
export type PersistedSyncConfigId = string;
export interface ParseSyncConfigOptions {
  defaultSchema: string;
}
