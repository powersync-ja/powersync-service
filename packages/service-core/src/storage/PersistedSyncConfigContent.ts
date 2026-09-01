import { logger as defaultLogger, ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import {
  CompatibilityContext,
  CompatibilityOption,
  compileEventDefinitions,
  DEFAULT_HYDRATION_STATE,
  deserializeSyncPlan,
  ErrorLocation,
  HydratedSyncConfig,
  HydrationState,
  nodeSqlite,
  PrecompiledSyncConfig,
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
  const errors: YamlError[] = [];
  // Compiled events are additive to plan versions 1 and 2. New readers prefer them when present; when an older plan
  // does not contain them, normalize the dual-written raw SQL at this loading boundary. This keeps legacy event
  // evaluators out of PrecompiledSyncConfig while older binaries can continue reading the same persisted config.
  if (compiledPlan.plan.events == null) {
    const normalized = compileEventDefinitions(compiledPlan.eventDescriptors, {
      ...parseOptions,
      // The legacy evaluator ignored event payload filters. Preserve that behavior for plans deployed before compiled
      // events existed; a redeploy compiles and validates those filters before the replacement config is activated.
      compileEventPayloadFilters: false
    });
    const fatalErrors = normalized.errors.filter((error) => error.type == 'fatal');
    if (fatalErrors.length != 0) {
      throw new Error(
        `Failed to compile persisted replication events: ${fatalErrors.map((error) => error.message).join(', ')}`
      );
    }
    plan.events = normalized.events;
    errors.push(...normalized.errors.map((error) => new YamlError(error)));
  }

  const precompiled = new PrecompiledSyncConfig(plan, compatibility, {
    defaultSchema: parseOptions.defaultSchema,
    sourceText: content
  });

  // Note: If the original content did not define a storage version, this will still set the storage version.
  // This means asUpdateOptions will not change the storage version, even if the default changes.
  precompiled.storageVersion = storageVersion;

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
 * This represents the persisted config content. Fetch per-config status with
 * {@link getSyncConfigStatus}. Use {@link PersistedReplicationStream} for stream
 * lifecycle concerns such as locking and termination.
 */

export abstract class PersistedSyncConfigContent implements PersistedSyncConfigContentData {
  readonly replicationStreamId: number;
  readonly sync_rules_content: string;
  readonly compiled_plan: SerializedSyncPlan | null;
  readonly replicationStreamName: string;
  readonly storageVersion: number;
  readonly logger: Logger;
  readonly syncConfigId: PersistedSyncConfigId | null;
  readonly syncConfigState: SyncRuleState;
  readonly version_label: string | undefined;

  constructor(data: PersistedSyncConfigContentData) {
    this.replicationStreamId = data.replicationStreamId;
    this.sync_rules_content = data.sync_rules_content;
    this.compiled_plan = data.compiled_plan;
    this.replicationStreamName = data.replicationStreamName;
    this.storageVersion = data.storageVersion;
    this.syncConfigId = data.syncConfigId ?? null;
    this.syncConfigState = data.syncConfigState;
    this.version_label = data.version_label;
    const versionPrefix = this.version_label == null ? '' : `[${this.version_label}]`;
    this.logger = defaultLogger.child({ prefix: `[${this.replicationStreamName}]${versionPrefix} ` });
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

    let hydrated: HydratedSyncConfig | undefined;
    return {
      replicationStreamId: this.replicationStreamId,
      replicationStreamName: this.replicationStreamName,
      syncConfigs: [config],
      hydrationState,
      get hydratedSyncConfig(): HydratedSyncConfig {
        hydrated ??= config.config.hydrate({ hydrationState, sqlite: nodeSqlite(sqlite) });
        return hydrated;
      }
    };
  }

  asUpdateOptions(options?: Omit<UpdateSyncRulesOptions, 'config'>): UpdateSyncRulesOptions {
    // defaultSchema is not relevant for the parsed version here
    const parsed = this.parseSingleConfig({ defaultSchema: 'not_applicable' });
    return {
      config: { yaml: this.sync_rules_content, plan: this.compiled_plan, parsed },
      version_label: this.version_label,
      ...options
    };
  }

  /**
   * Fetch the current persisted state for this exact sync config.
   */
  abstract getSyncConfigStatus(): Promise<PersistedSyncConfigStatus | null>;
}
export interface PersistedSyncConfigContentData {
  readonly replicationStreamId: number;
  readonly sync_rules_content: string;
  readonly compiled_plan: SerializedSyncPlan | null;
  readonly replicationStreamName: string;
  readonly storageVersion: number;

  readonly syncConfigId?: PersistedSyncConfigId | null;
  readonly syncConfigState: SyncRuleState;
  readonly version_label?: string;
}
export type PersistedSyncConfigId = string;
export interface ParseSyncConfigOptions {
  defaultSchema: string;
}
