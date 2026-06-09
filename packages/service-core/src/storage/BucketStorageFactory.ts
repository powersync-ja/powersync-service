import { BaseObserver, logger } from '@powersync/lib-services-framework';
import {
  PrecompiledSyncConfig,
  SerializedCompatibilityContext,
  SerializedSyncPlanV1,
  serializeSyncPlan,
  SqlSyncRules,
  SyncConfigWithErrors
} from '@powersync/service-sync-rules';
import { ReplicationError } from '@powersync/service-types';
import { syncConfigYamlErrorToReplicationError } from '../util/errors.js';
import { PersistedReplicationStream } from './PersistedReplicationStream.js';
import { PersistedSyncConfigContent } from './PersistedSyncConfigContent.js';
import { ReplicationEventPayload } from './ReplicationEventPayload.js';
import { ReplicationLock } from './ReplicationLock.js';
import { ReportStorage } from './ReportStorage.js';
import { SyncRulesBucketStorage } from './SyncRulesBucketStorage.js';

/**
 * Represents a configured storage provider.
 *
 * The provider can handle multiple replication streams concurrently, each with their own storage.
 * This is to handle replication of a new version of sync config, while the old replication stream is still active.
 *
 * Storage APIs for a specific replication stream are provided by the `SyncRulesBucketStorage` instances.
 */
export abstract class BucketStorageFactory
  extends BaseObserver<BucketStorageFactoryListener>
  implements AsyncDisposable
{
  /**
   * Update sync config from configuration, if changed.
   */
  async configureSyncRules(
    options: UpdateSyncRulesOptions
  ): Promise<{ updated: boolean; persisted_sync_rules?: PersistedReplicationStream; lock?: ReplicationLock }> {
    const deploying = await this.getDeployingSyncConfigContent();
    const active = await this.getActiveSyncConfigContent();

    if (deploying?.sync_rules_content == options.config.yaml) {
      logger.info('Sync config unchanged');
      return { updated: false };
    } else if (deploying == null && active?.sync_rules_content == options.config.yaml) {
      logger.info('Sync config unchanged');
      return { updated: false };
    } else {
      logger.info('Sync config updated');
      const persisted_sync_rules = await this.updateSyncRules(options);
      return { updated: true, persisted_sync_rules, lock: persisted_sync_rules.current_lock ?? undefined };
    }
  }

  /**
   * Get a storage instance to query sync data for specific sync config.
   */
  abstract getInstance(
    replicationStream: PersistedReplicationStream,
    options?: GetIntanceOptions
  ): SyncRulesBucketStorage;

  /**
   * Deploy new sync config.
   */
  abstract updateSyncRules(options: UpdateSyncRulesOptions): Promise<PersistedReplicationStream>;

  /**
   * Indicate that a slot was removed, and we should re-sync by creating
   * a new replication stream.
   *
   * This is roughly the same as deploying a new version of the current sync
   * config, but also accounts for cases where the current sync config is not
   * the latest one.
   *
   * Replication should be restarted after this.
   */
  abstract restartReplication(replicationStreamId: number): Promise<void>;

  /**
   * Get the sync config used for querying.
   */
  abstract getActiveSyncConfigContent(): Promise<PersistedSyncConfigContent | null>;

  /**
   * Get the sync config that is still deploying.
   */
  abstract getDeployingSyncConfigContent(): Promise<PersistedSyncConfigContent | null>;

  /**
   * Get a replication stream by id, regardless of state.
   *
   * This is the canonical way to obtain a {@link PersistedReplicationStream} for use with
   * {@link getInstance} when starting from a {@link PersistedSyncConfigContent}.
   */
  abstract getReplicationStream(replicationStreamId: number): Promise<PersistedReplicationStream | null>;

  /**
   * Get all replication streams currently replicating.
   */
  abstract getReplicatingReplicationStreams(): Promise<PersistedReplicationStream[]>;

  /**
   * Get all replication streams stopped but not terminated yet.
   */
  abstract getStoppedReplicationStreams(): Promise<PersistedReplicationStream[]>;

  /**
   * Get the active storage instance.
   */
  abstract getActiveStorage(): Promise<SyncRulesBucketStorage | null>;

  /**
   * Get storage size of active replication stream.
   */
  abstract getStorageMetrics(): Promise<StorageMetrics>;

  /**
   * Get the unique identifier for this instance of Powersync.
   */
  abstract getPowerSyncInstanceId(): Promise<string>;

  /**
   * Get a unique identifier for the system used for storage.
   */
  abstract getSystemIdentifier(): Promise<BucketStorageSystemIdentifier>;

  abstract [Symbol.asyncDispose](): PromiseLike<void>;
}

export interface BucketStorageFactoryListener {
  syncStorageCreated: (storage: SyncRulesBucketStorage) => void;
  replicationEvent: (event: ReplicationEventPayload) => void;
}

export interface StorageMetrics {
  /**
   * Size of operations (bucket_data)
   */
  operations_size_bytes: number;

  /**
   * Size of parameter storage.
   *
   * Replication storage -> raw data as received from Postgres.
   */
  parameters_size_bytes: number;

  /**
   * Size of current_data.
   */
  replication_size_bytes: number;
}

export interface UpdateSyncRulesOptions {
  config: {
    yaml: string;
    /**
     * The serialized sync plan for the sync configuration, or `null` for configurations not using the sync stream
     * compiler.
     */
    plan: SerializedSyncPlan | null;

    /**
     * Parsed sync config, primarily to generate a definition mapping.
     * Not persisted, and the defaultSchema used for parsing is not relevant.
     */
    parsed: SyncConfigWithErrors;
  };
  lock?: boolean;
  storageVersion?: number;

  /**
   * Only relevant if the result is used. This does not affect the persisted config.
   */
  defaultSchema?: string;
}

export interface SerializedSyncPlan {
  /**
   * The serialized plan, from {@link serializeSyncPlan}.
   */
  plan: SerializedSyncPlanV1;
  compatibility: SerializedCompatibilityContext;
  /**
   * Event descriptors are not currently represented in the sync plan because they don't use the sync streams compiler
   * yet.
   *
   * We might revisit that in the future, but for now we store SQL text of their definitions here to be able to restore
   * them.
   */
  eventDescriptors: Record<string, string[]>;
  errors?: ReplicationError[];
}

export function updateSyncRulesFromYaml(
  content: string,
  options?: Omit<UpdateSyncRulesOptions, 'config'> & { validate?: boolean }
): UpdateSyncRulesOptions {
  const config = SqlSyncRules.fromYaml(content, {
    // No schema-based validation at this point
    schema: undefined,
    defaultSchema: options?.defaultSchema ?? 'not_applicable', // Not needed for validation
    throwOnError: options?.validate ?? false
  });

  return updateSyncRulesFromConfig(config, options);
}

export function updateSyncRulesFromConfig(
  parsed: SyncConfigWithErrors,
  options?: Omit<UpdateSyncRulesOptions, 'config'>
): UpdateSyncRulesOptions {
  let plan: SerializedSyncPlan | null = null;
  const { config, errors } = parsed;
  if (config instanceof PrecompiledSyncConfig) {
    const eventDescriptors: Record<string, string[]> = {};
    for (const event of config.eventDescriptors) {
      eventDescriptors[event.name] = event.sourceQueries.map((q) => q.sql);
    }

    plan = {
      compatibility: config.compatibility.serialize(),
      plan: serializeSyncPlan(config.plan),
      eventDescriptors,
      errors: errors.map((e) => syncConfigYamlErrorToReplicationError(e))
    };
  }

  return { config: { yaml: config.content, plan, parsed }, ...options };
}

export interface GetIntanceOptions {
  /**
   * Set to true to skip trigger any events for creating the instance.
   *
   * This is used when creating the instance only for clearing data.
   *
   * When this is used, note that some functionality such as write checkpoint mode
   * may not be configured correctly.
   */
  skipLifecycleHooks?: boolean;
}

export interface BucketStorageSystemIdentifier {
  /**
   * A unique identifier for the system used for storage.
   * For Postgres this can be the cluster `system_identifier` and database name.
   * For MongoDB this can be the replica set name.
   */
  id: string;
  /**
   * A unique type for the storage implementation.
   * e.g. `mongodb`, `postgresql`.
   */
  type: string;
}

/**
 * Helper for tests.
 * This is not in the `service-core-tests` package in order for storage modules
 * to provide relevant factories without requiring `service-core-tests` as a direct dependency.
 */
export interface TestStorageOptions {
  /**
   * By default, collections are only cleared/
   * Setting this to true will drop the collections completely.
   */
  dropAll?: boolean;

  doNotClear?: boolean;
}
export type TestStorageFactory = (options?: TestStorageOptions) => Promise<BucketStorageFactory>;
export type TestReportStorageFactory = (options?: TestStorageOptions) => Promise<ReportStorage>;

export interface TestStorageConfig {
  factory: TestStorageFactory;
  tableIdStrings: boolean;
  storageVersion?: number;
}
