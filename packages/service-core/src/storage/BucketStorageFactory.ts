import { BaseObserver, logger } from '@powersync/lib-services-framework';
import { ParseSyncRulesOptions, PersistedSyncRules, PersistedSyncRulesContent } from './PersistedSyncRulesContent.js';
import { ReplicationEventPayload } from './ReplicationEventPayload.js';
import { ReplicationLock } from './ReplicationLock.js';
import { SyncRulesBucketStorage } from './SyncRulesBucketStorage.js';
import { ReportStorage } from './ReportStorage.js';
import { SqlSyncRules, SyncConfig } from '@powersync/service-sync-rules';

/**
 * Represents a configured storage provider.
 *
 * The provider can handle multiple copies of sync rules concurrently, each with their own storage.
 * This is to handle replication of a new version of sync rules, while the old version is still active.
 *
 * Storage APIs for a specific copy of sync rules are provided by the `SyncRulesBucketStorage` instances.
 */
export abstract class BucketStorageFactory
  extends BaseObserver<BucketStorageFactoryListener>
  implements AsyncDisposable
{
  /**
   * Update sync rules from configuration, if changed.
   */
  async configureSyncRules(
    options: UpdateSyncRulesOptions
  ): Promise<{ updated: boolean; persisted_sync_rules?: PersistedSyncRulesContent; lock?: ReplicationLock }> {
    const next = await this.getNextSyncRulesContent();
    const active = await this.getActiveSyncRulesContent();

    if (next?.sync_rules_content == options.config.yaml) {
      logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else if (next == null && active?.sync_rules_content == options.config.yaml) {
      logger.info('Sync rules from configuration unchanged');
      return { updated: false };
    } else {
      logger.info('Sync rules updated from configuration');
      const persisted_sync_rules = await this.updateSyncRules(options);
      return { updated: true, persisted_sync_rules, lock: persisted_sync_rules.current_lock ?? undefined };
    }
  }

  /**
   * Get a storage instance to query sync data for specific sync rules.
   */
  abstract getInstance(syncRules: PersistedSyncRulesContent, options?: GetIntanceOptions): SyncRulesBucketStorage;

  /**
   * Deploy new sync rules.
   */
  abstract updateSyncRules(options: UpdateSyncRulesOptions): Promise<PersistedSyncRulesContent>;

  /**
   * Indicate that a slot was removed, and we should re-sync by creating
   * a new sync rules instance.
   *
   * This is roughly the same as deploying a new version of the current sync
   * rules, but also accounts for cases where the current sync rules are not
   * the latest ones.
   *
   * Replication should be restarted after this.
   */
  abstract restartReplication(sync_rules_group_id: number): Promise<void>;

  /**
   * Get the sync rules used for querying.
   */
  async getActiveSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null> {
    const content = await this.getActiveSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  /**
   * Get the sync rules used for querying.
   */
  abstract getActiveSyncRulesContent(): Promise<PersistedSyncRulesContent | null>;

  /**
   * Get the sync rules that will be active next once done with initial replicatino.
   */
  async getNextSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null> {
    const content = await this.getNextSyncRulesContent();
    return content?.parsed(options) ?? null;
  }

  /**
   * Get the sync rules that will be active next once done with initial replicatino.
   */
  abstract getNextSyncRulesContent(): Promise<PersistedSyncRulesContent | null>;

  /**
   * Get all sync rules currently replicating. Typically this is the "active" and "next" sync rules.
   */
  abstract getReplicatingSyncRules(): Promise<PersistedSyncRulesContent[]>;

  /**
   * Get all sync rules stopped but not terminated yet.
   */
  abstract getStoppedSyncRules(): Promise<PersistedSyncRulesContent[]>;

  /**
   * Get the active storage instance.
   */
  abstract getActiveStorage(): Promise<SyncRulesBucketStorage | null>;

  /**
   * Get storage size of active sync rules.
   */
  abstract getStorageMetrics(): Promise<StorageMetrics>;

  /**
   * Get the unique identifier for this instance of Powersync
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
    // TODO: Add serialized sync plan if available
  };
  lock?: boolean;
  storageVersion?: number;
}

export function updateSyncRulesFromYaml(
  content: string,
  options?: Omit<UpdateSyncRulesOptions, 'config'> & { validate: boolean }
): UpdateSyncRulesOptions {
  const { config } = SqlSyncRules.fromYaml(content, {
    // No schema-based validation at this point
    schema: undefined,
    defaultSchema: 'not_applicable', // Not needed for validation
    throwOnError: options?.validate ?? false
  });

  return updateSyncRulesFromConfig(config, options);
}

export function updateSyncRulesFromConfig(parsed: SyncConfig, options?: Omit<UpdateSyncRulesOptions, 'config'>) {
  return { config: { yaml: parsed.content }, ...options };
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
