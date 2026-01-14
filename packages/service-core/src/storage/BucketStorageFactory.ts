import { ObserverClient } from '@powersync/lib-services-framework';
import { ParseSyncRulesOptions, PersistedSyncRules, PersistedSyncRulesContent } from './PersistedSyncRulesContent.js';
import { ReplicationEventPayload } from './ReplicationEventPayload.js';
import { ReplicationLock } from './ReplicationLock.js';
import { StartBatchOptions, SyncRulesBucketStorage } from './SyncRulesBucketStorage.js';
import { ReportStorage } from './ReportStorage.js';
import { BucketDataWriter } from './BucketStorageBatch.js';

/**
 * Represents a configured storage provider.
 *
 * The provider can handle multiple copies of sync rules concurrently, each with their own storage.
 * This is to handle replication of a new version of sync rules, while the old version is still active.
 *
 * Storage APIs for a specific copy of sync rules are provided by the `SyncRulesBucketStorage` instances.
 */
export interface BucketStorageFactory extends ObserverClient<BucketStorageFactoryListener>, AsyncDisposable {
  /**
   * Update sync rules from configuration, if changed.
   */
  configureSyncRules(
    options: UpdateSyncRulesOptions
  ): Promise<{ updated: boolean; persisted_sync_rules?: PersistedSyncRulesContent; lock?: ReplicationLock }>;

  /**
   * Get a storage instance to query sync data for specific sync rules.
   */
  getInstance(syncRules: PersistedSyncRulesContent, options?: GetIntanceOptions): SyncRulesBucketStorage;

  createCombinedWriter(storage: SyncRulesBucketStorage[], options: StartBatchOptions): Promise<BucketDataWriter>;

  /**
   * Deploy new sync rules.
   */
  updateSyncRules(options: UpdateSyncRulesOptions): Promise<PersistedSyncRulesContent>;

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
  restartReplication(sync_rules_group_id: number): Promise<void>;

  /**
   * Get the sync rules used for querying.
   */
  getActiveSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null>;

  /**
   * Get the sync rules used for querying.
   */
  getActiveSyncRulesContent(): Promise<PersistedSyncRulesContent | null>;

  /**
   * Get the sync rules that will be active next once done with initial replicatino.
   */
  getNextSyncRules(options: ParseSyncRulesOptions): Promise<PersistedSyncRules | null>;

  /**
   * Get the sync rules that will be active next once done with initial replicatino.
   */
  getNextSyncRulesContent(): Promise<PersistedSyncRulesContent | null>;

  /**
   * Get all sync rules currently replicating. Typically this is the "active" and "next" sync rules.
   */
  getReplicatingSyncRules(): Promise<PersistedSyncRulesContent[]>;

  /**
   * Get all sync rules stopped but not terminated yet.
   */
  getStoppedSyncRules(): Promise<PersistedSyncRulesContent[]>;

  /**
   * Get the active storage instance.
   */
  getActiveStorage(): Promise<SyncRulesBucketStorage | null>;

  /**
   * Get storage size of active sync rules.
   */
  getStorageMetrics(): Promise<StorageMetrics>;

  /**
   * Get the unique identifier for this instance of Powersync
   */
  getPowerSyncInstanceId(): Promise<string>;

  /**
   * Get a unique identifier for the system used for storage.
   */
  getSystemIdentifier(): Promise<BucketStorageSystemIdentifier>;
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
  content: string;
  lock?: boolean;
  validate?: boolean;
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
}
