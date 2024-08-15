import { SqliteRow, TablePattern } from '@powersync/service-sync-rules';
import * as storage from '../../storage/storage-index.js';
import { SourceEntityDescriptor } from '../../storage/SourceEntity.js';

/**
 *  Any additional configuration required for the ReplicationAdapters that is only available
 *  at runtime.
 */
export interface RuntimeConfiguration {
  /**
   *  The unique identifier for the revision of the SyncRules that will be used for replication
   */
  syncRuleId: string;
}

export interface ReplicationAdapterFactory {
  /**
   *  Unique name to identify the adapters in the PowerSync system for this datasource
   *  Suggestion: datasource type + datasource name ie. postgres-prod1
   */
  name: string;
  create(configuration: RuntimeConfiguration): ReplicationAdapter;
}

/**
 * The ReplicationAdapter describes all the methods that are required by the
 * PluggableReplicator to replicate data from a datasource into the PowerSync Bucket storage
 */
export interface ReplicationAdapter {
  /**
   *  Unique name for the adapter, will usually be provided by the ReplicationAdapterFactory implementation,
   *  but can be overridden if necessary.
   *  When provided, name takes the form of ReplicationAdapterFactory.name + sync rule identifier.
   */
  name: string;

  /**
   *  Confirm that the required configuration for replication on the datasource is in place.
   *  If it isn't, attempt to create it.
   *  If any configuration is missing or could not be created, an error should be thrown with the details.
   */
  ensureConfiguration(options: EnsureConfigurationOptions): Promise<void>;

  /**
   * Get all the fully qualified entities that match the provided pattern
   * @param pattern // TODO: Need something more generic than TablePattern
   */
  resolveReplicationEntities(pattern: TablePattern): Promise<storage.SourceEntityDescriptor[]>;

  /**
   *  Get the number of entries for this Entity.
   *  Return -1n if unknown.
   *
   *  @param entity
   */
  count(entity: storage.SourceTable): Promise<bigint>;

  /**
   *  Retrieve the initial snapshot data for the entity. Results should be passed onto the provided entryConsumer in batches.
   *  The snapshot should happen in an isolated transaction. Returns with the LSN from when the snapshot was started, when the operation is finished.
   *  This LSN will be used as the starting point for the replication stream.
   * @param options
   */
  initializeData(options: InitializeDataOptions): Promise<void>;

  /**
   *  Start replicating data, assumes that initializeData has already finished running
   *  Stream any changes back on the provided changeListener
   *  @param options
   */
  startReplication(options: StartReplicationOptions): Promise<void>;

  /**
   *  Clean up any configuration or state for the replication with the given identifier on the datasource.
   *  This assumes that the replication is not currently active.
   */
  cleanupReplication(): Promise<void>;

  /**
   *  Return the LSN descriptor for this data source.
   */
  lsnDescriptor(): LSNDescriptor;
}

export interface EnsureConfigurationOptions {
  abortSignal: AbortSignal;
}

export interface InitializeDataBatch {
  entries: SqliteRow[];
  entity: storage.SourceTable;
  isLast: boolean;
  fromLSN: string;
}

export interface InitializeDataOptions {
  entities: storage.SourceTable[];
  entryConsumer: (batch: InitializeDataBatch) => {};
  abortSignal: AbortSignal;
}

export interface StartReplicationOptions {
  entities: storage.SourceTable[];
  changeListener: (change: ReplicationMessage) => {};
  abortSignal: AbortSignal;
}

export enum ReplicationMessageType {
  CRUD = 'CRUD',
  TRUNCATE = 'TRUNCATE',
  SCHEMA_CHANGE = 'SCHEMA_CHANGE',
  COMMIT = 'COMMIT',
  KEEP_ALIVE = 'KEEP_ALIVE'
}

export interface ReplicationMessage {
  type: ReplicationMessageType;
  payload: CrudOperation | SchemaUpdate | TruncateRequest | LSNUpdate;
}

export type CrudOperation = {
  /**
   * The entity for which the change occurred
   */
  entity: storage.SourceTable;
  /**
   *  Description of the change that happened to the entry
   */
  entry: storage.SaveOptions;
};

export type SchemaUpdate = {
  /**
   *  Describes the new schema
   */
  entityDescriptor: SourceEntityDescriptor;
  // TODO: This shouldn't have to be here
  connectionTag: string;
};

export type TruncateRequest = {
  /**
   *  The entities that should be truncated
   */
  entities: storage.SourceTable[];
};

export type LSNUpdate = {
  lsn: string;
};

/**
 *  Describes the LSN format for a data source, including a way to compare them.
 */
export interface LSNDescriptor {
  /**
   *  The zero LSN for this data source where transactions start from.
   */
  zeroLsn: string;
  /**
   *  Compare two LSNs in this data source's format and return a number indicating their order.
   *  @param lsnA
   *  @param lsnB
   */
  comparator: (lsnA: string, lsnB: string) => number;
}
