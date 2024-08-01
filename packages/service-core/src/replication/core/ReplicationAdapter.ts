import { SqliteRow, TablePattern } from '@powersync/service-sync-rules';
import * as storage from '../../storage/storage-index.js';
import { SourceEntityDescriptor } from '../../storage/SourceEntity.js';

/**
 * The ReplicationAdapter describes all the methods that are required by the
 * Replicator to replicate data from a datasource into the PowerSync Bucket storage
 */
export interface ReplicationAdapter {
  /**
   *  Unique name to identify this adapter in the PowerSync system
   *  Suggestion: datasource type + datasource name ie. postgres-prod1
   */
  name(): string;

  /**
   *  Check that the configuration required for replication on the datasource is in place.
   *  If any configuration is missing or incorrect, an error should be thrown with the details.
   */
  checkPrerequisites(): Promise<void>;

  /**
   * Get all the fully qualified entities that match the provided pattern
   * @param pattern // TODO: Need something more generic then SourceTable
   */
  resolveReplicationEntities(pattern: TablePattern): Promise<storage.SourceTable[]>;

  /**
   *  Get the number of entries for this Entity
   *  @param entity
   */
  count(entity: storage.SourceTable): Promise<number>;

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
   *  @param syncRuleId The id of the SyncRule that was used to configure the replication
   */
  cleanupReplication(syncRuleId: number): Promise<void>;
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
  changeListener: (change: ReplicationUpdate) => {};
  abortSignal: AbortSignal;
}

export enum UpdateType {
  INSERT = 'INSERT',
  UPDATE = 'UPDATE',
  DELETE = 'DELETE',
  TRUNCATE = 'TRUNCATE',
  SCHEMA_CHANGE = 'SCHEMA_CHANGE',
  COMMIT = 'COMMIT',
  KEEP_ALIVE = 'KEEP_ALIVE'
}

export interface ReplicationUpdate {
  type: UpdateType;
  /**
   *  Descriptor of the data source entities that were updated.
   *  Usually only one entity is updated at a time, but for the truncate operation there could be multiple
   */
  entities: storage.SourceTable[];
  /**
   *  Present when the update is an insert, update or delete. Contains the changed values for adding to the bucket storage
   */
  entry?: storage.SaveOptions;
  /**
   *  Present when the update is a schema change. Describes the new data source entity
   */
  entityDescriptor?: SourceEntityDescriptor;
  /**
   *  Present when the update is a commit or a keep alive.
   */
  lsn?: string;
}
