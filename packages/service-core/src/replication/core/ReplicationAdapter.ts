import { SqliteRow, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationEntity } from './ReplicationEntity.js';
import { SaveOptions } from '../../storage/BucketStorage.js';

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
   * @param pattern // TODO: Need something more generic than TablePattern
   */
  toReplicationEntities(pattern: TablePattern): Promise<ReplicationEntity[]>;

  /**
   *  Get the number of entries for this Entity
   *  @param entity
   */
  count(entity: ReplicationEntity): Promise<number>;

  /**
   *  Retrieve the initial snapshot data for the entity. Results should be passed onto the provided recordConsumer in batches.
   *  The snapshot should happen in an isolated transaction. Returns with the LSN from when the snapshot was started, when the operation is finished.
   *  This LSN will be used as the starting point for the replication stream.
   * @param options
   */
  initializeData(options: InitializeDataOptions): Promise<string>;

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

  /**
   * Close any resources that need graceful termination.
   */
  shutdown(): Promise<void>;
}

export interface InitializeDataOptions {
  entity: ReplicationEntity;
  entry_consumer: (batch: SqliteRow[]) => {};
  abort_signal: AbortSignal;
}

export interface StartReplicationOptions {
  entities: ReplicationEntity[];
  from_lsn: string;
  change_listener: (change: SaveOptions) => {};
  abort_signal: AbortSignal;
}
