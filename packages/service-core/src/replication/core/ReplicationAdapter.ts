import { TablePattern } from '@powersync/service-sync-rules';
import { ReplicationEntity } from './ReplicationEntity.js';
import { SaveOptions } from '../../storage/BucketStorage.js';

/**
 *  Manage the creation and termination of connections for a datasource.
 *  For some data sources there will be no difference between snapshot and replication connections
 */
interface ConnectionManager<TConnection> {
  /**
   *  Create a connection to use for replication.
   */
  createReplicationConnection(): Promise<TConnection>;

  /**
   *  Create a connection to use for the initial snapshot replication.
   *  This connection should not be shared
   */
  createConnection(): Promise<TConnection>;

  /**
   *  Map a datasource specific connection error to a general category of powersync errors
   *  @param error
   */
  mapError(error: Error): ConnectionError;
}

/**
 * The ReplicationAdapter describes all the methods that are required by the
 * Replicator to replicate data from a datasource into the PowerSync Bucket storage
 */
export interface ReplicationAdapter<TConnection> {
  /**
   *  Unique name to identify this adapter in the PowerSync system
   *  Suggestion: datasource type + datasource name
   */
  name(): string;

  /**
   *  Return a manager that can create connections to the DataSource
   */
  createConnectionManager(): ConnectionManager<TConnection>;

  /**
   *  Validate any configuration on the datasource that needs to be in place for
   *  replication. If any configuration is missing or incorrect, an error should be thrown with the details.
   */
  validateConfiguration(connection: TConnection): void;

  /**
   * Get all the fully qualified entities that match the provided pattern
   * @param connection
   * @param pattern // TODO: Need something more generic than TablePattern
   */
  toReplicationEntities(connection: TConnection, pattern: TablePattern): Promise<ReplicationEntity<any>[]>;

  /**
   *  Start replicating data, assumes that initializeData has already finished running
   *  Stream any changes back on the provided changeListener
   *  @param connection
   *  @param changeListener
   */
  startReplication(connection: TConnection, changeListener: (change: SaveOptions) => {}): Promise<void>;

  /**
   *  Immediately interrupt and stop any currently running replication.
   */
  terminateReplication(): Promise<void>;
}

export enum ConnectionError {
  INCORRECT_CREDENTIALS = 'INCORRECT_CREDENTIALS',
  HOST_INACCESSIBLE = 'HOST_INACCESSIBLE',
  CONNECTION_REFUSED = 'CONNECTION_REFUSED',
  CONNECTION_CLOSED = 'CONNECTION_CLOSED',
  RETRIEVAL_FAILED = 'RETRIEVAL_FAILED',
  GENERAL = 'GENERAL'
}
