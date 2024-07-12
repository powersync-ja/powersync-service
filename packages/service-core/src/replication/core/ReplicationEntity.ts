import { SourceTable } from '../../storage/SourceTable.js';
import { SqliteRow } from '@powersync/service-sync-rules';

/**
 *  Describes a replication entity, which is a logical representation of a table or collection in a data source.
 *  Extend this interface to add additional properties specific entities in the data source.
 */
export interface ReplicationEntityDescriptor {
  name: string;

  /**
   *  Fully qualified name of the entity, including any schema/namespace prefixes
   */
  fullyQualifiedName: string;

  /**
   *  Identifier to uniquely identify the entity in PowerSync
   */
  id: string;
  /**
   *  The entity's native identifier in the data source
   */
  dataSourceEntityId: string;
  /**
   *  The field(s) that uniquely identify an entry/row in this replication entity
   */
  primaryIdentifierFields: string[];
}

export abstract class ReplicationEntity<T extends ReplicationEntityDescriptor> {
  public descriptor: T;

  /**
   * Defaults to true for tests.
   */
  public syncData: boolean = true;

  /**
   * Defaults to true for tests.
   */
  public syncParameters: boolean = true;

  /**
   *  Indicates whether the snapshot of the entity has already been completed
   */
  public snapshotComplete: boolean = false;

  constructor(descriptor: T) {
    this.descriptor = descriptor;
  }

  public hasPrimaryIdentifierFields(): boolean {
    return this.descriptor.primaryIdentifierFields.length > 0;
  }

  public syncAny() {
    return this.syncData || this.syncParameters;
  }

  /**
    *  Get the number of entries for this Entity
    *  @param connection
   */
  public abstract count<TConnection>(connection: TConnection): Promise<number>;

  /**
   *  Retrieve the initial snapshot data for the entity. Results should be passed onto the provided recordConsumer in batches.
   *  The snapshot should happen in an isolated transaction. Returns with the LSN from when the snapshot was started, when the operation is finished.
   *  This LSN will be used as the starting point for the replication stream.
   *  @param connection
   *  @param entryConsumer
   */
  public abstract getSnapshot<TConnection>(connection: TConnection, entryConsumer: (batch: SqliteRow[]) => {}): Promise<string>;
}
