import { SqliteRow } from '@powersync/service-sync-rules';

/**
 *  Describes a replication entity, which is a logical representation of a table or collection in a data source.
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

  // Add any additional properties specific to the entity here
  additionalProperties: Record<string, string>;
}

export class ReplicationEntity {
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

  public snapshotLSN: string | null = null;

  constructor(public descriptor: ReplicationEntityDescriptor) {}

  public hasPrimaryIdentifierFields(): boolean {
    return this.descriptor.primaryIdentifierFields.length > 0;
  }

  public syncAny() {
    return this.syncData || this.syncParameters;
  }
}
