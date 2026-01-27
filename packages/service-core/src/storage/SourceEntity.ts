export interface ColumnDescriptor {
  name: string;
  /**
   *  The type of the column ie VARCHAR, INT, etc
   */
  type?: string;
  /**
   *  Some data sources have a type id that can be used to identify the type of the column
   */
  typeId?: number;
}

export interface SourceEntityDescriptor {
  /**
   * The internal id of the source entity structure in the database.
   * If undefined, the schema and name are used as the identifier.
   * If specified, this is specifically used to detect renames.
   */
  objectId: number | string | undefined;
  schema: string;
  name: string;
  /**
   *  The columns that are used to uniquely identify a record in the source entity.
   */
  replicaIdColumns: ColumnDescriptor[];
  /**
   * The replica identity type for this entity.
   * Only applicable for Postgres sources.
   * 'full' means complete row data is always sent with operations.
   */
  replicationIdentity?: 'default' | 'nothing' | 'full' | 'index';
}
