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

// TODO: This needs to be consolidated with SourceTable into something new.
export interface SourceEntityDescriptor {
  /**
   * The internal id of the data source structure in the database.
   *
   * If undefined, the schema and name are used as the identifier.
   *
   * If specified, this is specifically used to detect renames.
   */
  objectId: number | string | undefined;
  schema: string;
  name: string;
  replicationColumns: ColumnDescriptor[];
}
