export interface ColumnDescriptor {
  name: string;
  /**
   *  The type of the column ie VARCHAR, INT, etc
   */
  type: string;
  /**
   *  Some data sources have a type id that can be used to identify the type of the column
   */
  typeId?: number;
}

// TODO: This needs to be consolidate with SourceTable into something new.
export interface SourceEntityDescriptor {
  /**
   *  The internal id of the data source structure in the database
   */
  objectId: number | string;
  schema: string;
  name: string;
  replicationColumns: ColumnDescriptor[];
}
