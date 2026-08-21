import { SourceTableRef } from '@powersync/service-sync-rules';

/**
 * Source-specific JSON metadata. Storage does not interpret it. Source-table APIs use null to
 * represent the absence of metadata.
 */
export type JsonValue = string | number | boolean | null | JsonValue[] | { [key: string]: JsonValue };

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

export interface SourceEntityDescriptor extends SourceTableRef {
  /**
   * The internal id of the source entity structure in the database.
   * If undefined, the schema and name are used as the identifier.
   * If specified, this is specifically used to detect renames.
   */
  objectId: number | string | undefined;
  /**
   *  The columns that are used to uniquely identify a record in the source entity.
   */
  replicaIdColumns: ColumnDescriptor[];
  /**
   * Whether the source always sends complete row data with each operation (e.g. Postgres REPLICA
   * IDENTITY FULL). When true, no current_data copy is needed. Undefined means the source does not
   * report this, in which case we default to keeping a copy.
   */
  sendsCompleteRows?: boolean;
}
