import { SourceTableRef } from '@powersync/service-sync-rules';

/**
 * Opaque JSON value used for source-specific identity metadata.
 *
 * Storage persists and hydrates these values verbatim and never interprets them.
 * Only the source module that produced a value understands its shape.
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
  schema: string;
  name: string;
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

  /**
   * Optional opaque, source-specific identity metadata discovered for this entity.
   *
   * Storage never interprets this value. The authoritative metadata persisted on new
   * records is chosen by the source reconciler (see {@link SourceTableCandidateResolution}),
   * so most sources leave this undefined and select metadata inside the reconciler instead.
   *
   * Undefined preserves legacy metadata-free behavior.
   */
  sourceMetadata?: JsonValue;
}
