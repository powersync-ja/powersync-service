import { SourceTableRef } from '@powersync/service-sync-rules';

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
   * report this, in which case any previously-resolved value is preserved.
   */
  sendsCompleteRows?: boolean;
}

/** The per-table `store_current_data` decision derived by {@link resolveStoreCurrentData}. */
export interface ResolvedStoreCurrentData {
  /** Effective value for the in-memory {@link SourceTable}. */
  storeCurrentData: boolean;
  /** Value to persist on insert: null when unreported (MongoDB should coerce to undefined). */
  persistedValue: boolean | null;
  /** The resolved flag (`!sendsCompleteRows`); the new value to write when {@link changed}. */
  value: boolean;
  /** Whether the resolved flag differs from the persisted value and should be updated. */
  changed: boolean;
}

/**
 * Derives the per-table `store_current_data` flag from {@link SourceEntityDescriptor.sendsCompleteRows}
 * and the value already persisted, so the storage backends don't each re-derive the three-state logic:
 * reported true → false (no copy), reported false → true (keep a copy), unreported → keep persisted.
 */
export function resolveStoreCurrentData(
  sendsCompleteRows: boolean | undefined,
  persisted?: boolean | null
): ResolvedStoreCurrentData {
  const reported = sendsCompleteRows != null;
  const value = sendsCompleteRows !== true;
  const existing = persisted ?? true;
  return {
    storeCurrentData: reported ? value : existing,
    persistedValue: reported ? value : null,
    value,
    changed: reported && existing !== value
  };
}
