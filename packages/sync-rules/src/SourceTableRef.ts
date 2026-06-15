/**
 * Reference to a source table associated with a replicated row.
 */
export interface SourceTableRef {
  readonly connectionTag: string;
  readonly schema: string;
  readonly name: string;
}

export function sourceTableRefKey(table: SourceTableRef): string {
  return JSON.stringify([table.connectionTag, table.schema, table.name]);
}
