/**
 * Reference to a source table associated with a replicated row.
 */
export interface SourceTableRef {
  readonly connectionTag: string;
  readonly schema: string;
  readonly name: string;
}
