export type BenchmarkDataPhase = 'snapshot' | 'streaming';

export interface BenchmarkTableDefinition {
  readonly key: string;
  readonly name: string;
  readonly snapshot_row_count: number;
  readonly streaming_row_count: number;
  /** UTF-8 bytes of JSON.stringify(row), including all fixed fields. */
  readonly target_source_json_bytes: number;
  readonly distribution: { readonly owner_count: number };
  readonly marker: 'last';
}

export interface BenchmarkDataDefinition {
  readonly tables: readonly BenchmarkTableDefinition[];
  readonly batch_size: number;
  readonly transaction_count: number;
}
