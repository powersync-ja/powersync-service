export type BenchmarkSyncEngine = 'sync-streams' | 'sync-rules';

export interface BenchmarkStreamDefinition {
  readonly name: string;
  readonly tables: readonly string[];
  readonly bucket_count: number;
}

export interface BenchmarkSyncDefinition {
  readonly engine: BenchmarkSyncEngine;
  readonly streams: readonly BenchmarkStreamDefinition[];
}
