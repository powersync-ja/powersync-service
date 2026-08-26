export type BenchmarkLayer = 'storage' | 'replication' | 'api' | 'combined';
export type BenchmarkProfile = 'quick' | 'standard' | 'manual';

export interface BenchmarkSourceTable {
  readonly schema: string;
  readonly table: string;
}

export interface BenchmarkScenario<Workload extends object = {}> {
  id: string;
  description: string;
  layer: BenchmarkLayer;
  profile: BenchmarkProfile;
  tags: string[];
  prerequisites: string[];
  timeout_ms: number;
  warmup_iterations: number;
  measured_iterations: number;
  workload: Workload;
  syncRule(source: BenchmarkSourceTable): string;
  sync_parameters: Record<string, unknown>;
  expected_bucket_count: number;
  expected_bucket_operation_count: number;
}
