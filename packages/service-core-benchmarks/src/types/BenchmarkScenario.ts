export type BenchmarkLayer = 'storage' | 'replication' | 'api' | 'combined';
export type BenchmarkProfile = 'quick' | 'standard' | 'manual';

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
}
