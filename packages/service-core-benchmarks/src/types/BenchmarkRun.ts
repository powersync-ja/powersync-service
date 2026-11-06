import { BenchmarkResult } from './BenchmarkResult.js';

export type HostMetadata = object;

export interface BenchmarkRunOutput {
  schema_version: 1;
  generated_at: string;
  run: BenchmarkRunMetadata;
  results: BenchmarkResult[];
}

export interface BenchmarkRunMetadata {
  run_id: string;
  git_sha: string | null;
  git_dirty: boolean | null;
  node_version: string;
  pnpm_version: string | null;
  host: HostMetadata;
  selected_scenarios: string[];
}
