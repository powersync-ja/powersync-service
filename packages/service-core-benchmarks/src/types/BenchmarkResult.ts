import { BenchmarkError } from './BenchmarkError.js';
import { BenchmarkIterationResult } from './BenchmarkIteration.js';
import { BenchmarkScenario } from './BenchmarkScenario.js';
export { BenchmarkResourceComponent, ResourceMonitorResult } from './BenchmarkResource.js';

export interface BenchmarkStatisticSummary {
  sample_count: number;
  min: number;
  median: number;
  avg: number;
  p95: number;
  p99: number;
  max: number;
}

export interface BenchmarkSummary {
  measured_iterations: number;
  successful_iterations: number;
  failed_iterations: number;
  boundaries: Record<string, BenchmarkStatisticSummary>;
  counters: Record<string, BenchmarkStatisticSummary>;
}

export interface BenchmarkResult<Scenario extends BenchmarkScenario = BenchmarkScenario> {
  scenario: Scenario;
  status: 'passed' | 'failed' | 'skipped';
  environment: object;
  iterations: BenchmarkIterationResult[];
  summary: BenchmarkSummary | null;
  errors: BenchmarkError[];
}
