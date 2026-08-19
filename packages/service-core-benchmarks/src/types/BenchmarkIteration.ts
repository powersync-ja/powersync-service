import { BenchmarkError } from './BenchmarkError.js';
import { ResourceMonitorResult } from './BenchmarkResource.js';

export type BenchmarkIterationKind = 'warmup' | 'measured';

export interface BenchmarkIterationDescriptor {
  iteration: number;
  kind: BenchmarkIterationKind;
}

export interface BenchmarkCorrectnessCheck {
  name: string;
  passed: boolean;
  details?: object;
}

export interface BenchmarkCorrectnessResult {
  passed: boolean;
  checks: BenchmarkCorrectnessCheck[];
}

export type ExecutionResult<T> = {
  succeeded: boolean;
  observation?: T;
};

export interface BenchmarkTimingBoundary {
  start_event: string;
  end_event: string;
  started_at_ms: number;
  ended_at_ms: number;
  duration_ms: number;
}

export interface BenchmarkIterationResult extends BenchmarkIterationDescriptor {
  status: 'passed' | 'failed';
  boundaries: Record<string, BenchmarkTimingBoundary>;
  counters: Record<string, number>;
  resources: ResourceMonitorResult[];
  correctness: BenchmarkCorrectnessResult | null;
  errors: BenchmarkError[];
}
