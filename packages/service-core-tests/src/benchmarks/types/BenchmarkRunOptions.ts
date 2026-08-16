import { BenchmarkIterationMetrics } from '../metrics/BenchmarkIterationMetrics.js';
import { BenchmarkIterationDescriptor } from './BenchmarkIteration.js';
import { BenchmarkClock } from './BenchmarkMetrics.js';
import { ResourceMonitor } from './BenchmarkResource.js';

export interface BenchmarkRunOptions {
  readonly runId: string;
  readonly monitors: readonly ResourceMonitor[];
  readonly clock?: BenchmarkClock;
  readonly signal?: AbortSignal;
}

export interface BenchmarkIterationRuntime extends BenchmarkIterationDescriptor {
  readonly signal: AbortSignal;
  readonly metrics: BenchmarkIterationMetrics;
}
