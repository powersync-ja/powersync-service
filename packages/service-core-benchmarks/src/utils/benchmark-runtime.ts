import { BenchmarkIterationMetrics } from '../metrics/BenchmarkIterationMetrics.js';
import { BenchmarkIterationDescriptor } from '../types/BenchmarkIteration.js';
import { BenchmarkClock } from '../types/BenchmarkMetrics.js';
import { BenchmarkIterationRuntime } from '../types/BenchmarkRunOptions.js';

export const performanceClock: BenchmarkClock = {
  now: () => performance.now()
};

export function createIterationRuntime(
  descriptor: BenchmarkIterationDescriptor,
  signal: AbortSignal,
  clock: BenchmarkClock
): BenchmarkIterationRuntime {
  return {
    ...descriptor,
    signal,
    metrics: new BenchmarkIterationMetrics(clock)
  };
}
