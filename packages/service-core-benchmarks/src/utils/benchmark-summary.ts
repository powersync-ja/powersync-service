import { BenchmarkIterationResult } from '../types/BenchmarkIteration.js';
import { BenchmarkStatisticSummary, BenchmarkSummary } from '../types/BenchmarkResult.js';

export function summarizeBenchmarkIterations(iterations: readonly BenchmarkIterationResult[]): BenchmarkSummary {
  const measured = iterations.filter((iteration) => iteration.kind === 'measured');
  const successful = measured.filter((iteration) => iteration.status === 'passed');

  if (successful.length === 0) {
    return {
      measured_iterations: measured.length,
      successful_iterations: 0,
      failed_iterations: measured.length,
      boundaries: {},
      counters: {}
    };
  }

  assertConsistentNames(successful, 'boundaries');
  assertConsistentNames(successful, 'counters');

  const boundaryNames = Object.keys(successful[0].boundaries);
  const counterNames = Object.keys(successful[0].counters);

  return {
    measured_iterations: measured.length,
    successful_iterations: successful.length,
    failed_iterations: measured.length - successful.length,
    boundaries: Object.fromEntries(
      boundaryNames.map((name) => [
        name,
        summarizeSamples(successful.map((iteration) => iteration.boundaries[name].duration_ms))
      ])
    ),
    counters: Object.fromEntries(
      counterNames.map((name) => [name, summarizeSamples(successful.map((iteration) => iteration.counters[name]))])
    )
  };
}

function assertConsistentNames(
  iterations: readonly BenchmarkIterationResult[],
  property: 'boundaries' | 'counters'
): void {
  const expected = Object.keys(iterations[0][property]).sort();
  for (const iteration of iterations.slice(1)) {
    const actual = Object.keys(iteration[property]).sort();
    if (expected.length !== actual.length || expected.some((name, index) => name !== actual[index])) {
      const label = property === 'boundaries' ? 'boundary' : 'counter';
      throw new Error(`Successful measured iterations must use identical ${label} names`);
    }
  }
}

function summarizeSamples(samples: readonly number[]): BenchmarkStatisticSummary {
  const sorted = [...samples].sort((left, right) => left - right);
  return {
    sample_count: sorted.length,
    min: sorted[0],
    median: quantile(sorted, 0.5),
    avg: sorted.reduce((a, b) => a + b, 0) / sorted.length,
    p95: quantile(sorted, 0.95),
    p99: quantile(sorted, 0.99),
    max: sorted.at(-1)!
  };
}

function quantile(sorted: readonly number[], probability: number): number {
  const index = (sorted.length - 1) * probability;
  const lowerIndex = Math.floor(index);
  const upperIndex = Math.ceil(index);
  const lower = sorted[lowerIndex];
  const upper = sorted[upperIndex];
  return lower + (upper - lower) * (index - lowerIndex);
}
