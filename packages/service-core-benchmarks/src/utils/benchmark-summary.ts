import type { BenchmarkIterationResult } from '../types/BenchmarkIteration.js';
import type {
  BenchmarkResourceSummary,
  BenchmarkStatisticSummary,
  BenchmarkSummary
} from '../types/BenchmarkResult.js';
import type { BenchmarkLayer } from '../types/BenchmarkScenario.js';
import { counterUnits, rateDefinitions, resourceUnits } from './metric-definitions.js';

export function summarizeBenchmarkIterations(
  iterations: readonly BenchmarkIterationResult[],
  layer: BenchmarkLayer
): BenchmarkSummary {
  const measured = iterations.filter((iteration) => iteration.kind === 'measured');
  const successful = measured.filter((iteration) => iteration.status === 'passed');
  const summary: BenchmarkSummary = {
    measured_iterations: measured.length,
    successful_iterations: successful.length,
    failed_iterations: measured.length - successful.length,
    boundaries: {},
    counters: {},
    rates: {},
    resources: summarizeResources(measured)
  };

  if (successful.length === 0) return summary;

  assertConsistentNames(successful, 'boundaries');
  assertConsistentNames(successful, 'counters');
  for (const name of Object.keys(successful[0].boundaries).sort()) {
    summary.boundaries[name] = summarizeSamples(
      successful.map((iteration) => iteration.boundaries[name].duration_ms),
      'ms'
    );
  }

  for (const name of Object.keys(successful[0].counters).sort()) {
    const unit = counterUnits[name];
    if (unit == null) throw new Error(`No unit defined for benchmark counter ${name}`);

    summary.counters[name] = summarizeSamples(
      successful.map((iteration) => iteration.counters[name]),
      unit
    );
  }

  for (const [boundary, counters] of Object.entries(rateDefinitions(layer))) {
    // A replication scenario records either snapshot or streaming, never both.
    if (!(boundary in summary.boundaries)) continue;

    for (const counter of counters) {
      if (!(counter in summary.counters)) continue;

      const unit = `${counterUnits[counter]}/s`;
      const samples = successful.flatMap((iteration) => {
        const duration = iteration.boundaries[boundary]?.duration_ms;
        const count = iteration.counters[counter];
        const rate = (count * 1000) / duration;
        return duration > 0 && Number.isFinite(rate) ? [rate] : [];
      });

      const excluded = successful.length - samples.length;
      summary.rates[`${boundary}.${counter}_per_second`] = {
        counter,
        boundary,
        unit,
        statistics: samples.length ? summarizeSamples(samples, unit) : null,
        excluded_samples: excluded,
        reason: excluded ? 'Excluded samples with missing/nonpositive duration or non-finite rate' : null
      };
    }
  }

  return summary;
}

function summarizeResources(measured: readonly BenchmarkIterationResult[]): Record<string, BenchmarkResourceSummary> {
  const result: Record<string, BenchmarkResourceSummary> = {};
  const components = [
    ...new Set(measured.flatMap((iteration) => iteration.resources.map((resource) => resource.component)))
  ].sort();
  for (const component of components) {
    const resources = measured.flatMap((iteration) =>
      iteration.resources.filter((resource) => resource.component === component)
    );

    const available = measured
      .filter((iteration) => iteration.status === 'passed')
      .flatMap((iteration) =>
        iteration.resources.filter((resource) => resource.component === component && resource.status === 'available')
      );

    const metrics: Record<string, BenchmarkStatisticSummary> = {};
    for (const [metric, unit] of Object.entries(resourceUnits)) {
      const samples = available.flatMap((resource) => {
        const value = (resource.metrics as Record<string, unknown>)[metric];
        return typeof value === 'number' && Number.isFinite(value) ? [value] : [];
      });
      if (samples.length) metrics[metric] = summarizeSamples(samples, unit);
    }

    result[component] = {
      available: resources.filter((r) => r.status === 'available').length,
      unavailable: resources.filter((r) => r.status === 'unavailable').length,
      failed: resources.filter((r) => r.status === 'failed').length,
      reasons: [
        ...new Set(
          resources.flatMap((r) => [r.reason, ...r.errors.map((e) => e.message)]).filter((r): r is string => r != null)
        )
      ].sort(),
      metrics
    };
  }

  return result;
}

function assertConsistentNames(
  iterations: readonly BenchmarkIterationResult[],
  property: 'boundaries' | 'counters'
): void {
  const expected = Object.keys(iterations[0][property]).sort();
  for (const iteration of iterations.slice(1)) {
    const actual = Object.keys(iteration[property]).sort();
    if (expected.length !== actual.length || expected.some((name, index) => name !== actual[index])) {
      throw new Error(
        `Successful measured iterations must use identical ${property === 'boundaries' ? 'boundary' : 'counter'} names`
      );
    }
  }
}

function summarizeSamples(samples: readonly number[], unit: string): BenchmarkStatisticSummary {
  if (samples.some((sample) => !Number.isFinite(sample))) throw new Error('Benchmark samples must be finite');
  const sorted = [...samples].sort((left, right) => left - right);
  return {
    unit,
    sample_count: sorted.length,
    min: sorted[0],
    median: quantile(sorted, 0.5),
    avg: sorted.reduce((a, b) => a + b, 0) / sorted.length,
    ...(sorted.length >= 20 ? { p95: quantile(sorted, 0.95) } : {}),
    ...(sorted.length >= 100 ? { p99: quantile(sorted, 0.99) } : {}),
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
