import { randomUUID } from 'node:crypto';
import { describe, expect, test } from 'vitest';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { createDatabaseResourceMonitors } from '../monitors/database-monitors.js';
import { ReplicationBenchmark } from '../runner/ReplicationBenchmark.js';
import { replicationBenchmarkCases } from '../scenarios/benchmark-cases.js';
import { writeBenchmarkResult } from '../utils/output.js';

describe.each(replicationBenchmarkCases)('$scenario.id', ({ scenario, implementation }) => {
  test('runs', { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
    const benchmark = new ReplicationBenchmark(scenario, implementation, {
      runId: randomUUID(),
      monitors: [
        new NodeProcessResourceMonitor(),
        implementation.createServiceMonitor(),
        ...createDatabaseResourceMonitors(scenario)
      ],
      signal: AbortSignal.timeout(scenario.timeout_ms - 10_000)
    });

    const result = await benchmark.run();
    await writeBenchmarkResult(result);

    expect(result.status, JSON.stringify(result, null, 2)).toBe('passed');
    expect(result.iterations).toHaveLength(scenario.warmup_iterations + scenario.measured_iterations);
    expect(result.iterations.every((iteration) => iteration.status === 'passed')).toBe(true);
    expect(result.iterations.filter((iteration) => iteration.kind === 'measured')).toHaveLength(
      scenario.measured_iterations
    );
  });
});
