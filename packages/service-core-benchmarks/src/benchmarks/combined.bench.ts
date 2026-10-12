import { randomUUID } from 'node:crypto';
import { describe, expect, test } from 'vitest';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { createDatabaseResourceMonitors } from '../monitors/database-monitors.js';
import { CombinedBenchmark } from '../runner/CombinedBenchmark.js';
import { combinedBenchmarkCases } from '../scenarios/benchmark-cases.js';
import { writeBenchmarkResult } from '../utils/output.js';

describe.each(combinedBenchmarkCases)('$scenario.id', ({ scenario, implementation }) => {
  test('runs', { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
    const benchmark = new CombinedBenchmark(scenario, implementation, {
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
    expect(result.iterations.map(({ kind, status }) => ({ kind, status }))).toEqual([
      ...Array.from({ length: scenario.warmup_iterations }, () => ({ kind: 'warmup', status: 'passed' })),
      ...Array.from({ length: scenario.measured_iterations }, () => ({ kind: 'measured', status: 'passed' }))
    ]);
    expect(result.summary).toMatchObject({
      measured_iterations: scenario.measured_iterations,
      successful_iterations: scenario.measured_iterations,
      failed_iterations: 0,
      boundaries:
        scenario.mode === 'streaming'
          ? {
              replication_streaming: { sample_count: scenario.measured_iterations },
              http_read: { sample_count: scenario.measured_iterations },
              end_to_end_streaming: { sample_count: scenario.measured_iterations }
            }
          : {
              replication_snapshot: { sample_count: scenario.measured_iterations },
              http_read: { sample_count: scenario.measured_iterations },
              end_to_end_snapshot: { sample_count: scenario.measured_iterations }
            }
    });
  });
});
