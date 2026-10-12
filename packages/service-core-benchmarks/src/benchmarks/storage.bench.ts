import { randomUUID } from 'node:crypto';
import { describe, expect, test } from 'vitest';
import { createDataManifest } from '../generators/benchmark-data.js';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { createDatabaseResourceMonitors } from '../monitors/database-monitors.js';
import { StorageBenchmark } from '../runner/StorageBenchmark.js';
import { storageBenchmarkCases } from '../scenarios/benchmark-cases.js';
import { writeBenchmarkResult } from '../utils/output.js';

describe.each(storageBenchmarkCases)('$scenario.id', ({ scenario, implementation, expectedStorage }) => {
  test('runs', { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
    const benchmark = new StorageBenchmark(scenario, implementation, {
      runId: randomUUID(),
      monitors: [new NodeProcessResourceMonitor(), ...createDatabaseResourceMonitors(scenario)],
      signal: AbortSignal.timeout(scenario.timeout_ms - 10_000)
    });

    const result = await benchmark.run();

    await writeBenchmarkResult(result);

    // Check everything worked
    expect(result.status, JSON.stringify(result, null, 2)).toBe('passed');
    expect(result.scenario.storage).toEqual(expectedStorage);
    expect(result.iterations.map(({ kind, status }) => ({ kind, status }))).toEqual([
      ...Array.from({ length: scenario.warmup_iterations }, () => ({ kind: 'warmup', status: 'passed' })),
      ...Array.from({ length: scenario.measured_iterations }, () => ({ kind: 'measured', status: 'passed' }))
    ]);
    if (scenario.warmup_iterations) expect(result.iterations[0].resources).toEqual([]);
    expect(
      result.iterations.every((iteration) =>
        iteration.correctness?.checks.some((check) => check.name === 'total_memberships' && check.passed)
      )
    ).toBe(true);
    for (const iteration of result.iterations.filter((iteration) => iteration.kind === 'measured')) {
      expect(iteration.resources.map(({ component, status }) => ({ component, status }))).toEqual([
        { component: 'load_generator', status: 'available' },
        { component: 'storage_database', status: expect.stringMatching(/available|unavailable/) }
      ]);
    }
    const manifest = createDataManifest(scenario.data, 'snapshot');
    expect(result.summary).toMatchObject({
      measured_iterations: scenario.measured_iterations,
      successful_iterations: scenario.measured_iterations,
      failed_iterations: 0,
      boundaries: {
        [scenario.primary_boundary]: { sample_count: scenario.measured_iterations }
      },
      counters: {
        source_rows: {
          sample_count: scenario.measured_iterations,
          min: manifest.snapshotRowCount,
          max: manifest.snapshotRowCount
        },
        payload_bytes: {
          sample_count: scenario.measured_iterations,
          min: manifest.payloadBytes,
          max: manifest.payloadBytes
        },
        bucket_operations: {
          sample_count: scenario.measured_iterations,
          min: scenario.expected_bucket_operation_count,
          max: scenario.expected_bucket_operation_count
        },
        parameter_operations: { sample_count: scenario.measured_iterations, min: 0, max: 0 },
        distinct_buckets: {
          sample_count: scenario.measured_iterations,
          min: scenario.expected_bucket_count,
          max: scenario.expected_bucket_count
        }
      }
    });
    if (scenario.mode !== 'write') {
      expect(result.summary?.boundaries.storage_read?.sample_count).toBe(scenario.measured_iterations);
      expect(result.summary?.counters.read_data_bytes?.min).toBeGreaterThan(0);
    }
    if (scenario.mode !== 'read') {
      expect(result.summary?.boundaries.storage_write?.sample_count).toBe(scenario.measured_iterations);
      expect(result.summary?.counters.writer_save_calls?.min).toBe(manifest.snapshotRowCount);
    }
  });
});
