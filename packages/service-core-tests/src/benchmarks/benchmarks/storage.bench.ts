import { CURRENT_STORAGE_VERSION } from '@powersync/service-core';
import { randomUUID } from 'node:crypto';
import { expect, test } from 'vitest';
import { PostgresStorageBenchmarkImplementation } from '../implementations/storage/PostgresStorageBenchmarkImplementation.js';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { UnavailableResourceMonitor } from '../monitors/UnavailableResourceMonitor.js';
import { StorageBenchmark } from '../runner/StorageBenchmark.js';
import { createPostgresQuickStorageScenario } from '../scenarios/storage-scenarios.js';

//TODO: change this to a scenarios array
const scenario = createPostgresQuickStorageScenario();

test(scenario.id, { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
  const benchmark = new StorageBenchmark(
    scenario,
    new PostgresStorageBenchmarkImplementation({
      url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
    }),
    {
      runId: randomUUID(),
      monitors: [
        new NodeProcessResourceMonitor(),
        new UnavailableResourceMonitor('storage_database', 'PostgreSQL database resource monitoring is not implemented')
      ],
      signal: AbortSignal.timeout(scenario.timeout_ms - 10_000)
    }
  );

  const result = await benchmark.run();

  // Log the result
  console.info(
    JSON.stringify(
      {
        scenario: result.scenario.id,
        summary: result.summary,
        resources: result.iterations
          .filter((iteration) => iteration.kind === 'measured')
          .map((iteration) => iteration.resources)
      },
      null,
      2
    )
  );

  // Check everything worked
  // TODO: maybe remove? is this useful?
  expect(result.status, JSON.stringify(result, null, 2)).toBe('passed');
  expect(result.scenario.storage).toEqual({
    implementation: 'postgres-storage',
    version: CURRENT_STORAGE_VERSION
  });
  expect(result.iterations.map(({ kind, status }) => ({ kind, status }))).toEqual([
    { kind: 'warmup', status: 'passed' },
    { kind: 'measured', status: 'passed' },
    { kind: 'measured', status: 'passed' },
    { kind: 'measured', status: 'passed' }
  ]);
  expect(result.iterations[0].resources).toEqual([]);
  expect(
    result.iterations.every((iteration) =>
      iteration.correctness?.checks.some((check) => check.name === 'sample_payload_bytes' && check.passed)
    )
  ).toBe(true);
  expect(
    result.iterations
      .slice(1)
      .map((iteration) => iteration.resources.map(({ component, status }) => ({ component, status })))
  ).toEqual([
    [
      { component: 'load_generator', status: 'available' },
      { component: 'storage_database', status: 'unavailable' }
    ],
    [
      { component: 'load_generator', status: 'available' },
      { component: 'storage_database', status: 'unavailable' }
    ],
    [
      { component: 'load_generator', status: 'available' },
      { component: 'storage_database', status: 'unavailable' }
    ]
  ]);
  expect(result.summary).toMatchObject({
    measured_iterations: 3,
    successful_iterations: 3,
    failed_iterations: 0,
    boundaries: {
      storage_write: { sample_count: 3 }
    },
    counters: {
      source_rows: { sample_count: 3, min: 1_000, max: 1_000 },
      payload_bytes: { sample_count: 3, min: 256_000, max: 256_000 },
      writer_save_calls: { sample_count: 3, min: 1_000, max: 1_000 },
      bucket_operations: { sample_count: 3, min: 1_000, max: 1_000 },
      parameter_operations: { sample_count: 3, min: 0, max: 0 },
      distinct_buckets: { sample_count: 3, min: 1, max: 1 }
    }
  });
});
