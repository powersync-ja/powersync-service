import { STORAGE_VERSION_1, STORAGE_VERSION_2, STORAGE_VERSION_3 } from '@powersync/service-core';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { beforeAll, describe, expect, test } from 'vitest';
import { MongoStorageBenchmarkImplementation } from '../implementations/storage/MongoStorageBenchmarkImplementation.js';
import { PostgresStorageBenchmarkImplementation } from '../implementations/storage/PostgresStorageBenchmarkImplementation.js';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { UnavailableResourceMonitor } from '../monitors/UnavailableResourceMonitor.js';
import { StorageBenchmark } from '../runner/StorageBenchmark.js';
import {
  createMongoCategoryStorageScenario,
  createMongoQuickStorageScenario,
  createPostgresCategoryStorageScenario,
  createPostgresQuickStorageScenario
} from '../scenarios/storage-scenarios.js';
import { StorageBenchmarkImplementation, StorageBenchmarkScenario } from '../types/StorageBenchmark.js';
import { createArtifactsFolder, getArtifactFilename } from '../utils/output.js';

beforeAll(async () => {
  // TODO: Move this outside of this test file, into setup
  await createArtifactsFolder();
});

interface StorageBenchmarkCase {
  readonly scenario: StorageBenchmarkScenario;
  readonly implementation: StorageBenchmarkImplementation;
  readonly expectedStorage: StorageBenchmarkScenario['storage'];
  readonly unavailableMonitorReason: string;
}

const benchmarkCases: readonly StorageBenchmarkCase[] = [
  {
    scenario: createPostgresCategoryStorageScenario(STORAGE_VERSION_1),
    implementation: new PostgresStorageBenchmarkImplementation({
      url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
    }),
    expectedStorage: { implementation: 'postgres-storage', version: 1 },
    unavailableMonitorReason: 'PostgreSQL database resource monitoring is not implemented'
  },
  {
    scenario: createPostgresCategoryStorageScenario(STORAGE_VERSION_2),
    implementation: new PostgresStorageBenchmarkImplementation({
      url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
    }),
    expectedStorage: { implementation: 'postgres-storage', version: 2 },
    unavailableMonitorReason: 'PostgreSQL database resource monitoring is not implemented'
  },
  {
    scenario: createPostgresQuickStorageScenario(STORAGE_VERSION_2),
    implementation: new PostgresStorageBenchmarkImplementation({
      url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
    }),
    expectedStorage: { implementation: 'postgres-storage', version: 2 },
    unavailableMonitorReason: 'PostgreSQL database resource monitoring is not implemented'
  },
  {
    scenario: createPostgresQuickStorageScenario(STORAGE_VERSION_1),
    implementation: new PostgresStorageBenchmarkImplementation({
      url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
    }),
    expectedStorage: { implementation: 'postgres-storage', version: 1 },
    unavailableMonitorReason: 'PostgreSQL database resource monitoring is not implemented'
  },
  {
    scenario: createMongoQuickStorageScenario(STORAGE_VERSION_1),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    }),
    expectedStorage: { implementation: 'mongodb-storage', version: STORAGE_VERSION_1 },
    unavailableMonitorReason: 'MongoDB database resource monitoring is not implemented'
  },
  {
    scenario: createMongoQuickStorageScenario(STORAGE_VERSION_2),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    }),
    expectedStorage: { implementation: 'mongodb-storage', version: STORAGE_VERSION_2 },
    unavailableMonitorReason: 'MongoDB database resource monitoring is not implemented'
  },
  {
    scenario: createMongoQuickStorageScenario(STORAGE_VERSION_3),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    }),
    expectedStorage: { implementation: 'mongodb-storage', version: STORAGE_VERSION_3 },
    unavailableMonitorReason: 'MongoDB database resource monitoring is not implemented'
  },
  {
    scenario: createMongoCategoryStorageScenario(STORAGE_VERSION_1),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    }),
    expectedStorage: { implementation: 'mongodb-storage', version: STORAGE_VERSION_1 },
    unavailableMonitorReason: 'MongoDB database resource monitoring is not implemented'
  },
  {
    scenario: createMongoCategoryStorageScenario(STORAGE_VERSION_2),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    }),
    expectedStorage: { implementation: 'mongodb-storage', version: STORAGE_VERSION_2 },
    unavailableMonitorReason: 'MongoDB database resource monitoring is not implemented'
  },
  {
    scenario: createMongoCategoryStorageScenario(STORAGE_VERSION_3),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    }),
    expectedStorage: { implementation: 'mongodb-storage', version: STORAGE_VERSION_3 },
    unavailableMonitorReason: 'MongoDB database resource monitoring is not implemented'
  }
];

describe.each(benchmarkCases)('$scenario.id', (benchmarkCase) => {
  const { scenario } = benchmarkCase;

  test('runs', { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
    const benchmark = new StorageBenchmark(scenario, benchmarkCase.implementation, {
      runId: randomUUID(),
      monitors: [
        new NodeProcessResourceMonitor(),
        new UnavailableResourceMonitor('storage_database', benchmarkCase.unavailableMonitorReason)
      ],
      signal: AbortSignal.timeout(scenario.timeout_ms - 10_000)
    });

    const result = await benchmark.run();

    //TODO: Make this not overwrite?
    await writeFile(getArtifactFilename(scenario.id), `${JSON.stringify(result)}\n`, 'utf8');
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
    expect(result.scenario.storage).toEqual(benchmarkCase.expectedStorage);
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
        source_rows: { sample_count: 3, min: 10_000, max: 10_000 },
        payload_bytes: { sample_count: 3, min: 2_560_000, max: 2_560_000 },
        writer_save_calls: { sample_count: 3, min: 10_000, max: 10_000 },
        bucket_operations: { sample_count: 3, min: 10_000, max: 10_000 },
        parameter_operations: { sample_count: 3, min: 0, max: 0 },
        distinct_buckets: {
          sample_count: 3,
          min: scenario.expected_bucket_count,
          max: scenario.expected_bucket_count
        }
      }
    });
  });
});
