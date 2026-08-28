import { container } from '@powersync/lib-services-framework';
import { CURRENT_STORAGE_VERSION } from '@powersync/service-core';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { beforeAll, describe, expect, test } from 'vitest';
import { MongoStorageBenchmarkImplementation } from '../implementations/storage/MongoStorageBenchmarkImplementation.js';
import { PostgresStorageBenchmarkImplementation } from '../implementations/storage/PostgresStorageBenchmarkImplementation.js';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { UnavailableResourceMonitor } from '../monitors/UnavailableResourceMonitor.js';
import { ApiBenchmark } from '../runner/ApiBenchmark.js';
import {
  createMongoCategoryApiScenario,
  createMongoQuickApiScenario,
  createPostgresCategoryApiScenario,
  createPostgresQuickApiScenario
} from '../scenarios/api-scenarios.js';
import { ApiBenchmarkScenario } from '../types/ApiBenchmark.js';
import { StorageBenchmarkImplementation } from '../types/StorageBenchmark.js';
import { createArtifactsFolder, getArtifactFilename } from '../utils/output.js';

beforeAll(async () => {
  container.registerDefaults();
  await createArtifactsFolder();
});

interface ApiBenchmarkCase {
  readonly scenario: ApiBenchmarkScenario;
  readonly implementation: StorageBenchmarkImplementation;
}

const cases: readonly ApiBenchmarkCase[] = [
  {
    scenario: createPostgresCategoryApiScenario(CURRENT_STORAGE_VERSION),
    implementation: new PostgresStorageBenchmarkImplementation({
      url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
    })
  },
  {
    scenario: createPostgresQuickApiScenario(CURRENT_STORAGE_VERSION),
    implementation: new PostgresStorageBenchmarkImplementation({
      url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
    })
  },
  {
    scenario: createMongoQuickApiScenario(CURRENT_STORAGE_VERSION),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    })
  },
  {
    scenario: createMongoCategoryApiScenario(CURRENT_STORAGE_VERSION),
    implementation: new MongoStorageBenchmarkImplementation({
      url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
      isCI: process.env.CI === 'true'
    })
  }
];

describe.each(cases)('$scenario.id', (benchmarkCase) => {
  const { scenario } = benchmarkCase;

  test('runs', { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
    const benchmark = new ApiBenchmark(scenario, benchmarkCase.implementation, {
      runId: randomUUID(),
      monitors: [
        new NodeProcessResourceMonitor(),
        new UnavailableResourceMonitor('storage_database', 'Database resource monitoring is not implemented')
      ],
      signal: AbortSignal.timeout(scenario.timeout_ms - 10_000)
    });

    const result = await benchmark.run();
    await writeFile(getArtifactFilename(scenario.id), `${JSON.stringify(result)}\n`, 'utf8');

    console.info(JSON.stringify({ scenario: result.scenario.id, summary: result.summary }, null, 2));

    expect(result.status, JSON.stringify(result, null, 2)).toBe('passed');
    expect(result.scenario.storage).toEqual(scenario.storage);
    expect(result.iterations.map(({ kind, status }) => ({ kind, status }))).toEqual([
      { kind: 'warmup', status: 'passed' },
      { kind: 'measured', status: 'passed' },
      { kind: 'measured', status: 'passed' },
      { kind: 'measured', status: 'passed' }
    ]);
    expect(result.summary).toMatchObject({
      measured_iterations: 3,
      successful_iterations: 3,
      failed_iterations: 0,
      boundaries: { http_read: { sample_count: 3 } }
    });
  });
});
