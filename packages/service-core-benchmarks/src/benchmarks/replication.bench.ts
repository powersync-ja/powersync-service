import { CURRENT_STORAGE_VERSION } from '@powersync/service-core';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { beforeAll, describe, expect, test } from 'vitest';
import { assertDistinctMongoSourceAndStorage } from '../implementations/replication/mongodb/MongoSourceBenchmarkConfiguration.js';
import { assertDistinctPostgresSourceAndStorage } from '../implementations/replication/postgres/PostgresSourceBenchmarkConfiguration.js';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { UnavailableResourceMonitor } from '../monitors/UnavailableResourceMonitor.js';
import { ReplicationBenchmark } from '../runner/ReplicationBenchmark.js';
import {
  mongoReplicationStorage,
  mongoSourceCase,
  mongoSourceCategoryCase,
  postgresReplicationStorage,
  postgresSourceCase
} from '../scenarios/replication-scenarios.js';
import { createArtifactsFolder, getArtifactFilename } from '../utils/output.js';

beforeAll(async () => {
  await createArtifactsFolder();
});

const postgresStorage = postgresReplicationStorage(CURRENT_STORAGE_VERSION);
const mongoStorage = mongoReplicationStorage(CURRENT_STORAGE_VERSION);
const cases = [
  mongoSourceCategoryCase(postgresStorage),
  mongoSourceCase('snapshot', postgresStorage),
  mongoSourceCase('streaming', postgresStorage),
  mongoSourceCase('snapshot', mongoStorage, assertDistinctMongoSourceAndStorage),
  mongoSourceCase('streaming', mongoStorage, assertDistinctMongoSourceAndStorage),
  postgresSourceCase(postgresStorage, assertDistinctPostgresSourceAndStorage),
  postgresSourceCase(mongoStorage)
];

describe.each(cases)('$scenario.id', ({ scenario, implementation }) => {
  test('runs', { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
    const benchmark = new ReplicationBenchmark(scenario, implementation, {
      runId: randomUUID(),
      monitors: [
        new NodeProcessResourceMonitor(),
        implementation.createServiceMonitor(),
        new UnavailableResourceMonitor('storage_database', 'Database resource monitoring is not implemented')
      ],
      signal: AbortSignal.timeout(scenario.timeout_ms - 10_000)
    });

    const result = await benchmark.run();
    await writeFile(getArtifactFilename(scenario.id), `${JSON.stringify(result)}\n`, 'utf8');

    expect(result.status, JSON.stringify(result, null, 2)).toBe('passed');
    expect(result.iterations).toHaveLength(scenario.warmup_iterations + scenario.measured_iterations);
    expect(result.iterations.every((iteration) => iteration.status === 'passed')).toBe(true);
    expect(result.iterations.filter((iteration) => iteration.kind === 'measured')).toHaveLength(
      scenario.measured_iterations
    );
  });
});
