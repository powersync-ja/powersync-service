import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { beforeAll, describe, expect, test } from 'vitest';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { UnavailableResourceMonitor } from '../monitors/UnavailableResourceMonitor.js';
import { ReplicationBenchmark } from '../runner/ReplicationBenchmark.js';
import { mongoSourceCase } from '../scenarios/replication-scenarios.js';
import { createArtifactsFolder, getArtifactFilename } from '../utils/output.js';

beforeAll(async () => {
  await createArtifactsFolder();
});

const cases = [
  mongoSourceCase('snapshot', 'postgres-storage'),
  mongoSourceCase('streaming', 'postgres-storage'),
  mongoSourceCase('snapshot', 'mongodb-storage'),
  mongoSourceCase('streaming', 'mongodb-storage')
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
