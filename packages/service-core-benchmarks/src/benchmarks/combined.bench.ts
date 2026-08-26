import { CURRENT_STORAGE_VERSION } from '@powersync/service-core';
import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { beforeAll, describe, expect, test } from 'vitest';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { UnavailableResourceMonitor } from '../monitors/UnavailableResourceMonitor.js';
import { CombinedBenchmark } from '../runner/CombinedBenchmark.js';
import { createCategoryCombinedCases, createQuickCombinedCases } from '../scenarios/combined-scenarios.js';
import { createArtifactsFolder, getArtifactFilename } from '../utils/output.js';

beforeAll(async () => {
  await createArtifactsFolder();
});

const cases = [
  ...createCategoryCombinedCases(CURRENT_STORAGE_VERSION),
  ...createQuickCombinedCases(CURRENT_STORAGE_VERSION)
];

describe.each(cases)('$scenario.id', ({ scenario, implementation }) => {
  test('runs', { timeout: scenario.timeout_ms, sequential: true, tags: scenario.tags }, async () => {
    const benchmark = new CombinedBenchmark(scenario, implementation, {
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
      boundaries: {
        replication_snapshot: { sample_count: 3 },
        http_read: { sample_count: 3 },
        end_to_end_snapshot: { sample_count: 3 }
      }
    });
  });
});
