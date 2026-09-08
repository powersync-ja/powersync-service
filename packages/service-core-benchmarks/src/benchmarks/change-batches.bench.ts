import { randomUUID } from 'node:crypto';
import { writeFile } from 'node:fs/promises';
import { expect, test } from 'vitest';
import { NodeProcessResourceMonitor } from '../monitors/NodeProcessResourceMonitor.js';
import { MongoChangeBatchBenchmark } from '../runner/MongoChangeBatchBenchmark.js';
import { createChangeBatchScenario } from '../scenarios/mongodb-change-batches.js';
import { createArtifactsFolder, getArtifactFilename } from '../utils/output.js';

const scenario = createChangeBatchScenario();
test.skipIf(process.env.BENCHMARK_CHANGE_BATCHES !== 'true')(
  scenario.id,
  { timeout: scenario.timeout_ms },
  async () => {
    const started = performance.now();
    const runId = randomUUID();
    const result = await new MongoChangeBatchBenchmark(scenario, {
      runId,
      monitors: [new NodeProcessResourceMonitor()],
      signal: AbortSignal.timeout(scenario.timeout_ms)
    }).run();
    result.environment = { ...result.environment, run_wall_ms: performance.now() - started };
    await createArtifactsFolder();
    const artifact = getArtifactFilename(`${scenario.id}.${runId}`);
    await writeFile(artifact, JSON.stringify(result) + '\n');
    process.stdout.write(
      JSON.stringify(
        {
          scenario: scenario.id,
          status: result.status,
          duration_ms: result.summary?.boundaries.change_batches?.median,
          rows_per_second: result.summary?.counters.rows_per_second?.median,
          run_wall_ms: performance.now() - started,
          artifact
        },
        null,
        2
      ) + '\n'
    );
    expect(result.status, JSON.stringify(result, null, 2)).toBe('passed');
  }
);
