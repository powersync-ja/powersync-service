import { ApiBenchmarkScenario } from '../types/ApiBenchmark.js';
import { StorageBenchmarkImplementationId } from '../types/StorageBenchmark.js';

export function createPostgresQuickApiScenario(version: number): ApiBenchmarkScenario {
  return createQuickApiScenario('postgres-storage', version);
}

export function createMongoQuickApiScenario(version: number): ApiBenchmarkScenario {
  return createQuickApiScenario('mongodb-storage', version);
}

function createQuickApiScenario(
  implementation: StorageBenchmarkImplementationId,
  version: number
): ApiBenchmarkScenario {
  return {
    id: `api.initial.baseline.direct.${implementation}.v${version}.quick.ndjson`,
    description: `Drain an initial single-client NDJSON sync from ${implementation}`,
    layer: 'api',
    profile: 'quick',
    tags: ['api', 'initial', 'quick', 'http', 'ndjson', implementation],
    prerequisites: [implementation],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    storage: { implementation, version },
    mode: 'initial',
    transport: { encoding: 'ndjson', compression: 'none' },
    clients: { count: 1 },
    workload: { row_count: 1_000, payload_bytes: 256 }
  };
}
