import { env } from '../env.js';
import { MongoStorageBenchmarkImplementation } from '../implementations/storage/MongoStorageBenchmarkImplementation.js';
import { PostgresStorageBenchmarkImplementation } from '../implementations/storage/PostgresStorageBenchmarkImplementation.js';
import { ApiBenchmarkCase, ApiBenchmarkScenario } from '../types/ApiBenchmark.js';
import { StorageBenchmarkImplementationId } from '../types/StorageBenchmark.js';
import { categorySync, defineScenario, singleTableData } from './scenario-presets.js';

export function createPostgresQuickApiScenario(version: number): ApiBenchmarkScenario {
  return createQuickApiScenario('storage:postgres', version);
}

export function createMongoQuickApiScenario(version: number): ApiBenchmarkScenario {
  return createQuickApiScenario('storage:mongodb', version);
}

export function createPostgresStreamingApiScenario(version: number): ApiBenchmarkScenario {
  return createStreamingApiScenario('storage:postgres', version);
}

export function createMongoStreamingApiScenario(version: number): ApiBenchmarkScenario {
  return createStreamingApiScenario('storage:mongodb', version);
}

function createStreamingApiScenario(
  implementation: StorageBenchmarkImplementationId,
  version: number
): ApiBenchmarkScenario {
  const initial = createQuickApiScenario(implementation, version);
  return defineScenario<ApiBenchmarkScenario>({
    ...initial,
    description: `Deliver post-initial updates to one NDJSON client from ${implementation}`,
    tags: initial.tags.map((tag) => (tag === 'phase:snapshot' ? 'phase:streaming' : tag)),
    mode: 'streaming'
  });
}

export function createMongoCategoryApiScenario(version: number): ApiBenchmarkScenario {
  const scenario = createMongoQuickApiScenario(version);
  return defineScenario<ApiBenchmarkScenario>({
    ...scenario,
    description: 'Drain an initial single-client NDJSON sync across 10 category buckets from mongodb-storage',
    tags: [...scenario.tags, 'shape:10-buckets'],
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 })
  });
}

export function createPostgresCategoryApiScenario(version: number): ApiBenchmarkScenario {
  const scenario = createPostgresQuickApiScenario(version);
  return defineScenario<ApiBenchmarkScenario>({
    ...scenario,
    description: 'Drain an initial single-client NDJSON sync across 10 category buckets from postgres-storage',
    tags: [...scenario.tags, 'shape:10-buckets'],
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 })
  });
}

function createQuickApiScenario(
  implementation: StorageBenchmarkImplementationId,
  version: number
): ApiBenchmarkScenario {
  return defineScenario<ApiBenchmarkScenario>({
    description: `Drain an initial single-client NDJSON sync from ${implementation}`,
    layer: 'api',
    profile: 'quick',
    tags: ['layer:api', 'phase:snapshot', 'profile:quick', implementation, `version:${version}`],
    prerequisites: [implementation],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    storage: { implementation, version },
    mode: 'initial',
    transport: { encoding: 'ndjson', compression: 'none' },
    clients: { count: 1 },
    data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
  });
}

export function createPostgresApiCase(scenario: ApiBenchmarkScenario): ApiBenchmarkCase {
  return {
    scenario,
    implementation: new PostgresStorageBenchmarkImplementation({
      url: env.BENCHMARK_POSTGRES_STORAGE_URL!
    })
  };
}

export function createMongoApiCase(scenario: ApiBenchmarkScenario): ApiBenchmarkCase {
  return {
    scenario,
    implementation: new MongoStorageBenchmarkImplementation({
      url: env.BENCHMARK_MONGODB_STORAGE_URL!,
      isCI: env.CI
    })
  };
}
