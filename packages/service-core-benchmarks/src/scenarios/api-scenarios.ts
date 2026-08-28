import { ApiBenchmarkScenario } from '../types/ApiBenchmark.js';
import { StorageBenchmarkImplementationId } from '../types/StorageBenchmark.js';
import {
  createCategoryStorageSyncRules,
  createCategorySyncParameters,
  createStorageSyncRules
} from '../utils/replication-sync-rules.js';

export function createPostgresQuickApiScenario(version: number): ApiBenchmarkScenario {
  return createQuickApiScenario('postgres-storage', version);
}

export function createMongoQuickApiScenario(version: number): ApiBenchmarkScenario {
  return createQuickApiScenario('mongodb-storage', version);
}

export function createMongoCategoryApiScenario(version: number): ApiBenchmarkScenario {
  const scenario = createMongoQuickApiScenario(version);
  return {
    ...scenario,
    id: `api.initial.buckets-10.direct.mongodb-storage.v${version}.quick.ndjson`,
    description: 'Drain an initial single-client NDJSON sync across 10 category buckets from mongodb-storage',
    tags: [...scenario.tags, 'multi-buckets', 'buckets-10'],
    syncRule: createCategoryStorageSyncRules,
    sync_parameters: createCategorySyncParameters(),
    expected_bucket_count: 10
  };
}

export function createPostgresCategoryApiScenario(version: number): ApiBenchmarkScenario {
  const scenario = createPostgresQuickApiScenario(version);
  return {
    ...scenario,
    id: `api.initial.buckets-10.direct.postgres-storage.v${version}.quick.ndjson`,
    description: 'Drain an initial single-client NDJSON sync across 10 category buckets from postgres-storage',
    tags: [...scenario.tags, 'multi-buckets', 'buckets-10'],
    syncRule: createCategoryStorageSyncRules,
    sync_parameters: createCategorySyncParameters(),
    expected_bucket_count: 10
  };
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
    workload: { row_count: 1_000, payload_bytes: 256 },
    syncRule: createStorageSyncRules,
    sync_parameters: {},
    expected_bucket_count: 1,
    expected_bucket_operation_count: 1_000
  };
}
