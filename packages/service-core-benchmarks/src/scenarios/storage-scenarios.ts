import { StorageBenchmarkImplementationId, StorageBenchmarkScenario } from '../types/StorageBenchmark.js';
import {
  createCategoryStorageSyncRules,
  createCategorySyncParameters,
  createStorageSyncRules
} from '../utils/replication-sync-rules.js';

export function createPostgresQuickStorageScenario(version: number): StorageBenchmarkScenario {
  return createQuickStorageScenario(
    'postgres-storage',
    'Write 1000 rows to global bucket directly to PostgreSQL bucket storage',
    version
  );
}

export function createMongoQuickStorageScenario(version: number): StorageBenchmarkScenario {
  return createQuickStorageScenario(
    'mongodb-storage',
    'Write 1000 rows to global bucket directly to MongoDB bucket storage',
    version
  );
}

export function createPostgresCategoryStorageScenario(version: number): StorageBenchmarkScenario {
  const scenario = createPostgresQuickStorageScenario(version);
  return {
    ...scenario,
    id: `storage.write.buckets-10.postgres-storage.v${version}.quick`,
    description: 'Write 10000 rows across 10 category buckets directly to PostgreSQL bucket storage',
    tags: [...scenario.tags, 'multi-buckets', 'buckets-10'],
    syncRule: createCategoryStorageSyncRules,
    sync_parameters: createCategorySyncParameters(),
    expected_bucket_count: 10
  };
}

export function createMongoCategoryStorageScenario(version: number): StorageBenchmarkScenario {
  const scenario = createMongoQuickStorageScenario(version);
  return {
    ...scenario,
    id: `storage.write.buckets-10.mongo-storage.v${version}.quick`,
    description: 'Write 10000 rows across 10 category buckets directly to MongoDB bucket storage',
    tags: [...scenario.tags, 'multi-buckets', 'buckets-10'],
    syncRule: createCategoryStorageSyncRules,
    sync_parameters: createCategorySyncParameters(),
    expected_bucket_count: 10
  };
}

function createQuickStorageScenario(
  implementation: StorageBenchmarkImplementationId,
  description: string,
  version: number
): StorageBenchmarkScenario {
  return {
    id: `storage.write.${implementation}.v${version}.quick`,
    description,
    layer: 'storage',
    profile: 'quick',
    tags: ['storage', 'quick', implementation],
    prerequisites: [implementation],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    storage: {
      implementation,
      version: version
    },
    mode: 'write',
    flush_policy: 'automatic',
    workload: {
      row_count: 10_000,
      payload_bytes: 256
    },
    syncRule: createStorageSyncRules,
    sync_parameters: {},
    expected_bucket_count: 1,
    expected_bucket_operation_count: 10_000
  };
}
