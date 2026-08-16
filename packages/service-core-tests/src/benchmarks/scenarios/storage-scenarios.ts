import { StorageBenchmarkImplementationId, StorageBenchmarkScenario } from '../types/StorageBenchmark.js';

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
      row_count: 1_000,
      payload_bytes: 256
    }
  };
}
