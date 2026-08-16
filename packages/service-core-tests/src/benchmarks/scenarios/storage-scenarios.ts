import { STORAGE_VERSION_2 } from '@powersync/service-core';
import { StorageBenchmarkImplementationId, StorageBenchmarkScenario } from '../types/StorageBenchmark.js';

export function createPostgresQuickStorageScenario(): StorageBenchmarkScenario {
  return createQuickStorageScenario(
    'postgres-storage',
    'Write 1000 rows to global bucket directly to PostgreSQL bucket storage'
  );
}

export function createMongoQuickStorageScenario(): StorageBenchmarkScenario {
  return createQuickStorageScenario(
    'mongodb-storage',
    'Write 1000 rows to global bucket directly to MongoDB bucket storage'
  );
}

function createQuickStorageScenario(
  implementation: StorageBenchmarkImplementationId,
  description: string
): StorageBenchmarkScenario {
  return {
    id: `storage.write.baseline.direct.${implementation}.v${STORAGE_VERSION_2}.quick`,
    description,
    layer: 'storage',
    profile: 'quick',
    tags: ['storage', 'quick', implementation],
    prerequisites: [implementation],
    expected_duration: 'short',
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    storage: {
      implementation,
      version: STORAGE_VERSION_2
    },
    mode: 'write',
    flush_policy: 'automatic',
    workload: {
      row_count: 1_000,
      payload_bytes: 256
    }
  };
}
