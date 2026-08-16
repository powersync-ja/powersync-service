import { CURRENT_STORAGE_VERSION } from '@powersync/service-core';
import { StorageBenchmarkScenario } from '../types/StorageBenchmark.js';

export function createPostgresQuickStorageScenario(): StorageBenchmarkScenario {
  return {
    id: `storage.write.baseline.direct.postgres-storage.v${CURRENT_STORAGE_VERSION}.quick`,
    description: 'Write 1000 rows to global bucket directly to PostgreSQL bucket storage',
    layer: 'storage',
    profile: 'quick',
    tags: ['storage', 'quick', 'postgres-storage'],
    prerequisites: ['postgres-storage'],
    expected_duration: 'short',
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    storage: {
      implementation: 'postgres-storage',
      version: CURRENT_STORAGE_VERSION
    },
    mode: 'write',
    flush_policy: 'automatic',
    workload: {
      row_count: 1_000,
      payload_bytes: 256
    }
  };
}
