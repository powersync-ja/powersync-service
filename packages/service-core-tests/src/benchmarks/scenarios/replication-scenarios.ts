import { CURRENT_STORAGE_VERSION } from '@powersync/service-core';
import { ControlledSyntheticReplicationBenchmarkImplementation } from '../implementations/replication/ControlledSyntheticReplicationBenchmarkImplementation.js';
import { ControlledMongoReplicationBenchmarkImplementation } from '../implementations/replication/mongodb/ControlledMongoReplicationBenchmarkImplementation.js';
import { ReplicationBenchmarkPhase, ReplicationBenchmarkScenario } from '../types/ReplicationBenchmark.js';
import { StorageBenchmarkImplementationId } from '../types/StorageBenchmark.js';

export function createSyntheticQuickReplicationScenario(
  phase: ReplicationBenchmarkPhase,
  storage: StorageBenchmarkImplementationId,
  storageVersion: number
): ReplicationBenchmarkScenario {
  return {
    id: `replication.${phase}.baseline.synthetic-source.${storage}.v${storageVersion}.quick`,
    description: `Synthetic ${phase} replication into ${storage} storage version ${storageVersion}`,
    layer: 'replication',
    profile: 'quick',
    tags: ['replication', phase, 'baseline', 'synthetic-source', storage, `storage-v${storageVersion}`, 'quick'],
    prerequisites: [storage],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    producer: 'synthetic-source',
    phase,
    storage: { implementation: storage, version: storageVersion },
    checkpoint_policy: 'target-visible',
    core_verification: false,
    workload: {
      snapshot_row_count: 1_000,
      streaming_mutation_count: 1_000,
      transaction_count: 10,
      payload_bytes: 256
    }
  };
}

export function createMongoSourceQuickReplicationScenario(
  phase: ReplicationBenchmarkPhase,
  storage: StorageBenchmarkImplementationId,
  storageVersion: number
): ReplicationBenchmarkScenario {
  return {
    id: `replication.${phase}.baseline.mongodb-source.${storage}.v${storageVersion}.quick`,
    description: `MongoDB ${phase} replication into ${storage} storage version ${storageVersion}`,
    layer: 'replication',
    profile: 'quick',
    tags: ['replication', phase, 'baseline', 'mongodb-source', storage, `storage-v${storageVersion}`, 'quick'],
    prerequisites: ['mongodb-source', storage],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    producer: 'mongodb-source',
    phase,
    storage: { implementation: storage, version: storageVersion },
    checkpoint_policy: 'target-visible',
    core_verification: false,
    workload: {
      snapshot_row_count: 1_000,
      streaming_mutation_count: 1_000,
      transaction_count: 10,
      payload_bytes: 256
    }
  };
}

export function postgresCase(phase: ReplicationBenchmarkPhase): {
  scenario: ReplicationBenchmarkScenario;
  implementation: ControlledSyntheticReplicationBenchmarkImplementation;
} {
  return {
    scenario: createSyntheticQuickReplicationScenario(phase, 'postgres-storage', CURRENT_STORAGE_VERSION),
    implementation: new ControlledSyntheticReplicationBenchmarkImplementation({
      storage: {
        implementation: 'postgres-storage',
        version: CURRENT_STORAGE_VERSION,
        url: process.env.PG_STORAGE_TEST_URL ?? 'postgres://postgres:postgres@localhost:5432/powersync_storage_test'
      }
    })
  };
}

export function mongoCase(phase: ReplicationBenchmarkPhase): {
  scenario: ReplicationBenchmarkScenario;
  implementation: ControlledSyntheticReplicationBenchmarkImplementation;
} {
  return {
    scenario: createSyntheticQuickReplicationScenario(phase, 'mongodb-storage', CURRENT_STORAGE_VERSION),
    implementation: new ControlledSyntheticReplicationBenchmarkImplementation({
      storage: {
        implementation: 'mongodb-storage',
        version: CURRENT_STORAGE_VERSION,
        url: process.env.MONGO_TEST_URL ?? 'mongodb://localhost:27017/powersync_test',
        isCI: process.env.CI === 'true'
      }
    })
  };
}

export function mongoSourceCase(
  phase: ReplicationBenchmarkPhase,
  storage: StorageBenchmarkImplementationId
): {
  scenario: ReplicationBenchmarkScenario;
  implementation: ControlledMongoReplicationBenchmarkImplementation;
} {
  return {
    scenario: createMongoSourceQuickReplicationScenario(phase, storage, CURRENT_STORAGE_VERSION),
    implementation: new ControlledMongoReplicationBenchmarkImplementation({
      storage: {
        implementation: storage,
        version: CURRENT_STORAGE_VERSION,
        isCI: process.env.CI === 'true'
      }
    })
  };
}
