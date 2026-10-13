import {
  ControlledReplicationBenchmarkImplementation,
  ReplicationBenchmarkSourceSelection,
  ReplicationBenchmarkStorageSelection
} from '../implementations/replication/ControlledReplicationBenchmarkImplementation.js';
import { MongoReplicationSourceAdapter } from '../implementations/replication/mongodb/MongoReplicationSourceAdapter.js';
import {
  canonicalMongoAuthority,
  resolveMongoSourceBenchmarkConfiguration
} from '../implementations/replication/mongodb/MongoSourceBenchmarkConfiguration.js';
import { PostgresReplicationSourceAdapter } from '../implementations/replication/postgres/PostgresReplicationSourceAdapter.js';
import { resolvePostgresSourceBenchmarkConfiguration } from '../implementations/replication/postgres/PostgresSourceBenchmarkConfiguration.js';
import { ReplicationBenchmarkPhase, ReplicationBenchmarkScenario } from '../types/ReplicationBenchmark.js';
import { StorageBenchmarkImplementationId } from '../types/StorageBenchmark.js';
import { categorySync, defineScenario, singleTableData } from './scenario-presets.js';

export function createMongoSourceQuickReplicationScenario(
  phase: ReplicationBenchmarkPhase,
  storage: StorageBenchmarkImplementationId,
  storageVersion: number
): ReplicationBenchmarkScenario {
  return defineScenario<ReplicationBenchmarkScenario>({
    description: `MongoDB ${phase} replication into ${storage} storage version ${storageVersion}`,
    layer: 'replication',
    profile: 'quick',
    tags: [
      'layer:replication',
      `phase:${phase}`,
      'source:mongodb',
      storage,
      `version:${storageVersion}`,
      'profile:quick'
    ],
    prerequisites: ['source:mongodb', storage],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    producer: 'source:mongodb',
    phase,
    storage: { implementation: storage, version: storageVersion },
    checkpoint_policy: 'target-visible',
    core_verification: false,
    data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
  });
}

export function createPostgresSourceQuickReplicationScenario(
  phase: ReplicationBenchmarkPhase,
  storage: StorageBenchmarkImplementationId,
  storageVersion: number
): ReplicationBenchmarkScenario {
  return defineScenario<ReplicationBenchmarkScenario>({
    description: `PostgreSQL ${phase} replication into ${storage} storage version ${storageVersion}`,
    layer: 'replication',
    profile: 'quick',
    tags: [
      'layer:replication',
      `phase:${phase}`,
      'source:postgres',
      storage,
      `version:${storageVersion}`,
      'profile:quick'
    ],
    prerequisites: ['source:postgres', storage],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    producer: 'source:postgres',
    phase,
    storage: { implementation: storage, version: storageVersion },
    checkpoint_policy: 'target-visible',
    core_verification: false,
    data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
  });
}

export function createMongoSourceCategoryReplicationScenario(
  storage: StorageBenchmarkImplementationId,
  storageVersion: number
): ReplicationBenchmarkScenario {
  const scenario = createMongoSourceQuickReplicationScenario('snapshot', storage, storageVersion);
  return defineScenario<ReplicationBenchmarkScenario>({
    ...scenario,
    description: `MongoDB snapshot replication across 10 category buckets into ${storage} storage version ${storageVersion}`,
    tags: [...scenario.tags, 'shape:10-buckets'],
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 })
  });
}

export function postgresReplicationStorage(version: number): ReplicationBenchmarkStorageSelection {
  return {
    id: 'storage:postgres',
    version,
    createChildDescriptor(environment) {
      const url = requiredEnvironmentUrl(environment, 'BENCHMARK_POSTGRES_STORAGE_URL', 'PostgreSQL storage');
      assertPostgresUrl(url, 'BENCHMARK_POSTGRES_STORAGE_URL');
      return {
        moduleUrl: import.meta.resolve('../implementations/storage/PostgresStorageBenchmarkImplementation.js'),
        exportName: 'PostgresStorageBenchmarkImplementation',
        constructorArgs: [{ url }]
      };
    }
  };
}

export function mongoReplicationStorage(version: number): ReplicationBenchmarkStorageSelection {
  return {
    id: 'storage:mongodb',
    version,
    createChildDescriptor(environment) {
      const url = requiredEnvironmentUrl(environment, 'BENCHMARK_MONGODB_STORAGE_URL', 'MongoDB storage');
      canonicalMongoAuthority(url);
      return {
        moduleUrl: import.meta.resolve('../implementations/storage/MongoStorageBenchmarkImplementation.js'),
        exportName: 'MongoStorageBenchmarkImplementation',
        constructorArgs: [{ url, isCI: environment.CI === 'true' }]
      };
    }
  };
}

export function mongoReplicationSource(): ReplicationBenchmarkSourceSelection {
  return {
    id: 'source:mongodb',
    resolveIterationFactory(environment) {
      const { sourceUrl } = resolveMongoSourceBenchmarkConfiguration(environment);
      return () => {
        const adapter = new MongoReplicationSourceAdapter(sourceUrl);
        return {
          adapter,
          createChildDescriptor() {
            return {
              moduleUrl: import.meta.resolve(
                '../implementations/replication/mongodb/MongoReplicationChildImplementation.js'
              ),
              exportName: 'MongoReplicationChildImplementation',
              constructorArgs: [adapter.sourceConfig]
            };
          }
        };
      };
    }
  };
}

export function postgresReplicationSource(): ReplicationBenchmarkSourceSelection {
  return {
    id: 'source:postgres',
    resolveIterationFactory(environment) {
      const { sourceUrl } = resolvePostgresSourceBenchmarkConfiguration(environment);
      return () => {
        const adapter = new PostgresReplicationSourceAdapter(sourceUrl);
        return {
          adapter,
          createChildDescriptor() {
            return {
              moduleUrl: import.meta.resolve(
                '../implementations/replication/postgres/PostgresReplicationChildImplementation.js'
              ),
              exportName: 'PostgresReplicationChildImplementation',
              constructorArgs: [adapter.sourceConfig]
            };
          }
        };
      };
    }
  };
}

export function mongoSourceCase(
  phase: ReplicationBenchmarkPhase,
  storage: ReplicationBenchmarkStorageSelection,
  validateEnvironment?: (environment: Readonly<Record<string, string | undefined>>) => void
): {
  scenario: ReplicationBenchmarkScenario;
  implementation: ControlledReplicationBenchmarkImplementation;
} {
  return {
    scenario: createMongoSourceQuickReplicationScenario(phase, storage.id, storage.version),
    implementation: new ControlledReplicationBenchmarkImplementation({
      source: mongoReplicationSource(),
      storage,
      validateEnvironment
    })
  };
}

export function mongoSourceCategoryCase(
  storage: ReplicationBenchmarkStorageSelection,
  validateEnvironment?: (environment: Readonly<Record<string, string | undefined>>) => void
): {
  scenario: ReplicationBenchmarkScenario;
  implementation: ControlledReplicationBenchmarkImplementation;
} {
  return {
    scenario: createMongoSourceCategoryReplicationScenario(storage.id, storage.version),
    implementation: new ControlledReplicationBenchmarkImplementation({
      source: mongoReplicationSource(),
      storage,
      validateEnvironment
    })
  };
}

export function postgresSourceCase(
  storage: ReplicationBenchmarkStorageSelection,
  validateEnvironment?: (environment: Readonly<Record<string, string | undefined>>) => void
): {
  scenario: ReplicationBenchmarkScenario;
  implementation: ControlledReplicationBenchmarkImplementation;
} {
  return {
    scenario: createPostgresSourceQuickReplicationScenario('snapshot', storage.id, storage.version),
    implementation: new ControlledReplicationBenchmarkImplementation({
      source: postgresReplicationSource(),
      storage,
      validateEnvironment
    })
  };
}

function assertPostgresUrl(url: string, variable: string): void {
  try {
    const parsed = new URL(url);
    if (!['postgres:', 'postgresql:'].includes(parsed.protocol) || parsed.hostname.length === 0) throw new Error();
  } catch {
    throw new Error(`${variable} must be a valid PostgreSQL URL`);
  }
}

function requiredEnvironmentUrl(
  environment: Readonly<Record<string, string | undefined>>,
  variable: string,
  description: string
): string {
  const value = environment[variable]?.trim();
  if (value == null || value.length === 0) throw new Error(`${variable} is required for ${description} benchmarks`);
  return value;
}
