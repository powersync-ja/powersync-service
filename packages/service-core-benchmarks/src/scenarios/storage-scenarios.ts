import { env } from '../env.js';
import { MongoStorageBenchmarkImplementation } from '../implementations/storage/MongoStorageBenchmarkImplementation.js';
import { PostgresStorageBenchmarkImplementation } from '../implementations/storage/PostgresStorageBenchmarkImplementation.js';
import {
  StorageBenchmarkCase,
  StorageBenchmarkImplementationId,
  StorageBenchmarkScenario
} from '../types/StorageBenchmark.js';
import { categorySync, defineScenario, singleTableData } from './scenario-presets.js';

export function makeStorageScenario(
  options: Pick<StorageBenchmarkScenario, 'storage' | 'mode' | 'data' | 'sync'> &
    Partial<Pick<StorageBenchmarkScenario, 'profile' | 'timeout_ms' | 'warmup_iterations' | 'measured_iterations'>>
): StorageBenchmarkScenario {
  const profile = options.profile ?? 'quick';
  return defineScenario<StorageBenchmarkScenario>({
    ...options,
    profile,
    layer: 'storage',
    description: `Direct ${options.mode} benchmark`,
    tags: ['layer:storage', `profile:${profile}`, options.storage.implementation, `version:${options.storage.version}`],
    prerequisites: [options.storage.implementation],
    timeout_ms: options.timeout_ms ?? (profile === 'manual' ? 1_800_000 : 120_000),
    warmup_iterations: options.warmup_iterations ?? (profile === 'manual' ? 0 : 1),
    measured_iterations: options.measured_iterations ?? (profile === 'manual' ? 1 : 3),
    flush_policy: 'automatic',
    primary_boundary:
      options.mode === 'write-read' ? 'storage_write_read' : options.mode === 'read' ? 'storage_read' : 'storage_write'
  });
}

export function createPostgresQuickStorageScenario(version: number): StorageBenchmarkScenario {
  return createQuickStorageScenario(
    'storage:postgres',
    'Write 1000 rows to global bucket directly to PostgreSQL bucket storage',
    version
  );
}

export function createMongoQuickStorageScenario(version: number): StorageBenchmarkScenario {
  return createQuickStorageScenario(
    'storage:mongodb',
    'Write 1000 rows to global bucket directly to MongoDB bucket storage',
    version
  );
}

export function createPostgresCategoryStorageScenario(version: number): StorageBenchmarkScenario {
  const scenario = createPostgresQuickStorageScenario(version);
  return defineScenario<StorageBenchmarkScenario>({
    ...scenario,
    description: 'Write 1000 rows across 10 category buckets directly to PostgreSQL bucket storage',
    tags: [...scenario.tags, 'shape:10-buckets'],
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 })
  });
}

export function createMongoCategoryStorageScenario(version: number): StorageBenchmarkScenario {
  const scenario = createMongoQuickStorageScenario(version);
  return defineScenario<StorageBenchmarkScenario>({
    ...scenario,
    description: 'Write 1000 rows across 10 category buckets directly to MongoDB bucket storage',
    tags: [...scenario.tags, 'shape:10-buckets'],
    sync: categorySync({ engine: 'sync-streams', bucketCount: 10 })
  });
}

export function createStorageReadScenario(write: StorageBenchmarkScenario): StorageBenchmarkScenario {
  return defineScenario<StorageBenchmarkScenario>({
    ...write,
    description: `Read ${write.data.tables.reduce((count, table) => count + table.snapshot_row_count, 0)} rows from ${write.expected_bucket_count} bucket${write.expected_bucket_count === 1 ? '' : 's'} in ${write.storage.implementation} bucket storage`,
    primary_boundary: 'storage_read',
    mode: 'read'
  });
}

export function createStorageWriteReadScenario(write: StorageBenchmarkScenario): StorageBenchmarkScenario {
  return defineScenario<StorageBenchmarkScenario>({
    ...write,
    description: `Write and read ${write.data.tables.reduce((count, table) => count + table.snapshot_row_count, 0)} rows in ${write.storage.implementation} bucket storage across ${write.expected_bucket_count} bucket${write.expected_bucket_count === 1 ? '' : 's'}`,
    primary_boundary: 'storage_write_read',
    mode: 'write-read'
  });
}

function createQuickStorageScenario(
  implementation: StorageBenchmarkImplementationId,
  description: string,
  version: number
): StorageBenchmarkScenario {
  return defineScenario<StorageBenchmarkScenario>({
    description,
    layer: 'storage',
    profile: 'quick',
    tags: ['layer:storage', 'profile:quick', implementation, `version:${version}`],
    prerequisites: [implementation],
    timeout_ms: 120_000,
    warmup_iterations: 1,
    measured_iterations: 3,
    storage: {
      implementation,
      version: version
    },
    mode: 'write',
    primary_boundary: 'storage_write',
    flush_policy: 'automatic',
    data: singleTableData({ rowCount: 1_000, rowBytes: 512 }),
    sync: categorySync({ engine: 'sync-streams', bucketCount: 1 })
  });
}

export function createPostgresStorageCase(scenario: StorageBenchmarkScenario): StorageBenchmarkCase {
  return {
    scenario,
    implementation: new PostgresStorageBenchmarkImplementation({
      url: env.BENCHMARK_POSTGRES_STORAGE_URL!
    }),
    expectedStorage: { implementation: 'storage:postgres', version: scenario.storage.version }
  };
}

export function createMongoStorageCase(scenario: StorageBenchmarkScenario): StorageBenchmarkCase {
  return {
    scenario,
    implementation: new MongoStorageBenchmarkImplementation({
      url: env.BENCHMARK_MONGODB_STORAGE_URL!,
      isCI: env.CI
    }),
    expectedStorage: { implementation: 'storage:mongodb', version: scenario.storage.version }
  };
}

export function createStorageModeCases(
  write: StorageBenchmarkScenario,
  createCase: (scenario: StorageBenchmarkScenario) => StorageBenchmarkCase
): StorageBenchmarkCase[] {
  return [write, createStorageReadScenario(write), createStorageWriteReadScenario(write)].map(createCase);
}
