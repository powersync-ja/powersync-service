import { env } from '../env.js';
import { assertDistinctMongoSourceAndStorage } from '../implementations/replication/mongodb/MongoSourceBenchmarkConfiguration.js';
import { assertDistinctPostgresSourceAndStorage } from '../implementations/replication/postgres/PostgresSourceBenchmarkConfiguration.js';
import { MongoStorageBenchmarkImplementation } from '../implementations/storage/MongoStorageBenchmarkImplementation.js';
import { PostgresStorageBenchmarkImplementation } from '../implementations/storage/PostgresStorageBenchmarkImplementation.js';
import type { ApiBenchmarkScenario } from '../types/ApiBenchmark.js';
import type { BenchmarkScenario } from '../types/BenchmarkScenario.js';
import type { StorageBenchmarkImplementation, StorageBenchmarkScenario } from '../types/StorageBenchmark.js';
import {
  createMongoCategoryApiScenario,
  createMongoQuickApiScenario,
  createMongoStreamingApiScenario,
  createPostgresCategoryApiScenario,
  createPostgresQuickApiScenario,
  createPostgresStreamingApiScenario
} from './api-scenarios.js';
import {
  createCategoryCombinedCases,
  createQuickCombinedCases,
  createStreamingCombinedCases
} from './combined-scenarios.js';
import {
  mongoReplicationStorage,
  mongoSourceCase,
  mongoSourceCategoryCase,
  postgresReplicationStorage,
  postgresSourceCase
} from './replication-scenarios.js';
import { expandScenario } from './scenario-presets.js';
import {
  createMongoCategoryStorageScenario,
  createMongoQuickStorageScenario,
  createPostgresCategoryStorageScenario,
  createPostgresQuickStorageScenario,
  createStorageReadScenario,
  createStorageWriteReadScenario
} from './storage-scenarios.js';
import { MONGO_STORAGE_BENCHMARK_VERSIONS, POSTGRES_STORAGE_BENCHMARK_VERSIONS } from './storage-versions.js';

interface ApiBenchmarkCase {
  readonly scenario: ApiBenchmarkScenario;
  readonly implementation: StorageBenchmarkImplementation;
}

interface StorageBenchmarkCase {
  readonly scenario: StorageBenchmarkScenario;
  readonly implementation: StorageBenchmarkImplementation;
  readonly expectedStorage: StorageBenchmarkScenario['storage'];
}

const baseApiBenchmarkCases: readonly ApiBenchmarkCase[] = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    createPostgresApiCase(createPostgresCategoryApiScenario(version)),
    createPostgresApiCase(createPostgresQuickApiScenario(version)),
    createPostgresApiCase(createPostgresStreamingApiScenario(version))
  ]),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    createMongoApiCase(createMongoQuickApiScenario(version)),
    createMongoApiCase(createMongoCategoryApiScenario(version)),
    createMongoApiCase(createMongoStreamingApiScenario(version))
  ])
];

const baseStorageBenchmarkCases: readonly StorageBenchmarkCase[] = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    ...createStorageModeCases(createPostgresCategoryStorageScenario(version), createPostgresStorageCase),
    ...createStorageModeCases(createPostgresQuickStorageScenario(version), createPostgresStorageCase)
  ]),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    ...createStorageModeCases(createMongoQuickStorageScenario(version), createMongoStorageCase),
    ...createStorageModeCases(createMongoCategoryStorageScenario(version), createMongoStorageCase)
  ])
];

function createStorageModeCases(
  write: StorageBenchmarkScenario,
  createCase: (scenario: StorageBenchmarkScenario) => StorageBenchmarkCase
): StorageBenchmarkCase[] {
  return [write, createStorageReadScenario(write), createStorageWriteReadScenario(write)].map(createCase);
}

const baseReplicationBenchmarkCases = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = postgresReplicationStorage(version);
    return [
      mongoSourceCategoryCase(storage),
      mongoSourceCase('snapshot', storage),
      mongoSourceCase('streaming', storage),
      postgresSourceCase(storage, assertDistinctPostgresSourceAndStorage)
    ];
  }),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = mongoReplicationStorage(version);
    return [
      mongoSourceCategoryCase(storage, assertDistinctMongoSourceAndStorage),
      mongoSourceCase('snapshot', storage, assertDistinctMongoSourceAndStorage),
      mongoSourceCase('streaming', storage, assertDistinctMongoSourceAndStorage),
      postgresSourceCase(storage)
    ];
  })
];

const baseCombinedBenchmarkCases = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = postgresReplicationStorage(version);
    return [
      ...createCategoryCombinedCases(storage),
      ...createQuickCombinedCases(storage),
      ...createStreamingCombinedCases(storage)
    ];
  }),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = mongoReplicationStorage(version);
    return [
      ...createCategoryCombinedCases(storage),
      ...createQuickCombinedCases(storage),
      ...createStreamingCombinedCases(storage)
    ];
  })
];

export const apiBenchmarkCases = expandCases(baseApiBenchmarkCases);
export const storageBenchmarkCases = expandCases(baseStorageBenchmarkCases);
export const replicationBenchmarkCases = expandCases(baseReplicationBenchmarkCases);
export const combinedBenchmarkCases = expandCases(baseCombinedBenchmarkCases);

function expandCases<T extends { readonly scenario: BenchmarkScenario }>(cases: readonly T[]): T[] {
  const expanded: T[] = [];
  for (const base of cases) {
    for (const scenario of expandScenario(base.scenario)) expanded.push({ ...base, scenario });
  }
  return [...new Map(expanded.map((entry) => [entry.scenario.id, entry])).values()];
}

export const allBenchmarkScenarios = [
  ...apiBenchmarkCases,
  ...storageBenchmarkCases,
  ...replicationBenchmarkCases,
  ...combinedBenchmarkCases
].map(({ scenario }) => scenario);

function createPostgresApiCase(scenario: ApiBenchmarkScenario): ApiBenchmarkCase {
  return {
    scenario,
    implementation: new PostgresStorageBenchmarkImplementation({
      url: env.BENCHMARK_POSTGRES_STORAGE_URL!
    })
  };
}

function createMongoApiCase(scenario: ApiBenchmarkScenario): ApiBenchmarkCase {
  return {
    scenario,
    implementation: new MongoStorageBenchmarkImplementation({
      url: env.BENCHMARK_MONGODB_STORAGE_URL!,
      isCI: env.CI
    })
  };
}

function createPostgresStorageCase(scenario: StorageBenchmarkScenario): StorageBenchmarkCase {
  return {
    scenario,
    implementation: new PostgresStorageBenchmarkImplementation({
      url: env.BENCHMARK_POSTGRES_STORAGE_URL!
    }),
    expectedStorage: { implementation: 'storage:postgres', version: scenario.storage.version }
  };
}

function createMongoStorageCase(scenario: StorageBenchmarkScenario): StorageBenchmarkCase {
  return {
    scenario,
    implementation: new MongoStorageBenchmarkImplementation({
      url: env.BENCHMARK_MONGODB_STORAGE_URL!,
      isCI: env.CI
    }),
    expectedStorage: { implementation: 'storage:mongodb', version: scenario.storage.version }
  };
}
