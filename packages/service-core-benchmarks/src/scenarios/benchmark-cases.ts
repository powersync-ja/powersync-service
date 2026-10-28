import { assertDistinctMongoSourceAndStorage } from '../implementations/replication/mongodb/MongoSourceBenchmarkConfiguration.js';
import { assertDistinctPostgresSourceAndStorage } from '../implementations/replication/postgres/PostgresSourceBenchmarkConfiguration.js';
import type { ApiBenchmarkCase } from '../types/ApiBenchmark.js';
import type { BenchmarkScenario } from '../types/BenchmarkScenario.js';
import { StorageBenchmarkCase } from '../types/StorageBenchmark.js';
import {
  createMongoApiCase,
  createMongoQuickApiScenario,
  createMongoStreamingApiScenario,
  createPostgresApiCase,
  createPostgresQuickApiScenario,
  createPostgresStreamingApiScenario
} from './api-scenarios.js';
import { BASELINE_DATASETS, type BenchmarkDataset } from './baseline-datasets.js';
import { createQuickCombinedCases, createStreamingCombinedCases } from './combined-scenarios.js';
import {
  mongoReplicationStorage,
  mongoSourceCase,
  postgresReplicationStorage,
  postgresSourceCase
} from './replication-scenarios.js';
import { defineScenario } from './scenario-presets.js';
import {
  createMongoQuickStorageScenario,
  createMongoStorageCase,
  createPostgresQuickStorageScenario,
  createPostgresStorageCase,
  createStorageModeCases
} from './storage-scenarios.js';
import { MONGO_STORAGE_BENCHMARK_VERSIONS, POSTGRES_STORAGE_BENCHMARK_VERSIONS } from './storage-versions.js';

const baseApiBenchmarkCases: readonly ApiBenchmarkCase[] = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    createPostgresApiCase(createPostgresQuickApiScenario(version)),
    createPostgresApiCase(createPostgresStreamingApiScenario(version))
  ]),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    createMongoApiCase(createMongoQuickApiScenario(version)),
    createMongoApiCase(createMongoStreamingApiScenario(version))
  ])
];

const baseReplicationBenchmarkCases = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = postgresReplicationStorage(version);
    return [
      mongoSourceCase('snapshot', storage),
      mongoSourceCase('streaming', storage),
      postgresSourceCase(storage, assertDistinctPostgresSourceAndStorage)
    ];
  }),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = mongoReplicationStorage(version);
    return [
      mongoSourceCase('snapshot', storage, assertDistinctMongoSourceAndStorage),
      mongoSourceCase('streaming', storage, assertDistinctMongoSourceAndStorage),
      postgresSourceCase(storage)
    ];
  })
];

const baseCombinedBenchmarkCases = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = postgresReplicationStorage(version);
    return [...createQuickCombinedCases(storage), ...createStreamingCombinedCases(storage)];
  }),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => {
    const storage = mongoReplicationStorage(version);
    return [...createQuickCombinedCases(storage), ...createStreamingCombinedCases(storage)];
  })
];

const baseStorageBenchmarkCases: readonly StorageBenchmarkCase[] = [
  ...POSTGRES_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    ...createStorageModeCases(createPostgresQuickStorageScenario(version), createPostgresStorageCase)
  ]),
  ...MONGO_STORAGE_BENCHMARK_VERSIONS.flatMap((version) => [
    ...createStorageModeCases(createMongoQuickStorageScenario(version), createMongoStorageCase)
  ])
];

function createDatasetCases<T extends { readonly scenario: BenchmarkScenario }>(
  cases: readonly T[],
  datasets: readonly BenchmarkDataset[]
): T[] {
  return cases.flatMap((base) =>
    datasets.map((dataset) => ({
      ...base,
      scenario: defineScenario(
        {
          ...base.scenario,
          profile: dataset.profile ?? base.scenario.profile,
          timeout_ms: dataset.timeout_ms ?? base.scenario.timeout_ms,
          description: `${dataset.name}: ${base.scenario.layer === 'storage' && 'mode' in base.scenario ? `Direct ${base.scenario.mode} benchmark` : base.scenario.description}`
        },
        dataset.data,
        dataset.sync
      )
    }))
  );
}

export const apiBenchmarkCases = createDatasetCases(baseApiBenchmarkCases, BASELINE_DATASETS);
export const storageBenchmarkCases = createDatasetCases(baseStorageBenchmarkCases, BASELINE_DATASETS);
export const replicationBenchmarkCases = createDatasetCases(baseReplicationBenchmarkCases, BASELINE_DATASETS);
export const combinedBenchmarkCases = createDatasetCases(baseCombinedBenchmarkCases, BASELINE_DATASETS);

export const allBenchmarkScenarios = [
  ...apiBenchmarkCases,
  ...storageBenchmarkCases,
  ...replicationBenchmarkCases,
  ...combinedBenchmarkCases
].map(({ scenario }) => scenario);
