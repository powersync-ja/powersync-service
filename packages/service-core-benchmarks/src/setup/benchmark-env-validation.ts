import { mongo } from '@powersync/lib-service-mongodb';
import {
  assertDistinctMongoSourceAndStorage,
  canonicalMongoAuthority
} from '../implementations/replication/mongodb/MongoSourceBenchmarkConfiguration.js';
import {
  assertDistinctPostgresSourceAndStorage,
  canonicalPostgresAuthority
} from '../implementations/replication/postgres/PostgresSourceBenchmarkConfiguration.js';
import type { BenchmarkLayer } from '../types/BenchmarkScenario.js';
import type { ReplicationBenchmarkProducerId } from '../types/ReplicationBenchmark.js';
import type { StorageBenchmarkImplementationId } from '../types/StorageBenchmark.js';

export interface BenchmarkEnvironmentScenario {
  readonly tags: readonly string[];
  readonly layer: BenchmarkLayer;
  readonly producer?: ReplicationBenchmarkProducerId;
  readonly storage: { readonly implementation: StorageBenchmarkImplementationId };
}

export function validateSelectedBenchmarkEnvironment(
  scenarios: readonly BenchmarkEnvironmentScenario[],
  environment: Readonly<Record<string, string | undefined>>,
  matchesTags: (tags: string[]) => boolean
): void {
  const requiredVariables = new Set<string>();
  const distinctSources = new Set<ReplicationBenchmarkProducerId>();

  for (const scenario of scenarios) {
    if (!matchesTags([...scenario.tags])) continue;

    const postgresStorage = scenario.storage.implementation === 'storage:postgres';
    const storageVariable = postgresStorage ? 'BENCHMARK_POSTGRES_STORAGE_URL' : 'BENCHMARK_MONGODB_STORAGE_URL';
    requiredVariables.add(storageVariable);
    if (scenario.layer === 'storage' || scenario.layer === 'api') {
      continue;
    }

    if (scenario.producer === 'source:postgres') {
      requiredVariables.add('BENCHMARK_POSTGRES_SOURCE_URL');
      if (postgresStorage) distinctSources.add(scenario.producer);
    } else if (scenario.producer === 'source:mongodb') {
      requiredVariables.add('BENCHMARK_MONGODB_SOURCE_URL');
      if (!postgresStorage) distinctSources.add(scenario.producer);
    }
  }

  const errors: string[] = [];
  const validVariables = new Set<string>();
  for (const variable of requiredVariables) {
    const value = environment[variable]?.trim();
    if (!value) {
      errors.push(`${variable} is required`);
      continue;
    }
    try {
      if (variable.includes('POSTGRES')) {
        canonicalPostgresAuthority(value, variable);
      } else {
        try {
          new mongo.MongoClient(value);
        } catch {
          throw new Error('must be a valid mongodb:// or mongodb+srv:// URL');
        }
        canonicalMongoAuthority(value);
      }
      validVariables.add(variable);
    } catch (error) {
      errors.push(`${variable}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  if (
    distinctSources.has('source:postgres') &&
    validVariables.has('BENCHMARK_POSTGRES_SOURCE_URL') &&
    validVariables.has('BENCHMARK_POSTGRES_STORAGE_URL')
  ) {
    try {
      assertDistinctPostgresSourceAndStorage(environment);
    } catch (error) {
      errors.push(error instanceof Error ? error.message : String(error));
    }
  }
  if (
    distinctSources.has('source:mongodb') &&
    validVariables.has('BENCHMARK_MONGODB_SOURCE_URL') &&
    validVariables.has('BENCHMARK_MONGODB_STORAGE_URL')
  ) {
    try {
      assertDistinctMongoSourceAndStorage(environment);
    } catch (error) {
      errors.push(error instanceof Error ? error.message : String(error));
    }
  }

  if (errors.length > 0) throw new Error(`Invalid benchmark environment:\n- ${errors.join('\n- ')}`);
}
