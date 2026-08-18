import { mongoTestStorageFactoryGenerator } from '@module/utils/test-utils.js';
import type { StorageBenchmarkResult } from '@powersync/service-core-tests';
import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { env } from './env.js';
import { TEST_STORAGE_VERSIONS } from './util.js';

const MONGO_BENCHMARK_STORAGE_FACTORY = mongoTestStorageFactoryGenerator({
  url: env.MONGO_TEST_URL,
  isCI: env.CI,
  runMigrations: true,
  clientOptions: {
    connectTimeoutMS: 60_000,
    socketTimeoutMS: 1_200_000,
    serverSelectionTimeoutMS: 60_000
  }
});

const results: StorageBenchmarkResult[] = [];
register.registerStorageBenchmarkSummary(results);

describe.sequential('MongoDB Sync Bucket Storage Benchmarks', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`v${storageVersion}`, () => {
      register.registerStorageBenchmarks(
        { ...MONGO_BENCHMARK_STORAGE_FACTORY, storageVersion },
        {
          storageName: 'mongodb',
          storageVersion,
          results
        }
      );
    });
  }
});
