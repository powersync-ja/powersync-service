import type { StorageBenchmarkResult } from '@powersync/service-core-tests';
import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const results: StorageBenchmarkResult[] = [];
register.registerStorageBenchmarkSummary(results);

const scenarios = register.DEFAULT_STORAGE_BENCHMARK_SCENARIOS.filter(
  (scenario) =>
    // scenario.todo_row_count < 1_000_000 &&
    scenario.max_bucket_count == null || scenario.max_bucket_count <= 1_000
);

describe.sequential('Postgres Sync Bucket Storage Benchmarks', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe.skipIf(storageVersion !== 1)(`v${storageVersion}`, () => {
      register.registerStorageBenchmarks(
        { ...POSTGRES_STORAGE_FACTORY, storageVersion },
        {
          storageName: 'postgresql',
          storageVersion,
          scenarios,
          results
        }
      );
    });
  }
});
