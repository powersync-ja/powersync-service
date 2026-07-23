import type { StorageBenchmarkResult } from '@powersync/service-core-tests';
import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { POSTGRES_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const results: StorageBenchmarkResult[] = [];
register.registerStorageBenchmarkSummary(results);

describe.sequential('Postgres Sync Bucket Storage Benchmarks', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`v${storageVersion}`, () => {
      register.registerStorageBenchmarks(
        { ...POSTGRES_STORAGE_FACTORY, storageVersion },
        {
          storageName: 'postgresql',
          storageVersion,
          results
        }
      );
    });
  }
});
