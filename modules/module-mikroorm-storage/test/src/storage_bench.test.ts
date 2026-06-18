import type { StorageBenchmarkResult } from '@powersync/service-core-tests';
import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { env } from './env.js';
import { MIKRO_ORM_MYSQL_STORAGE_FACTORY, MIKRO_ORM_SQLITE_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

const results: StorageBenchmarkResult[] = [];
register.registerStorageBenchmarkSummary(results);

describe.sequential('MikroORM SQLite Sync Bucket Storage Benchmarks', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`v${storageVersion}`, () => {
      register.registerStorageBenchmarks(
        { ...MIKRO_ORM_SQLITE_STORAGE_FACTORY, storageVersion },
        {
          storageName: 'mikroorm:sqlite',
          storageVersion,
          results
        }
      );
    });
  }
});

describe
  .skipIf(!env.MIKROORM_MYSQL_STORAGE_TEST_URI)
  .sequential('MikroORM MySQL Sync Bucket Storage Benchmarks', () => {
    for (const storageVersion of TEST_STORAGE_VERSIONS) {
      describe(`v${storageVersion}`, () => {
        register.registerStorageBenchmarks(
          { ...MIKRO_ORM_MYSQL_STORAGE_FACTORY, storageVersion },
          {
            storageName: 'mikroorm:mysql',
            storageVersion,
            results
          }
        );
      });
    }
  });
