import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { DRIZZLE_SQLITE_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe('Sync Bucket Validation', register.registerBucketValidationTests);

for (let storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`Drizzle SQLite Sync Bucket Storage - Parameters - v${storageVersion}`, () =>
    register.registerDataStorageParameterTests({ ...DRIZZLE_SQLITE_STORAGE_FACTORY, storageVersion }));

  describe(`Drizzle SQLite Sync Bucket Storage - Data - v${storageVersion}`, () =>
    register.registerDataStorageDataTests({ ...DRIZZLE_SQLITE_STORAGE_FACTORY, storageVersion }));

  describe(`Drizzle SQLite Sync Bucket Storage - Checkpoints - v${storageVersion}`, () =>
    register.registerDataStorageCheckpointTests({ ...DRIZZLE_SQLITE_STORAGE_FACTORY, storageVersion }));

  describe(`Drizzle SQLite Sync Bucket Storage - Compaction - v${storageVersion}`, () => {
    register.registerCompactTests({ ...DRIZZLE_SQLITE_STORAGE_FACTORY, storageVersion });
    register.registerParameterCompactTests({ ...DRIZZLE_SQLITE_STORAGE_FACTORY, storageVersion });
  });
}
