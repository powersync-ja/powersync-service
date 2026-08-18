import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { MIKRO_ORM_SQLITE_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe('Sync Bucket Validation', register.registerBucketValidationTests);

for (let storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`MikroORM SQLite Sync Bucket Storage - Parameters - v${storageVersion}`, () =>
    register.registerDataStorageParameterTests({ ...MIKRO_ORM_SQLITE_STORAGE_FACTORY, storageVersion }));

  describe(`MikroORM SQLite Sync Bucket Storage - Data - v${storageVersion}`, () =>
    register.registerDataStorageDataTests({ ...MIKRO_ORM_SQLITE_STORAGE_FACTORY, storageVersion }));

  describe(`MikroORM SQLite Sync Bucket Storage - Checkpoints - v${storageVersion}`, () =>
    register.registerDataStorageCheckpointTests({ ...MIKRO_ORM_SQLITE_STORAGE_FACTORY, storageVersion }));

  describe(`MikroORM SQLite Sync Bucket Storage - Compaction - v${storageVersion}`, () => {
    register.registerCompactTests({ ...MIKRO_ORM_SQLITE_STORAGE_FACTORY, storageVersion });
    register.registerParameterCompactTests({ ...MIKRO_ORM_SQLITE_STORAGE_FACTORY, storageVersion });
  });
}
