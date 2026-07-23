import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { MIKRO_ORM_SQLITE_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe('sync - MikroORM SQLite', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`storage v${storageVersion}`, () => {
      register.registerSyncTests(MIKRO_ORM_SQLITE_STORAGE_FACTORY.factory, {
        storageVersion,
        tableIdStrings: MIKRO_ORM_SQLITE_STORAGE_FACTORY.tableIdStrings
      });
    });
  }
});
