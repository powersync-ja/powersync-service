import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { DRIZZLE_SQLITE_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe('sync - Drizzle SQLite', () => {
  for (const storageVersion of TEST_STORAGE_VERSIONS) {
    describe(`storage v${storageVersion}`, () => {
      register.registerSyncTests(DRIZZLE_SQLITE_STORAGE_FACTORY.factory, {
        storageVersion,
        tableIdStrings: DRIZZLE_SQLITE_STORAGE_FACTORY.tableIdStrings
      });
    });
  }
});
