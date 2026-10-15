import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

for (const storageVersion of TEST_STORAGE_VERSIONS) {
  describe(`storage v${storageVersion}`, () => {
    register.registerInvalidSyncConfigTests(INITIALIZED_MONGO_STORAGE_FACTORY.factory, storageVersion);
  });
}
