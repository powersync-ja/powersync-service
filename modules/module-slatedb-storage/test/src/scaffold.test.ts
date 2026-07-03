import { register } from '@powersync/service-core-tests';
import { describe } from 'vitest';
import { SLATEDB_STORAGE_FACTORY, TEST_STORAGE_VERSIONS } from './util.js';

describe('Sync Bucket Validation', register.registerBucketValidationTests);

for (const storageVersion of TEST_STORAGE_VERSIONS) {
  // describe(`SlateDB Sync Bucket Storage - Parameters - v${storageVersion}`, () =>
  //   register.registerDataStorageParameterTests({ ...SLATEDB_STORAGE_FACTORY, storageVersion }));

  describe(`SlateDB Sync Bucket Storage - Data - v${storageVersion}`, () =>
    register.registerDataStorageDataTests({
      ...SLATEDB_STORAGE_FACTORY,
      storageVersion,
      compressedBucketStorage: false
    }));

  // describe(`SlateDB Sync Bucket Storage - Checkpoints - v${storageVersion}`, () =>
  //   register.registerDataStorageCheckpointTests({ ...SLATEDB_STORAGE_FACTORY, storageVersion }));
}
