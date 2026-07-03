import { storage } from '@powersync/service-core';
import { SlateDBBucketStorageFactory } from '../../src/storage/SlateDBBucketStorageFactory.js';

export const SLATEDB_STORAGE_FACTORY: storage.TestStorageConfig = {
  tableIdStrings: true,
  async factory() {
    return new SlateDBBucketStorageFactory({
      config: {
        type: 'slatedb',
        object_store: {
          type: 'memory'
        },
        db_path: `powersync-slatedb-storage-test-${crypto.randomUUID()}`
      },
      replicationStreamNamePrefix: 'test_'
    });
  }
};

export const TEST_STORAGE_VERSIONS = [3];
