import { storage } from '@powersync/service-core';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SlateDBBucketStorageFactory } from '../../src/storage/SlateDBBucketStorageFactory.js';

class TestSlateDBBucketStorageFactory extends SlateDBBucketStorageFactory {
  constructor(
    options: ConstructorParameters<typeof SlateDBBucketStorageFactory>[0],
    private readonly cleanupPath: string
  ) {
    super(options);
  }

  override async [Symbol.asyncDispose](): Promise<void> {
    await super[Symbol.asyncDispose]();
    await fs.rm(this.cleanupPath, { force: true, recursive: true });
  }
}

export const SLATEDB_STORAGE_FACTORY: storage.TestStorageConfig = {
  tableIdStrings: true,
  async factory() {
    const storagePath = await fs.mkdtemp(path.join(os.tmpdir(), 'powersync-slatedb-storage-test-'));

    return new TestSlateDBBucketStorageFactory(
      {
        config: {
          type: 'slatedb',
          path: storagePath
        },
        replicationStreamNamePrefix: 'test_'
      },
      storagePath
    );
  }
};

export const TEST_STORAGE_VERSIONS = [3];
