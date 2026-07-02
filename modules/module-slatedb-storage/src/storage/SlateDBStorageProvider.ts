import { storage } from '@powersync/service-core';

import { isSlateDBStorageConfig, SLATEDB_STORAGE_TYPE, SlateDBStorageConfig } from '../types/types.js';
import { SlateDBBucketStorageFactory } from './SlateDBBucketStorageFactory.js';
import { SlateDBReportStorage } from './SlateDBReportStorage.js';

export class SlateDBStorageProvider implements storage.StorageProvider {
  get type() {
    return SLATEDB_STORAGE_TYPE;
  }

  async getStorage(options: storage.GetStorageOptions): Promise<storage.ActiveStorage> {
    const { resolvedConfig } = options;
    const { storage } = resolvedConfig;

    if (!isSlateDBStorageConfig(storage)) {
      throw new Error(`Cannot create SlateDB bucket storage with provided config ${storage.type} !== ${this.type}`);
    }

    const decodedConfig = SlateDBStorageConfig.decode(storage);
    const storageFactory = new SlateDBBucketStorageFactory({
      config: decodedConfig,
      replicationStreamNamePrefix: resolvedConfig.slot_name_prefix
    });
    const reportStorage = new SlateDBReportStorage();

    return {
      reportStorage,
      storage: storageFactory,
      shutDown: async () => {
        await storageFactory[Symbol.asyncDispose]();
        await reportStorage[Symbol.asyncDispose]();
      },
      tearDown: async () => {
        await storageFactory[Symbol.asyncDispose]();
        await reportStorage[Symbol.asyncDispose]();
        return false;
      }
    } satisfies storage.ActiveStorage;
  }
}
