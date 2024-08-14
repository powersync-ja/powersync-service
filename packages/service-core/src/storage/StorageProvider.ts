import { BucketStorageFactory } from '../storage/BucketStorage.js';

import * as util from '../util/util-index.js';

export type GeneratedStorage = {
  storage: BucketStorageFactory;
  disposer: () => Promise<void>;
};

export type StorageGenerationParams = {
  resolved_config: util.ResolvedPowerSyncConfig;
};

export interface BucketStorageProvider {
  // The storage type which should match the `type` field in the config
  type: string;

  generate(storageConfig: StorageGenerationParams): Promise<GeneratedStorage>;
}
