import { BucketStorageFactory } from '../storage/BucketStorage.js';

export type GeneratedStorage = {
  storage: BucketStorageFactory;
  disposer: () => Promise<void>;
};

export type BaseStorageConfig = {
  slot_name_prefix: string;
};

export interface StorageProvider<StorageConfig extends BaseStorageConfig = BaseStorageConfig> {
  // The storage type
  type: string;

  generate(config: StorageConfig): Promise<GeneratedStorage>;
}
