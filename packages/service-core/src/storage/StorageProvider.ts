import { BucketStorageFactory } from './BucketStorage.js';
import * as util from '../util/util-index.js';

export interface ActiveStorage {
  storage: BucketStorageFactory;
  shutDown(): Promise<void>;

  /**
   *  Drop/Dispose of the storage permanently
   */
  dispose(): Promise<boolean>;
}

export interface GetStorageOptions {
  // TODO: This should just be the storage config. Update once the slot name prefix coupling has been removed from the storage
  resolvedConfig: util.ResolvedPowerSyncConfig;
}

export interface BucketStorageProvider {
  /**
   *  The storage type that this provider provides.
   *  The type should match the `type` field in the config.
   */
  type: string;

  getStorage(options: GetStorageOptions): Promise<ActiveStorage>;
}
