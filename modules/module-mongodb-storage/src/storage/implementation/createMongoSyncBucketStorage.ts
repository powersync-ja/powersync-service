import { storage } from '@powersync/service-core';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { MongoPersistedSyncRulesContent } from './MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';
import { MongoSyncBucketStorageV1 } from './v1/MongoSyncBucketStorageV1.js';
import { MongoSyncBucketStorageV3 } from './v3/MongoSyncBucketStorageV3.js';
import { MongoSyncBucketStorageV5 } from './v5/MongoSyncBucketStorageV5.js';

export { MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';

export type { MongoSyncBucketStorage };

export function createMongoSyncBucketStorage(
  factory: MongoBucketStorage,
  group_id: number,
  sync_rules: MongoPersistedSyncRulesContent,
  slot_name: string,
  writeCheckpointMode: storage.WriteCheckpointMode | undefined,
  options: MongoSyncBucketStorageOptions
): MongoSyncBucketStorage {
  const storageConfig = sync_rules.getStorageConfig();
  if (storageConfig.compressedBucketStorage) {
    return new MongoSyncBucketStorageV5(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
  }

  if (storageConfig.incrementalReprocessing) {
    return new MongoSyncBucketStorageV3(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
  }

  return new MongoSyncBucketStorageV1(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
}
