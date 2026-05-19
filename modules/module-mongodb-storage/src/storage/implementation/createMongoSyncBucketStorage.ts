import { storage } from '@powersync/service-core';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { MongoPersistedSyncRulesContentV1 } from './MongoPersistedSyncRulesContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';
import { MongoSyncBucketStorageV1 } from './v1/MongoSyncBucketStorageV1.js';
import { MongoSyncBucketStorageV3 } from './v3/MongoSyncBucketStorageV3.js';

export { MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';

export type { MongoSyncBucketStorage };

export function createMongoSyncBucketStorage(
  factory: MongoBucketStorage,
  group_id: number,
  sync_rules: MongoPersistedSyncRulesContentV1,
  slot_name: string,
  writeCheckpointMode: storage.WriteCheckpointMode | undefined,
  options: MongoSyncBucketStorageOptions
): MongoSyncBucketStorage {
  if (sync_rules.getStorageConfig().incrementalReprocessing) {
    return new MongoSyncBucketStorageV3(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
  }

  return new MongoSyncBucketStorageV1(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
}
