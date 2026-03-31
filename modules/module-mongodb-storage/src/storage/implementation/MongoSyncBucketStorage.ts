import { HydratedSyncRules, ScopedParameterLookup, SqliteJsonRow } from '@powersync/service-sync-rules';
import {
  GetCheckpointChangesOptions,
  PopulateChecksumCacheOptions,
  PopulateChecksumCacheResults,
  storage,
  utils,
  WatchWriteCheckpointOptions
} from '@powersync/service-core';
import * as bson from 'bson';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { MongoPersistedSyncRulesContent } from './MongoPersistedSyncRulesContent.js';
import { BaseMongoSyncBucketStorage, MongoSyncBucketStorageOptions } from './common/MongoSyncBucketStorageBase.js';
import { MongoSyncBucketStorageV1 } from './v1/MongoSyncBucketStorageV1.js';
import { MongoSyncBucketStorageV3 } from './v3/MongoSyncBucketStorageV3.js';

export { MongoSyncBucketStorageOptions } from './common/MongoSyncBucketStorageBase.js';

export type MongoSyncBucketStorage = BaseMongoSyncBucketStorage;

export function createMongoSyncBucketStorage(
  factory: MongoBucketStorage,
  group_id: number,
  sync_rules: MongoPersistedSyncRulesContent,
  slot_name: string,
  writeCheckpointMode: storage.WriteCheckpointMode | undefined,
  options: MongoSyncBucketStorageOptions
): MongoSyncBucketStorage {
  if (sync_rules.getStorageConfig().incrementalReprocessing) {
    return new MongoSyncBucketStorageV3(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
  }

  return new MongoSyncBucketStorageV1(factory, group_id, sync_rules, slot_name, writeCheckpointMode, options);
}
