import { storage } from '@powersync/service-core';
import { MongoBucketStorage } from '../MongoBucketStorage.js';
import { MongoPersistedSyncConfigContentV1 } from './MongoPersistedSyncConfigContent.js';
import { MongoSyncBucketStorage, MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';
import { MongoSyncBucketStorageV1 } from './v1/MongoSyncBucketStorageV1.js';
import { MongoSyncBucketStorageV3 } from './v3/MongoSyncBucketStorageV3.js';

export { MongoSyncBucketStorageOptions } from './MongoSyncBucketStorage.js';

export type { MongoSyncBucketStorage };

export function createMongoSyncBucketStorage(
  factory: MongoBucketStorage,
  replicationStreamId: number,
  sync_rules: MongoPersistedSyncConfigContentV1,
  replicationStreamName: string,
  writeCheckpointMode: storage.WriteCheckpointMode | undefined,
  options: MongoSyncBucketStorageOptions
): MongoSyncBucketStorage {
  if (sync_rules.getStorageConfig().incrementalReprocessing) {
    return new MongoSyncBucketStorageV3(
      factory,
      replicationStreamId,
      sync_rules,
      replicationStreamName,
      writeCheckpointMode,
      options
    );
  }

  return new MongoSyncBucketStorageV1(
    factory,
    replicationStreamId,
    sync_rules,
    replicationStreamName,
    writeCheckpointMode,
    options
  );
}
