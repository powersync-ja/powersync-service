import { migrations } from '@powersync/service-core';
import { MongoBucketStorage, createPowerSyncMongo } from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async ({ service_context: { configuration } }) => {
  const db = createPowerSyncMongo(configuration.storage as MongoStorageConfig);
  try {
    const factory = new MongoBucketStorage(db, { replicationStreamNamePrefix: configuration.slot_name_prefix });
    await factory.dropUnusedV1StorageCollections();
  } finally {
    await db.client.close();
  }
};

export const down: migrations.PowerSyncMigrationFunction = async ({ service_context: { configuration } }) => {
  const db = createPowerSyncMongo(configuration.storage as MongoStorageConfig);
  try {
    await db.initializeV1StorageCollections();
  } finally {
    await db.client.close();
  }
};
