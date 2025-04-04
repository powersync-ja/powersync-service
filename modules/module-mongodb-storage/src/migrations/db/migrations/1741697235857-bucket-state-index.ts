import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

const INDEX_NAME = 'bucket_updates';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    await db.bucket_state.createIndex(
      {
        '_id.g': 1,
        last_op: 1
      },
      { name: INDEX_NAME, unique: true }
    );
  } finally {
    await db.client.close();
  }
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;

  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    if (await db.bucket_state.indexExists(INDEX_NAME)) {
      await db.bucket_state.dropIndex(INDEX_NAME);
    }
  } finally {
    await db.client.close();
  }
};
