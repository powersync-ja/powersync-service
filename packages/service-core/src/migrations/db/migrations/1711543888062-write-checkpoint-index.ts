import { configFile } from '@powersync/service-types';

import * as storage from '../../../storage/storage-index.js';
import { PowerSyncMigrationFunction } from '../../PowerSyncMigrationManager.js';

export const up: PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as configFile.MongoStorageConfig);

  try {
    await db.write_checkpoints.createIndex(
      {
        user_id: 1
      },
      { name: 'user_id' }
    );
  } finally {
    await db.client.close();
  }
};

export const down: PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;

  const db = storage.createPowerSyncMongo(configuration.storage as configFile.MongoStorageConfig);

  try {
    if (await db.write_checkpoints.indexExists('user_id')) {
      await db.write_checkpoints.dropIndex('user_id');
    }
  } finally {
    await db.client.close();
  }
};
