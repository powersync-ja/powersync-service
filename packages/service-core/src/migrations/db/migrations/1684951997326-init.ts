import { configFile } from '@powersync/service-types';
import * as mongo from '../../../db/mongo.js';
import * as storage from '../../../storage/storage-index.js';
import { PowerSyncMigrationFunction } from '../../PowerSyncMigrationManager.js';

export const up: PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const database = storage.createPowerSyncMongo(configuration.storage as configFile.MongoStorageConfig);
  await mongo.waitForAuth(database.db);
  try {
    await database.bucket_parameters.createIndex(
      {
        'key.g': 1,
        lookup: 1,
        _id: 1
      },
      { name: 'lookup1' }
    );
  } finally {
    await database.client.close();
  }
};

export const down: PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;

  const database = storage.createPowerSyncMongo(configuration.storage as configFile.MongoStorageConfig);
  try {
    if (await database.bucket_parameters.indexExists('lookup')) {
      await database.bucket_parameters.dropIndex('lookup1');
    }
  } finally {
    await database.client.close();
  }
};
