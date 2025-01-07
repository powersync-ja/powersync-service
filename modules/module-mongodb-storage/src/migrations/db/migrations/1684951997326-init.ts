import * as lib_mongo from '@powersync/lib-service-mongodb';
import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const database = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);
  await lib_mongo.waitForAuth(database.db);
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

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;

  const database = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);
  try {
    if (await database.bucket_parameters.indexExists('lookup')) {
      await database.bucket_parameters.dropIndex('lookup1');
    }
  } finally {
    await database.client.close();
  }
};
