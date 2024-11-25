import { configFile } from '@powersync/service-types';
import * as mongo from '../../../db/mongo.js';
import * as storage from '../../../storage/storage-index.js';
import * as utils from '../../../util/util-index.js';

export const up = async (context: utils.MigrationContext) => {
  const { runner_config } = context;

  const config = await utils.loadConfig(runner_config);

  const database = storage.createPowerSyncMongo(config.storage as configFile.MongoStorageConfig);
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

export const down = async (context: utils.MigrationContext) => {
  const { runner_config } = context;
  const config = await utils.loadConfig(runner_config);

  const database = storage.createPowerSyncMongo(config.storage as configFile.MongoStorageConfig);
  try {
    if (await database.bucket_parameters.indexExists('lookup')) {
      await database.bucket_parameters.dropIndex('lookup1');
    }
  } finally {
    await database.client.close();
  }
};
