import { configFile } from '@powersync/service-types';

import * as storage from '../../../storage/storage-index.js';
import * as utils from '../../../util/util-index.js';

export const up = async (context: utils.MigrationContext) => {
  const { runner_config } = context;
  const config = await utils.loadConfig(runner_config);
  const db = storage.createPowerSyncMongo(config.storage as configFile.MongoStorageConfig);

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

export const down = async (context: utils.MigrationContext) => {
  const { runner_config } = context;
  const config = await utils.loadConfig(runner_config);

  const db = storage.createPowerSyncMongo(config.storage as configFile.MongoStorageConfig);

  try {
    if (await db.write_checkpoints.indexExists('user_id')) {
      await db.write_checkpoints.dropIndex('user_id');
    }
  } finally {
    await db.client.close();
  }
};
