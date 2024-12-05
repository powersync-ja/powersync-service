import { migrations } from '@powersync/service-core';
import { configFile } from '@powersync/service-types';
import * as storage from '../../../storage/storage-index.js';

const INDEX_NAME = 'user_sync_rule_unique';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as configFile.MongoStorageConfig);

  try {
    await db.custom_write_checkpoints.createIndex(
      {
        user_id: 1,
        sync_rules_id: 1
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

  const db = storage.createPowerSyncMongo(configuration.storage as configFile.MongoStorageConfig);

  try {
    if (await db.custom_write_checkpoints.indexExists(INDEX_NAME)) {
      await db.custom_write_checkpoints.dropIndex(INDEX_NAME);
    }
  } finally {
    await db.client.close();
  }
};
