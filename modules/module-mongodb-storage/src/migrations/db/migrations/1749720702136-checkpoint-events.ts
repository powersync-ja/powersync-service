import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    await db.createCheckpointEventsCollection();

    await db.write_checkpoints.createIndex(
      {
        processed_at_lsn: 1
      },
      { name: 'processed_at_lsn' }
    );

    await db.custom_write_checkpoints.createIndex(
      {
        op_id: 1
      },
      { name: 'op_id' }
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
    if (await db.write_checkpoints.indexExists('processed_at_lsn')) {
      await db.write_checkpoints.dropIndex('processed_at_lsn');
    }
    if (await db.custom_write_checkpoints.indexExists('op_id')) {
      await db.custom_write_checkpoints.dropIndex('op_id');
    }
    await db.db.dropCollection('checkpoint_events');
  } finally {
    await db.client.close();
  }
};
