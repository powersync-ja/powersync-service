import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    await db.write_checkpoints.createIndex(
      {
        checkpoint_requested_at: 1
      },
      {
        name: 'checkpoint_requested_at',
        // Only client-requested checkpoints have this field; generated
        // checkpoints leave it unset. This keeps the index limited to the
        // documents the compact job's retention delete scans.
        partialFilterExpression: { checkpoint_requested_at: { $exists: true } }
      }
    );
    await db.custom_write_checkpoints.createIndex(
      {
        checkpoint_requested_at: 1
      },
      {
        name: 'checkpoint_requested_at',
        partialFilterExpression: { checkpoint_requested_at: { $exists: true } }
      }
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
    if (await db.write_checkpoints.indexExists('checkpoint_requested_at')) {
      await db.write_checkpoints.dropIndex('checkpoint_requested_at');
    }
    if (await db.custom_write_checkpoints.indexExists('checkpoint_requested_at')) {
      await db.custom_write_checkpoints.dropIndex('checkpoint_requested_at');
    }
  } finally {
    await db.client.close();
  }
};
