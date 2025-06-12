import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    // We cover the case where the replication process was started before running this migration.
    const existingCollections = await db.db
      .listCollections({ name: 'checkpoint_events' }, { nameOnly: false })
      .toArray();
    const collection = existingCollections[0];
    if (collection != null) {
      if (!collection.options?.capped) {
        // Collection was auto-created but not capped, so we need to drop it
        await db.db.dropCollection('checkpoint_events');
      } else {
        // Collection previously created somehow - ignore
        return;
      }
    }

    await db.db.createCollection('checkpoint_events', {
      capped: true,
      size: 10 * 1024, // 10 KB
      max: 10 // 10 documents
    });
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
    await db.db.dropCollection('checkpoint_events');
  } finally {
    await db.client.close();
  }
};
