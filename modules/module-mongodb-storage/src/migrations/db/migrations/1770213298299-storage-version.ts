import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    await db.sync_rules.updateMany(
      { storage_version: { $exists: false } },
      { $set: { storage_version: storage.LEGACY_STORAGE_VERSION } }
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
    const newRules = await db.sync_rules.find({ storage_version: { $gt: storage.LEGACY_STORAGE_VERSION } }).toArray();
    if (newRules.length > 0) {
      throw new Error(
        `Cannot revert migration due to newer storage versions in use: ${newRules.map((r) => `${r._id}: v${r.storage_version}`).join(', ')}`
      );
    }
    await db.sync_rules.updateMany(
      { storage_version: storage.LEGACY_STORAGE_VERSION },
      { $unset: { storage_version: 1 } }
    );
  } finally {
    await db.client.close();
  }
};
