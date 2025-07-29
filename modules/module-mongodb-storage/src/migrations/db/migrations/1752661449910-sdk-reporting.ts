import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    await db.createSdkReportingCollection();

    await db.sdk_report_events.createIndex(
      {
        connect_at: 1,
        jwt_exp: 1,
        disconnect_at: 1
      },
      { name: 'sdk_list_index' }
    );

    await db.sdk_report_events.createIndex(
      {
        user_id: 1
      },
      { name: 'sdk_user_id_index' }
    );
    await db.sdk_report_events.createIndex(
      {
        client_id: 1
      },
      { name: 'sdk_client_id_index' }
    );
    await db.sdk_report_events.createIndex(
      {
        sdk: 1
      },
      { name: 'sdk_index' }
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
    if (await db.write_checkpoints.indexExists('connect_at')) {
      await db.write_checkpoints.dropIndex('connect_at');
    }
    if (await db.custom_write_checkpoints.indexExists('user_id')) {
      await db.custom_write_checkpoints.dropIndex('user_id');
    }
    if (await db.custom_write_checkpoints.indexExists('client_id')) {
      await db.custom_write_checkpoints.dropIndex('client_id');
    }
    await db.db.dropCollection('sdk_report_events');
  } finally {
    await db.client.close();
  }
};
