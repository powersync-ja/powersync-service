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
        connected_at: 1,
        jwt_exp: 1,
        disconnected_at: 1
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
    if (await db.sdk_report_events.indexExists('sdk_list_index')) {
      await db.sdk_report_events.dropIndex('sdk_list_index');
    }
    if (await db.sdk_report_events.indexExists('sdk_user_id_index')) {
      await db.sdk_report_events.dropIndex('sdk_user_id_index');
    }
    if (await db.sdk_report_events.indexExists('sdk_client_id_index')) {
      await db.sdk_report_events.dropIndex('sdk_client_id_index');
    }
    if (await db.sdk_report_events.indexExists('sdk_index')) {
      await db.sdk_report_events.dropIndex('sdk_index');
    }
    await db.db.dropCollection('sdk_report_events');
  } finally {
    await db.client.close();
  }
};
