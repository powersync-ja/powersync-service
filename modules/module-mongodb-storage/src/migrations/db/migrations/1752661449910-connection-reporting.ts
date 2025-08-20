import { migrations } from '@powersync/service-core';
import * as storage from '../../../storage/storage-index.js';
import { MongoStorageConfig } from '../../../types/types.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  const db = storage.createPowerSyncMongo(configuration.storage as MongoStorageConfig);

  try {
    await db.createConnectionReportingCollection();

    await db.connection_report_events.createIndex(
      {
        connected_at: 1,
        jwt_exp: 1,
        disconnected_at: 1
      },
      { name: 'connection_list_index' }
    );

    await db.connection_report_events.createIndex(
      {
        user_id: 1
      },
      { name: 'connection_user_id_index' }
    );
    await db.connection_report_events.createIndex(
      {
        client_id: 1
      },
      { name: 'connection_client_id_index' }
    );
    await db.connection_report_events.createIndex(
      {
        sdk: 1
      },
      { name: 'connection_index' }
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
    if (await db.connection_report_events.indexExists('connection_list_index')) {
      await db.connection_report_events.dropIndex('connection_list_index');
    }
    if (await db.connection_report_events.indexExists('connection_user_id_index')) {
      await db.connection_report_events.dropIndex('connection_user_id_index');
    }
    if (await db.connection_report_events.indexExists('connection_client_id_index')) {
      await db.connection_report_events.dropIndex('connection_client_id_index');
    }
    if (await db.connection_report_events.indexExists('connection_index')) {
      await db.connection_report_events.dropIndex('connection_index');
    }
    await db.db.dropCollection('connection_report_events');
  } finally {
    await db.client.close();
  }
};
