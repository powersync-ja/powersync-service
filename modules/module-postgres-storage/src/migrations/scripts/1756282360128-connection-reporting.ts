import { migrations } from '@powersync/service-core';
import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.transaction(async (db) => {
    await db.sql`
      CREATE TABLE connection_report_events (
        id TEXT PRIMARY KEY,
        user_agent TEXT NOT NULL,
        client_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        sdk TEXT NOT NULL,
        jwt_exp TIMESTAMP WITH TIME ZONE,
        connected_at TIMESTAMP WITH TIME ZONE NOT NULL,
        disconnected_at TIMESTAMP WITH TIME ZONE
      )
    `.execute();

    await db.sql`
      CREATE INDEX sdk_list_index ON connection_report_events (connected_at, jwt_exp, disconnected_at)
    `.execute();

    await db.sql`CREATE INDEX sdk_user_id_index ON connection_report_events (user_id)`.execute();

    await db.sql`CREATE INDEX sdk_client_id_index ON connection_report_events (client_id)`.execute();

    await db.sql`CREATE INDEX sdk_index ON connection_report_events (sdk)`.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.sql`DROP TABLE IF EXISTS connection_report_events`.execute();
};
