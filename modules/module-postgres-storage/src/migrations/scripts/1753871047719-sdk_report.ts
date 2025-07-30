import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`
      CREATE TABLE IF NOT EXISTS sdk_report_events (
        id TEXT PRIMARY KEY,
        user_agent TEXT NOT NULL,
        client_id TEXT NOT NULL,
        user_id TEXT NOT NULL,
        sdk TEXT NOT NULL,
        jwt_exp TIMESTAMP WITH TIME ZONE,
        connect_at TIMESTAMP WITH TIME ZONE NOT NULL,
        disconnect_at TIMESTAMP WITH TIME ZONE
      )
    `.execute();

    await db.sql`
      CREATE INDEX IF NOT EXISTS sdk_list_index ON sdk_report_events (connect_at, jwt_exp, disconnect_at)
    `.execute();

    await db.sql`CREATE INDEX IF NOT EXISTS sdk_user_id_index ON sdk_report_events (user_id)`.execute();

    await db.sql`CREATE INDEX IF NOT EXISTS sdk_client_id_index ON sdk_report_events (client_id)`.execute();

    await db.sql`CREATE INDEX IF NOT EXISTS sdk_index ON sdk_report_events (sdk)`.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  client.lockConnection(async (db) => {
    await db.sql`DROP TABLE IF EXISTS sdk_report_events`.execute();
  });
};
