import { migrations } from '@powersync/service-core';
import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.transaction(async (db) => {
    await db.sql`
      ALTER TABLE current_data
      ADD COLUMN pending_delete BIGINT NULL
    `.execute();
    await db.sql`
      CREATE INDEX IF NOT EXISTS current_data_pending_deletes ON current_data (group_id, pending_delete)
      WHERE
        pending_delete IS NOT NULL
    `.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.transaction(async (db) => {
    await db.sql`DROP INDEX IF EXISTS current_data_pending_deletes`.execute();
    await db.sql`
      ALTER TABLE current_data
      DROP COLUMN pending_delete
    `.execute();
  });
};
