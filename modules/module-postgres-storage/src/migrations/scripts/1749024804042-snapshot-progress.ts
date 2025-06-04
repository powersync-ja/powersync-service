import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`
      ALTER TABLE sync_rules
      ADD COLUMN snapshot_lsn TEXT
    `.execute();
    await db.sql`
      ALTER TABLE source_tables
      ADD COLUMN snapshot_total_estimated_count INTEGER,
      ADD COLUMN snapshot_replicated_count INTEGER,
      ADD COLUMN snapshot_last_key BYTEA
    `.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`
      ALTER TABLE sync_rules
      DROP COLUMN snapshot_lsn
    `.execute();
    await db.sql`
      ALTER TABLE source_tables
      DROP COLUMN snapshot_total_estimated_count,
      DROP COLUMN snapshot_replicated_count,
      DROP COLUMN snapshot_last_key
    `.execute();
  });
};
