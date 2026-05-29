import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  // Per-table flag for storing a copy of replicated rows in current_data. Nullable, with NULL
  // treated as true (the safe default) until resolveTables populates it.
  await client.sql`
    ALTER TABLE source_tables
    ADD COLUMN store_current_data BOOLEAN
  `.execute();
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.sql`
    ALTER TABLE source_tables
    DROP COLUMN store_current_data
  `.execute();
};
