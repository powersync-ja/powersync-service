import { migrations } from '@powersync/service-core';
import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.sql`
    ALTER TABLE sync_rules
    ADD COLUMN sync_plan JSON;
  `.execute();
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.sql`ALTER TABLE sync_rules DROP COLUMN sync_plan`.execute();
};
