import { migrations } from '@powersync/service-core';
import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.sql`
    ALTER TABLE source_tables
    ALTER COLUMN snapshot_total_estimated_count TYPE BIGINT,
    ALTER COLUMN snapshot_replicated_count TYPE BIGINT,
    ADD COLUMN snapshot_updated_at TIMESTAMPTZ,
    ADD COLUMN snapshot_replication_rate DOUBLE PRECISION
  `.execute();
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  // The counts stay BIGINT: older versions already decode them as bigint, and narrowing them could fail for large
  // tables.
  await client.sql`
    ALTER TABLE source_tables
    DROP COLUMN snapshot_updated_at,
    DROP COLUMN snapshot_replication_rate
  `.execute();
};
