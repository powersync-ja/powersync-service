import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

/**
 * Cursor for incremental parameter compaction.
 *
 * All replication streams share the `bucket_parameters` table and the `op_id_sequence`, so a single
 * exclusive operation-id boundary per stream is enough to record how far its parameter entries have
 * been compacted.
 *
 * Existing streams start with NULL, which is treated as 0: their first pass compacts the full
 * retained history.
 */
export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`
      ALTER TABLE sync_rules
      ADD COLUMN parameter_compacted_before BIGINT
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
      DROP COLUMN parameter_compacted_before
    `.execute();
  });
};
