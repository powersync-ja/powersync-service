import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

/**
 * State for incremental parameter compaction.
 *
 * All replication streams share the `bucket_parameters` table and the `op_id_sequence`, so a single
 * operation-id boundary per stream is enough for each of these.
 *
 * `parameter_compacted_before` is the compaction cursor: an exclusive boundary through which the
 * stream's parameter entries have all been processed. It only advances once a pass completes.
 *
 * `parameter_reads_invalid_before` is the read fence: parameter history below it may have been
 * removed, so parameter queries cannot be evaluated at a checkpoint below it. It is raised before a
 * pass issues its first delete, so it is always at or ahead of the cursor.
 *
 * Existing streams start with NULL for both, treated as 0: their first pass compacts the full
 * retained history, and no checkpoint is fenced until it starts deleting.
 */
export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`
      ALTER TABLE sync_rules
      ADD COLUMN parameter_compacted_before BIGINT,
      ADD COLUMN parameter_reads_invalid_before BIGINT
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
      DROP COLUMN parameter_compacted_before,
      DROP COLUMN parameter_reads_invalid_before
    `.execute();
  });
};
