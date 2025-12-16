import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

/**
 * Migration: Add specialized index for bucket_data compaction query optimization
 *
 * This migration creates an index optimized for the compaction query in PostgresCompactor.ts
 * which uses COLLATE "C" (binary collation) and DESC ordering.
 * The index enables PostgreSQL to push COLLATE "C" conditions into Index Cond
 * instead of applying them as filters, significantly reducing rows scanned.
 */
export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`
      CREATE INDEX IF NOT EXISTS idx_bucket_data_compact ON bucket_data (
        group_id,
        bucket_name COLLATE "C" DESC,
        op_id DESC
      )
    `.execute();

    await db.sql`ANALYZE bucket_data`.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`DROP INDEX IF EXISTS idx_bucket_data_compact`.execute();
  });
};
