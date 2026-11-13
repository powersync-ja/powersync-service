import { migrations } from '@powersync/service-core';
import { openMigrationDB } from '../migration-utils.js';

/**
 * Up to this size of bucket_data, the index is created during the migration. Without CONCURRENTLY, this blocks writes
 * to bucket_data while it is built. Larger storage keeps working without it, and PostgresSyncRulesStorage logs a
 * warning with the statement to create it manually.
 */
const OP_ID_INDEX_MAX_TABLE_BYTES = 64 * 1024 * 1024;

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.sql`ALTER TABLE sync_rules ADD COLUMN checkpoint_changes_invalid_before BIGINT`.execute();

  const row = await client.sql`SELECT pg_relation_size('bucket_data') AS size`.first<{ size: bigint | number }>();
  if (Number(row?.size ?? 0) <= OP_ID_INDEX_MAX_TABLE_BYTES) {
    // Same name as BUCKET_DATA_OP_ID_INDEX in PostgresSyncRulesStorage.
    await client.sql`CREATE INDEX IF NOT EXISTS bucket_data_op_id_index ON bucket_data (group_id, op_id)`.execute();
  }
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.sql`DROP INDEX IF EXISTS bucket_data_op_id_index`.execute();
  await client.sql`ALTER TABLE sync_rules DROP COLUMN checkpoint_changes_invalid_before`.execute();
};
