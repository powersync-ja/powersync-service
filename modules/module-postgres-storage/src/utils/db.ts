import * as lib_postgres from '@powersync/lib-service-postgres';

export const STORAGE_SCHEMA_NAME = 'powersync';

export const NOTIFICATION_CHANNEL = 'powersynccheckpoints';

/**
 * Re export for prettier to detect the tag better
 */
export const sql = lib_postgres.sql;

/**
 * Drop all Postgres storage tables used by the service, including migrations.
 */
export const dropTables = async (client: lib_postgres.DatabaseClient) => {
  // Lock a connection for automatic schema search paths
  await client.lockConnection(async (db) => {
    await db.sql`DROP TABLE IF EXISTS bucket_data`.execute();
    await db.sql`DROP TABLE IF EXISTS bucket_parameters`.execute();
    await db.sql`DROP TABLE IF EXISTS sync_rules`.execute();
    await db.sql`DROP TABLE IF EXISTS instance`.execute();
    await db.sql`DROP TABLE IF EXISTS bucket_data`.execute();
    await db.sql`DROP TABLE IF EXISTS current_data`.execute();
    await db.sql`DROP TABLE IF EXISTS source_tables`.execute();
    await db.sql`DROP TABLE IF EXISTS write_checkpoints`.execute();
    await db.sql`DROP TABLE IF EXISTS custom_write_checkpoints`.execute();
    await db.sql`DROP SEQUENCE IF EXISTS op_id_sequence`.execute();
    await db.sql`DROP SEQUENCE IF EXISTS sync_rules_id_sequence`.execute();
    await db.sql`DROP TABLE IF EXISTS migrations`.execute();
  });
};

/**
 * Clear all Postgres storage tables and reset sequences.
 *
 * Does not clear migration state.
 */
export const truncateTables = async (db: lib_postgres.DatabaseClient) => {
  // Lock a connection for automatic schema search paths
  await db.query(
    {
      statement: `TRUNCATE TABLE bucket_data,
        bucket_parameters,
        sync_rules,
        instance,
        current_data,
        source_tables,
        write_checkpoints,
        custom_write_checkpoints,
        connection_report_events RESTART IDENTITY CASCADE
    `
    },
    {
      statement: `ALTER SEQUENCE IF EXISTS op_id_sequence RESTART
      WITH
        1`
    },
    {
      statement: `ALTER SEQUENCE IF EXISTS sync_rules_id_sequence RESTART
      WITH
        1`
    }
  );
};
