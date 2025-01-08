import { DatabaseClient } from './connection/DatabaseClient.js';

export const dropTables = async (client: DatabaseClient) => {
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
  });
};
