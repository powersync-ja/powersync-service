import { migrations } from '@powersync/service-core';

import { dropTables } from '../../utils/db.js';
import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  /**
   * Request an explicit connection which will automatically set the search
   * path to the powersync schema
   */
  await client.lockConnection(async (db) => {
    await db.sql`
      CREATE SEQUENCE op_id_sequence AS int8 START
      WITH
        1
    `.execute();

    await db.sql`
      CREATE SEQUENCE sync_rules_id_sequence AS int START
      WITH
        1
    `.execute();

    await db.sql`
      CREATE TABLE bucket_data (
        group_id integer NOT NULL,
        bucket_name TEXT NOT NULL,
        op_id bigint NOT NULL,
        CONSTRAINT unique_id UNIQUE (group_id, bucket_name, op_id),
        op text NOT NULL,
        source_table TEXT,
        source_key bytea,
        table_name TEXT,
        row_id TEXT,
        checksum bigint NOT NULL,
        data TEXT,
        target_op bigint
      )
    `.execute();

    await db.sql`CREATE TABLE instance (id TEXT PRIMARY KEY) `.execute();

    await db.sql`
      CREATE TABLE sync_rules (
        id BIGSERIAL PRIMARY KEY,
        state TEXT NOT NULL,
        snapshot_done BOOLEAN NOT NULL DEFAULT FALSE,
        last_checkpoint BIGINT,
        last_checkpoint_lsn TEXT,
        no_checkpoint_before TEXT,
        slot_name TEXT,
        last_checkpoint_ts TIMESTAMP WITH TIME ZONE,
        last_keepalive_ts TIMESTAMP WITH TIME ZONE,
        keepalive_op TEXT,
        last_fatal_error TEXT,
        content TEXT NOT NULL
      );
    `.execute();

    await db.sql`
      CREATE TABLE bucket_parameters (
        id BIGINT DEFAULT nextval('op_id_sequence') PRIMARY KEY,
        group_id integer NOT NULL,
        source_table TEXT NOT NULL,
        source_key bytea NOT NULL,
        lookup bytea NOT NULL,
        bucket_parameters jsonb NOT NULL
      );
    `.execute();

    await db.sql`
      CREATE INDEX bucket_parameters_lookup_index ON bucket_parameters (group_id ASC, lookup ASC, id DESC)
    `.execute();

    await db.sql`
      CREATE INDEX bucket_parameters_source_index ON bucket_parameters (group_id, source_table, source_key)
    `.execute();

    await db.sql`
      CREATE TABLE current_data (
        group_id integer NOT NULL,
        source_table TEXT NOT NULL,
        source_key bytea NOT NULL,
        CONSTRAINT unique_current_data_id UNIQUE (group_id, source_table, source_key),
        buckets jsonb NOT NULL,
        data bytea NOT NULL,
        lookups bytea[] NOT NULL
      );
    `.execute();

    await db.sql`CREATE INDEX current_data_lookup ON current_data (group_id, source_table, source_key)`.execute();

    await db.sql`
      CREATE TABLE source_tables (
        --- This is currently a TEXT column to make the (shared) tests easier to integrate
        --- we could improve this if necessary
        id TEXT PRIMARY KEY,
        group_id integer NOT NULL,
        connection_id integer NOT NULL,
        relation_id text,
        schema_name text NOT NULL,
        table_name text NOT NULL,
        replica_id_columns jsonb,
        snapshot_done BOOLEAN NOT NULL DEFAULT FALSE
      )
    `.execute();

    await db.sql`CREATE INDEX source_table_lookup ON source_tables (group_id, table_name)`.execute();

    await db.sql`
      CREATE TABLE write_checkpoints (
        user_id text PRIMARY KEY,
        lsns jsonb NOT NULL,
        write_checkpoint BIGINT NOT NULL
      )
    `.execute();

    await db.sql`CREATE INDEX write_checkpoint_by_user ON write_checkpoints (user_id)`.execute();

    await db.sql`
      CREATE TABLE custom_write_checkpoints (
        user_id text NOT NULL,
        write_checkpoint BIGINT NOT NULL,
        sync_rules_id integer NOT NULL,
        CONSTRAINT unique_user_sync UNIQUE (user_id, sync_rules_id)
      );
    `.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await dropTables(client);
};
