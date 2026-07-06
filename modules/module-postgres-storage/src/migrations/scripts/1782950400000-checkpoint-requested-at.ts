import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    await db.sql`
      ALTER TABLE write_checkpoints
      ADD COLUMN checkpoint_requested_at TIMESTAMP WITH TIME ZONE
    `.execute();
    await db.sql`
      ALTER TABLE custom_write_checkpoints
      ADD COLUMN checkpoint_requested_at TIMESTAMP WITH TIME ZONE
    `.execute();
    await db.sql`
      CREATE INDEX write_checkpoints_checkpoint_requested_at ON write_checkpoints (checkpoint_requested_at)
      WHERE
        checkpoint_requested_at IS NOT NULL
    `.execute();
    await db.sql`
      CREATE INDEX custom_write_checkpoints_checkpoint_requested_at ON custom_write_checkpoints (checkpoint_requested_at)
      WHERE
        checkpoint_requested_at IS NOT NULL
    `.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    // Also drops the write_checkpoints_checkpoint_requested_at index.
    await db.sql`
      ALTER TABLE write_checkpoints
      DROP COLUMN checkpoint_requested_at
    `.execute();
    // Also drops the custom_write_checkpoints_checkpoint_requested_at index.
    await db.sql`
      ALTER TABLE custom_write_checkpoints
      DROP COLUMN checkpoint_requested_at
    `.execute();
  });
};
