import { migrations, storage } from '@powersync/service-core';
import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.transaction(async (db) => {
    await db.sql`
      ALTER TABLE sync_rules
      ADD COLUMN storage_version integer
    `.execute();

    await db.sql`
      UPDATE sync_rules
      SET
        storage_version = ${{ type: 'int4', value: storage.LEGACY_STORAGE_VERSION }}
      WHERE
        storage_version IS NULL
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
      DROP COLUMN storage_version
    `.execute();
  });
};
