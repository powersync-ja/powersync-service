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
      ADD COLUMN storage_version integer NOT NULL DEFAULT 1
    `.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);
  await client.transaction(async (db) => {
    const newRules = await db.sql`
      SELECT
        id,
        storage_version
      FROM
        sync_rules
      WHERE
        storage_version > ${{ type: 'int4', value: storage.LEGACY_STORAGE_VERSION }}
    `.rows<{ id: number | bigint; storage_version: number | bigint }>();

    if (newRules.length > 0) {
      throw new Error(
        `Cannot revert migration due to newer storage versions in use: ${newRules.map((r) => `${r.id}: v${r.storage_version}`).join(', ')}`
      );
    }

    await db.sql`
      ALTER TABLE sync_rules
      DROP COLUMN storage_version
    `.execute();
  });
};
