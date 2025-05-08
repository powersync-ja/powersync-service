import { migrations } from '@powersync/service-core';

import { openMigrationDB } from '../migration-utils.js';

export const up: migrations.PowerSyncMigrationFunction = async (context) => {
  const {
    service_context: { configuration }
  } = context;
  await using client = openMigrationDB(configuration.storage);

  await client.transaction(async (db) => {
    // This ensures that op_id_sequence is initialized, so that
    //   SELECT nextval('op_id_sequence')
    // is always greater than
    //   SELECT LAST_VALUE FROM op_id_sequence
    // Without initializing, there is a case where last_value = 1, is_called = false, and the next call to nextval is also 1.
    // This call is harmless if the sequence was already initialized - it would only increment the sequence by 1.
    await db.sql`
      SELECT
        nextval('op_id_sequence');
    `.execute();
  });
};

export const down: migrations.PowerSyncMigrationFunction = async (context) => {
  // No-op - no need to revert the initialization when migrating down.
};
