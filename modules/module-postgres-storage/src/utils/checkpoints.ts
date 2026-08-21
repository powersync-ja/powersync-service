import * as lib_postgres from '@powersync/lib-service-postgres';
import { storage } from '@powersync/service-core';
import { models } from '../types/types.js';

/**
 * Gets the latest active checkpoint document.
 * This is mainly exported for mocking in tests.
 */
export async function getActiveCheckpointDocument({
  db
}: {
  db: lib_postgres.DatabaseClient;
}): Promise<models.ActiveCheckpointDecoded | null> {
  return db.sql`
    SELECT
      id,
      last_checkpoint,
      last_checkpoint_lsn
    FROM
      sync_rules
    WHERE
      state = ${{ value: storage.SyncRuleState.ACTIVE, type: 'varchar' }}
      OR state = ${{ value: storage.SyncRuleState.ERRORED, type: 'varchar' }}
    ORDER BY
      id DESC
    LIMIT
      1
  `
    .decoded(models.ActiveCheckpoint)
    .first();
}
