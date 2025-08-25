import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

describe.skipIf(!env.TEST_POSTGRES_STORAGE)('replication storage combination - postgres', function () {
  test('should allow the same Postgres cluster to be used for data and storage', async () => {
    // Use the same cluster for the storage as the data source
    await using context = await WalStreamTestContext.open(
      postgres_storage.test_utils.postgresTestStorageFactoryGenerator({
        url: env.PG_TEST_URL
      }),
      { doNotClear: false }
    );

    await context.updateSyncRules(/* yaml */
    ` bucket_definitions:
        global:
          data:
            - SELECT * FROM "test_data" `);

    const { pool, connectionManager } = context;

    const sourceVersion = await connectionManager.getServerVersion();

    await pool.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);

    if (sourceVersion!.compareMain('14.0.0') < 0) {
      await expect(context.replicateSnapshot()).rejects.toThrow();
    } else {
      // Should resolve
      await context.replicateSnapshot();
    }
  });
});
