import * as postgres_storage from '@powersync/service-module-postgres-storage';
import { describe, expect, test } from 'vitest';
import { env } from './env.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

describe.skipIf(!env.TEST_POSTGRES_STORAGE)('replication storage combination - postgres', function () {
  test('should allow the same Postgres cluster to be used for data and storage', async () => {
    // Use the same cluster for the storage as the data source
    await using context = await WalStreamTestContext.open(
      postgres_storage.PostgresTestStorageFactoryGenerator({
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

    await context.replicateSnapshot();

    // Perhaps we should check and throw when replicating the snapshot or somewhere else earlier.
    context.startStreaming();

    if (sourceVersion!.compareMain('14.0.0') < 0) {
      console.log('waiting for throw');
      await expect(context.waitForStream()).rejects.toThrow();
      console.log('done with throw');
    } else {
      await expect(context.waitForStream()).resolves.toReturn();
    }
  });
});
