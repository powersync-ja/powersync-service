import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import { checkpointUserId, createWriteCheckpoint, TestStorageFactory } from '@powersync/service-core';
import { describe, test } from 'vitest';
import { describeWithStorage } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

import timers from 'node:timers/promises';

const BASIC_SYNC_RULES = `bucket_definitions:
  global:
    data:
      - SELECT id, description, other FROM "test_data"`;

describe('checkpoint tests', () => {
  describeWithStorage({}, checkpointTests);
});

const checkpointTests = (factory: TestStorageFactory) => {
  test('write checkpoints', { timeout: 50_000 }, async () => {
    await using context = await WalStreamTestContext.open(factory);

    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { pool } = context;
    const api = new PostgresRouteAPIAdapter(pool);
    const serverVersion = await context.connectionManager.getServerVersion();
    if (serverVersion!.compareMain('13.0.0') < 0) {
      // The test is not stable on Postgres 11 or 12. See the notes on
      // PostgresRouteAPIAdapter.createReplicationHead() for details.
      // Postgres 12 is already EOL, so not worth finding a fix - just skip the tests.
      console.log('Skipping write checkpoint test on Postgres < 13.0.0');
      return;
    }

    await pool.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);

    await context.replicateSnapshot();

    context.startStreaming();
    // Wait for a consistent checkpoint before we start.
    await context.getCheckpoint();
    const storage = context.storage!;

    const controller = new AbortController();
    try {
      const stream = storage.watchCheckpointChanges({
        user_id: checkpointUserId('test_user', 'test_client'),
        signal: controller.signal
      });

      let lastWriteCheckpoint: bigint | null = null;

      (async () => {
        try {
          for await (const cp of stream) {
            lastWriteCheckpoint = cp.writeCheckpoint;
          }
        } catch (e) {
          if (e.name != 'AbortError') {
            throw e;
          }
        }
      })();

      for (let i = 0; i < 10; i++) {
        const cp = await createWriteCheckpoint({
          userId: 'test_user',
          clientId: 'test_client',
          api,
          storage: context.factory
        });

        const start = Date.now();
        while (lastWriteCheckpoint == null || lastWriteCheckpoint < BigInt(cp.writeCheckpoint)) {
          if (Date.now() - start > 3_000) {
            throw new Error(
              `Timeout while waiting for checkpoint. last: ${lastWriteCheckpoint}, waiting for: ${cp.writeCheckpoint}`
            );
          }
          await timers.setTimeout(5, undefined, { signal: controller.signal });
        }
      }
    } finally {
      controller.abort();
    }
  });
};
