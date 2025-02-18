import { PostgresRouteAPIAdapter } from '@module/api/PostgresRouteAPIAdapter.js';
import { checkpointUserId, createWriteCheckpoint } from '@powersync/service-core';
import { describe, test } from 'vitest';
import { INITIALIZED_MONGO_STORAGE_FACTORY } from './util.js';
import { WalStreamTestContext } from './wal_stream_utils.js';

import timers from 'node:timers/promises';

const BASIC_SYNC_RULES = `bucket_definitions:
  global:
    data:
      - SELECT id, description, other FROM "test_data"`;

describe('checkpoint tests', () => {
  test('write checkpoints', { timeout: 30_000 }, async () => {
    const factory = INITIALIZED_MONGO_STORAGE_FACTORY;
    await using context = await WalStreamTestContext.open(factory);

    await context.updateSyncRules(BASIC_SYNC_RULES);
    const { pool } = context;
    const api = new PostgresRouteAPIAdapter(pool);

    await pool.query(`CREATE TABLE test_data(id text primary key, description text, other text)`);

    await context.replicateSnapshot();

    context.startStreaming();
    const storage = context.storage!;

    const controller = new AbortController();
    try {
      const stream = storage.watchWriteCheckpoint({
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
          if (Date.now() - start > 2_000) {
            throw new Error(`Timeout while waiting for checkpoint`);
          }
          await timers.setTimeout(0, undefined, { signal: controller.signal });
        }
      }
    } finally {
      controller.abort();
    }
  });
});
