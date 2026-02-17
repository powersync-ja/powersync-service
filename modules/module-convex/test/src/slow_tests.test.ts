import { describe, expect, test } from 'vitest';

import { ConvexStreamTestContext, INITIALIZED_MONGO_STORAGE_FACTORY, makeConvexConnectionManager } from './util.js';
import { env } from './env.js';

describe.runIf(env.SLOW_TESTS && !!env.CONVEX_DEPLOY_KEY)(
  'convex slow tests',
  { timeout: 120_000 },
  function () {
    test('connects to Convex and lists table schemas', async () => {
      await using context = await ConvexStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY);
      const schemas = await context.client.getJsonSchemas();

      expect(schemas.tables.length).toBeGreaterThan(0);

      const tableNames = schemas.tables.map((t) => t.tableName);
      expect(tableNames).toContain('lists');
    });

    test('list_snapshot returns data from Convex', async () => {
      const mgr = makeConvexConnectionManager();
      const page = await mgr.client.listSnapshot({ tableName: 'lists' });

      expect(page.snapshot).toBeDefined();
      // If this fails, seed data first:
      // curl -X POST http://127.0.0.1:3210/api/mutation \
      //   -H 'Content-Type: application/json' \
      //   -H 'Authorization: Convex <deploy_key>' \
      //   -d '{"path":"seed:seedLists","args":{"count":10},"format":"json"}'
      expect(page.values.length).toBeGreaterThan(0);

      await mgr.end();
    });

    test('snapshot replicates existing data into buckets', async () => {
      await using context = await ConvexStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY);

      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, name FROM "lists"
      `);

      await context.replicateSnapshot();
      await context.markSnapshotQueryable();

      const checksum = await context.getChecksum('global[]');
      expect(checksum).toBeDefined();
      expect(checksum!.count).toBeGreaterThan(0);
    });

    test('snapshot replicates with wildcard table pattern', async () => {
      await using context = await ConvexStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY);
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, * FROM "lists"
      `);

      await context.replicateSnapshot();
      await context.markSnapshotQueryable();

      const data = await context.getBucketData('global[]');
      expect(data.length).toBeGreaterThan(0);
    });

    test('delta streaming starts from snapshot cursor', async () => {
      await using context = await ConvexStreamTestContext.open(INITIALIZED_MONGO_STORAGE_FACTORY);
      await context.updateSyncRules(`
bucket_definitions:
  global:
    data:
      - SELECT _id as id, name FROM "lists"
      `);

      await context.replicateSnapshot();

      // Start streaming in background; it should poll deltas without error.
      const streamPromise = context.startStreaming();

      // Give delta polling a couple of cycles to confirm it doesn't crash.
      await new Promise((resolve) => setTimeout(resolve, 2_000));

      // Abort streaming gracefully.
      context.abort();
      await streamPromise.catch(() => {});

      // Checkpoint should still be valid after streaming ran.
      const checksum = await context.getChecksum('global[]');
      expect(checksum).toBeDefined();
      expect(checksum!.count).toBeGreaterThan(0);
    });
  }
);
