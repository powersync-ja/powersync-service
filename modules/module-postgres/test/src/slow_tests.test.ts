import * as bson from 'bson';
import { afterEach, describe, expect, test } from 'vitest';
import { WalStream, WalStreamOptions } from '../../src/replication/WalStream.js';
import { env } from './env.js';
import {
  clearTestDb,
  connectPgPool,
  getClientCheckpoint,
  INITIALIZED_MONGO_STORAGE_FACTORY,
  INITIALIZED_POSTGRES_STORAGE_FACTORY,
  TEST_CONNECTION_OPTIONS
} from './util.js';

import * as pgwire from '@powersync/service-jpgwire';
import { SqliteRow } from '@powersync/service-sync-rules';

import { PgManager } from '@module/replication/PgManager.js';
import { storage } from '@powersync/service-core';
import { test_utils } from '@powersync/service-core-tests';
import * as mongo_storage from '@powersync/service-module-mongodb-storage';
import * as postgres_storage from '@powersync/service-module-postgres-storage';
import * as timers from 'node:timers/promises';

describe.skipIf(!env.TEST_MONGO_STORAGE)('slow tests - mongodb', function () {
  // These are slow, inconsistent tests.
  // Not run on every test run, but we do run on CI, or when manually debugging issues.
  if (env.CI || env.SLOW_TESTS) {
    defineSlowTests(INITIALIZED_MONGO_STORAGE_FACTORY);
  } else {
    // Need something in this file.
    test('no-op', () => {});
  }
});

describe.skipIf(!env.TEST_POSTGRES_STORAGE)('slow tests - postgres', function () {
  // These are slow, inconsistent tests.
  // Not run on every test run, but we do run on CI, or when manually debugging issues.
  if (env.CI || env.SLOW_TESTS) {
    defineSlowTests(INITIALIZED_POSTGRES_STORAGE_FACTORY);
  } else {
    // Need something in this file.
    test('no-op', () => {});
  }
});

function defineSlowTests(factory: storage.TestStorageFactory) {
  let walStream: WalStream | undefined;
  let connections: PgManager | undefined;
  let abortController: AbortController | undefined;
  let streamPromise: Promise<void> | undefined;

  afterEach(async () => {
    // This cleans up, similar to WalStreamTestContext.dispose().
    // These tests are a little more complex than what is supported by WalStreamTestContext.
    abortController?.abort();
    await streamPromise;
    streamPromise = undefined;
    connections?.destroy();

    connections = undefined;
    walStream = undefined;
    abortController = undefined;
  });

  const TEST_DURATION_MS = 15_000;
  const TIMEOUT_MARGIN_MS = env.CI ? 30_000 : 15_000;

  // Test repeatedly replicating inserts and deletes, then check that we get
  // consistent data out at the end.
  //
  // Past issues that this could reproduce intermittently:
  // * Skipping LSNs after a keepalive message
  // * Skipping LSNs when source transactions overlap
  test(
    'repeated replication - basic',
    async () => {
      await testRepeatedReplication({ compact: false, maxBatchSize: 50, numBatches: 5 });
    },
    { timeout: TEST_DURATION_MS + TIMEOUT_MARGIN_MS }
  );

  test(
    'repeated replication - compacted',
    async () => {
      await testRepeatedReplication({ compact: true, maxBatchSize: 100, numBatches: 2 });
    },
    { timeout: TEST_DURATION_MS + TIMEOUT_MARGIN_MS }
  );

  async function testRepeatedReplication(testOptions: { compact: boolean; maxBatchSize: number; numBatches: number }) {
    const connections = new PgManager(TEST_CONNECTION_OPTIONS, {});
    const replicationConnection = await connections.replicationConnection();
    const pool = connections.pool;
    await clearTestDb(pool);
    using f = await factory();

    const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT * FROM "test_data"
`;
    const syncRules = await f.updateSyncRules({ content: syncRuleContent });
    using storage = f.getInstance(syncRules);
    abortController = new AbortController();
    const options: WalStreamOptions = {
      abort_signal: abortController.signal,
      connections,
      storage: storage
    };
    walStream = new WalStream(options);

    await pool.query(
      `CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text, num decimal)`
    );
    await pool.query(`ALTER TABLE test_data REPLICA IDENTITY FULL`);

    await walStream.initReplication(replicationConnection);
    await storage.autoActivate();
    let abort = false;
    streamPromise = walStream.streamChanges(replicationConnection).finally(() => {
      abort = true;
    });
    const start = Date.now();

    while (!abort && Date.now() - start < TEST_DURATION_MS) {
      const bg = async () => {
        for (let j = 0; j < testOptions.numBatches && !abort; j++) {
          const n = Math.max(1, Math.floor(Math.random() * testOptions.maxBatchSize));
          let statements: pgwire.Statement[] = [];
          for (let i = 0; i < n; i++) {
            const description = `test${i}`;
            statements.push({
              statement: `INSERT INTO test_data(description, num) VALUES($1, $2) returning id as test_id`,
              params: [
                { type: 'varchar', value: description },
                { type: 'float8', value: Math.random() }
              ]
            });
          }
          const results = await pool.query(...statements);
          const ids = results.results.map((sub) => {
            return sub.rows[0][0] as string;
          });
          await new Promise((resolve) => setTimeout(resolve, Math.random() * 30));

          if (Math.random() > 0.5) {
            const updateStatements: pgwire.Statement[] = ids.map((id) => {
              return {
                statement: `UPDATE test_data SET num = $2 WHERE id = $1`,
                params: [
                  { type: 'uuid', value: id },
                  { type: 'float8', value: Math.random() }
                ]
              };
            });

            await pool.query(...updateStatements);
            if (Math.random() > 0.5) {
              // Special case - an update that doesn't change data
              await pool.query(...updateStatements);
            }
          }

          const deleteStatements: pgwire.Statement[] = ids.map((id) => {
            return {
              statement: `DELETE FROM test_data WHERE id = $1`,
              params: [{ type: 'uuid', value: id }]
            };
          });
          await pool.query(...deleteStatements);

          await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
        }
      };

      let compactController = new AbortController();

      const bgCompact = async () => {
        // Repeatedly compact, and check that the compact conditions hold
        while (!compactController.signal.aborted) {
          const delay = Math.random() * 50;
          try {
            await timers.setTimeout(delay, undefined, { signal: compactController.signal });
          } catch (e) {
            break;
          }

          const checkpoint = BigInt((await storage.getCheckpoint()).checkpoint);
          if (f instanceof mongo_storage.storage.MongoBucketStorage) {
            const opsBefore = (await f.db.bucket_data.find().sort({ _id: 1 }).toArray())
              .filter((row) => row._id.o <= checkpoint)
              .map(mongo_storage.storage.mapOpEntry);
            await storage.compact({ maxOpId: checkpoint });
            const opsAfter = (await f.db.bucket_data.find().sort({ _id: 1 }).toArray())
              .filter((row) => row._id.o <= checkpoint)
              .map(mongo_storage.storage.mapOpEntry);

            test_utils.validateCompactedBucket(opsBefore, opsAfter);
          } else if (f instanceof postgres_storage.PostgresBucketStorageFactory) {
            const { db } = f;
            const opsBefore = (
              await db.sql`
                SELECT
                  *
                FROM
                  bucket_data
                WHERE
                  op_id <= ${{ type: 'int8', value: checkpoint }}
                ORDER BY
                  op_id ASC
              `
                .decoded(postgres_storage.models.BucketData)
                .rows()
            ).map(postgres_storage.utils.mapOpEntry);
            await storage.compact({ maxOpId: checkpoint });
            const opsAfter = (
              await db.sql`
                SELECT
                  *
                FROM
                  bucket_data
                WHERE
                  op_id <= ${{ type: 'int8', value: checkpoint }}
                ORDER BY
                  op_id ASC
              `
                .decoded(postgres_storage.models.BucketData)
                .rows()
            ).map(postgres_storage.utils.mapOpEntry);

            test_utils.validateCompactedBucket(opsBefore, opsAfter);
          }
        }
      };

      // Call the above loop multiple times concurrently
      const promises = [1, 2, 3].map((i) => bg());
      const compactPromise = testOptions.compact ? bgCompact() : null;
      await Promise.all(promises);
      compactController.abort();
      await compactPromise;

      // Wait for replication to finish
      let checkpoint = await getClientCheckpoint(pool, storage.factory, { timeout: TIMEOUT_MARGIN_MS });

      if (f instanceof mongo_storage.storage.MongoBucketStorage) {
        // Check that all inserts have been deleted again
        const docs = await f.db.current_data.find().toArray();
        const transformed = docs.map((doc) => {
          return bson.deserialize(doc.data.buffer) as SqliteRow;
        });
        expect(transformed).toEqual([]);

        // Check that each PUT has a REMOVE
        const ops = await f.db.bucket_data.find().sort({ _id: 1 }).toArray();

        // All a single bucket in this test
        const bucket = ops.map((op) => mongo_storage.storage.mapOpEntry(op));
        const reduced = test_utils.reduceBucket(bucket);
        expect(reduced).toMatchObject([
          {
            op_id: '0',
            op: 'CLEAR'
          }
          // Should contain no additional data
        ]);
      } else if (f instanceof postgres_storage.storage.PostgresBucketStorageFactory) {
        const { db } = f;
        // Check that all inserts have been deleted again
        const docs = await db.sql`
          SELECT
            *
          FROM
            current_data
        `
          .decoded(postgres_storage.models.CurrentData)
          .rows();
        const transformed = docs.map((doc) => {
          return bson.deserialize(doc.data) as SqliteRow;
        });
        expect(transformed).toEqual([]);

        // Check that each PUT has a REMOVE
        const ops = await db.sql`
          SELECT
            *
          FROM
            bucket_data
          ORDER BY
            op_id ASC
        `
          .decoded(postgres_storage.models.BucketData)
          .rows();

        // All a single bucket in this test
        const bucket = ops.map((op) => postgres_storage.utils.mapOpEntry(op));
        const reduced = test_utils.reduceBucket(bucket);
        expect(reduced).toMatchObject([
          {
            op_id: '0',
            op: 'CLEAR'
          }
          // Should contain no additional data
        ]);
      }
    }

    abortController.abort();
    await streamPromise;
  }

  // Test repeatedly performing initial replication.
  //
  // If the first LSN does not correctly match with the first replication transaction,
  // we may miss some updates.
  test(
    'repeated initial replication',
    async () => {
      const pool = await connectPgPool();
      await clearTestDb(pool);
      using f = await factory();

      const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;
      const syncRules = await f.updateSyncRules({ content: syncRuleContent });
      using storage = f.getInstance(syncRules);

      // 1. Setup some base data that will be replicated in initial replication
      await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

      let statements: pgwire.Statement[] = [];

      const n = Math.floor(Math.random() * 200);
      for (let i = 0; i < n; i++) {
        statements.push({
          statement: `INSERT INTO test_data(description) VALUES('test_init')`
        });
      }
      await pool.query(...statements);

      const start = Date.now();
      let i = 0;

      while (Date.now() - start < TEST_DURATION_MS) {
        // 2. Each iteration starts with a clean slate
        await pool.query(`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE active = FALSE`);
        i += 1;

        const connections = new PgManager(TEST_CONNECTION_OPTIONS, {});
        const replicationConnection = await connections.replicationConnection();

        abortController = new AbortController();
        const options: WalStreamOptions = {
          abort_signal: abortController.signal,
          connections,
          storage: storage
        };
        walStream = new WalStream(options);

        await storage.clear();

        // 3. Start initial replication, then streaming, but don't wait for any of this
        let initialReplicationDone = false;
        streamPromise = (async () => {
          await walStream.initReplication(replicationConnection);
          await storage.autoActivate();
          initialReplicationDone = true;
          await walStream.streamChanges(replicationConnection);
        })()
          .catch((e) => {
            initialReplicationDone = true;
            throw e;
          })
          .then((v) => {
            return v;
          });

        // 4. While initial replication is still running, write more changes
        while (!initialReplicationDone) {
          let statements: pgwire.Statement[] = [];
          const n = Math.floor(Math.random() * 10) + 1;
          for (let i = 0; i < n; i++) {
            const description = `test${i}`;
            statements.push({
              statement: `INSERT INTO test_data(description) VALUES('test1') returning id as test_id`,
              params: [{ type: 'varchar', value: description }]
            });
          }
          const results = await pool.query(...statements);
          const ids = results.results.map((sub) => {
            return sub.rows[0][0] as string;
          });
          await new Promise((resolve) => setTimeout(resolve, Math.random() * 30));
          const deleteStatements: pgwire.Statement[] = ids.map((id) => {
            return {
              statement: `DELETE FROM test_data WHERE id = $1`,
              params: [{ type: 'uuid', value: id }]
            };
          });
          await pool.query(...deleteStatements);
          await new Promise((resolve) => setTimeout(resolve, Math.random() * 10));
        }

        // 5. Once initial replication is done, wait for the streaming changes to complete syncing.
        // getClientCheckpoint() effectively waits for the above replication to complete
        // Race with streamingPromise to catch replication errors here.
        let checkpoint = await Promise.race([
          getClientCheckpoint(pool, storage.factory, { timeout: TIMEOUT_MARGIN_MS }),
          streamPromise
        ]);
        if (typeof checkpoint == undefined) {
          // This indicates an issue with the test setup - streamingPromise completed instead
          // of getClientCheckpoint()
          throw new Error('Test failure - streamingPromise completed');
        }

        abortController.abort();
        await streamPromise;
        await connections.end();
      }
    },
    { timeout: TEST_DURATION_MS + TIMEOUT_MARGIN_MS }
  );
}
