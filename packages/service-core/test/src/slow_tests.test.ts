import * as bson from 'bson';
import * as mongo from 'mongodb';
import { afterEach, describe, expect, test } from 'vitest';
import { WalStream, WalStreamOptions } from '../../src/replication/WalStream.js';
import { getClientCheckpoint } from '../../src/util/utils.js';
import { env } from './env.js';
import { MONGO_STORAGE_FACTORY, StorageFactory, TEST_CONNECTION_OPTIONS, connectPgPool } from './util.js';

import * as pgwire from '@powersync/service-jpgwire';
import { SqliteRow } from '@powersync/service-sync-rules';
import { MongoBucketStorage } from '../../src/storage/MongoBucketStorage.js';
import { PgManager } from '../../src/util/PgManager.js';
import { NoOpReporter, createInMemoryProbe } from '@powersync/service-framework';

describe('slow tests - mongodb', function () {
  // These are slow, inconsistent tests.
  // Not run on every test run, but we do run on CI, or when manually debugging issues.
  if (env.CI || env.SLOW_TESTS) {
    defineSlowTests(MONGO_STORAGE_FACTORY);
  } else {
    // Need something in this file.
    test('no-op', () => {});
  }
});

function defineSlowTests(factory: StorageFactory) {
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
    'repeated replication',
    async () => {
      const connections = new PgManager(TEST_CONNECTION_OPTIONS, {});
      const replicationConnection = await connections.replicationConnection();
      const pool = connections.pool;
      const f = (await factory()) as MongoBucketStorage;

      const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;
      const syncRules = await f.updateSyncRules({ content: syncRuleContent });
      const storage = f.getInstance(syncRules.parsed());
      abortController = new AbortController();
      const options: WalStreamOptions = {
        abort_signal: abortController.signal,
        connections,
        storage: storage,
        factory: f,
        probe: createInMemoryProbe(),
        errorReporter: NoOpReporter
      };
      walStream = new WalStream(options);

      await pool.query(`DROP TABLE IF EXISTS test_data`);
      await pool.query(`CREATE TABLE test_data(id uuid primary key default uuid_generate_v4(), description text)`);

      await walStream.initReplication(replicationConnection);
      await storage.autoActivate();
      let abort = false;
      streamPromise = walStream.streamChanges(replicationConnection).finally(() => {
        abort = true;
      });
      const start = Date.now();

      while (!abort && Date.now() - start < TEST_DURATION_MS) {
        const bg = async () => {
          for (let j = 0; j < 5 && !abort; j++) {
            const n = Math.floor(Math.random() * 50);
            let statements: pgwire.Statement[] = [];
            for (let i = 0; i < n; i++) {
              const description = `test${i}`;
              statements.push({
                statement: `INSERT INTO test_data(description) VALUES($1) returning id as test_id`,
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
        };

        // Call the above loop multiple times concurrently
        let promises = [1, 2, 3].map((i) => bg());
        await Promise.all(promises);

        // Wait for replication to finish
        let checkpoint = await getClientCheckpoint(pool, storage.factory, { timeout: TIMEOUT_MARGIN_MS });

        // Check that all inserts have been deleted again
        const docs = await f.db.current_data.find().toArray();
        const transformed = docs.map((doc) => {
          return bson.deserialize((doc.data as mongo.Binary).buffer) as SqliteRow;
        });
        expect(transformed).toEqual([]);
      }

      abortController.abort();
      await streamPromise;
    },
    { timeout: TEST_DURATION_MS + TIMEOUT_MARGIN_MS }
  );

  // Test repeatedly performing initial replication.
  //
  // If the first LSN does not correctly match with the first replication transaction,
  // we may miss some updates.
  test(
    'repeated initial replication',
    async () => {
      const pool = await connectPgPool();
      const f = await factory();

      const syncRuleContent = `
bucket_definitions:
  global:
    data:
      - SELECT id, description FROM "test_data"
`;
      const syncRules = await f.updateSyncRules({ content: syncRuleContent });
      const storage = f.getInstance(syncRules.parsed());

      // 1. Setup some base data that will be replicated in initial replication
      await pool.query(`DROP TABLE IF EXISTS test_data`);
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
          storage: storage,
          factory: f,
          probe: createInMemoryProbe(),
          errorReporter: NoOpReporter
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
