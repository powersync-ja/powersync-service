// Script to tear down the data when deleting an instance.
// This deletes:
// 1. The replication slots on the source postgres instance (if available).
// 2. The mongo database.

import * as timers from 'timers/promises';

import * as db from '../db/db-index.js';
import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';
import * as replication from '../replication/replication-index.js';
import { logger, createFSProbe, ErrorReporter, NoOpReporter } from '@powersync/service-framework';

/**
 * Attempt to terminate a single sync rules instance.
 *
 * This may fail with a lock error.
 */
async function terminateReplicator(
  storageFactory: storage.BucketStorageFactory,
  connection: utils.ResolvedConnection,
  syncRules: storage.PersistedSyncRulesContent,
  errorReporter: ErrorReporter
) {
  // The lock may still be active if the current replication instance
  // hasn't stopped yet.
  const lock = await syncRules.lock();
  try {
    const parsed = syncRules.parsed();
    const storage = storageFactory.getInstance(parsed);
    const stream = new replication.WalStreamRunner({
      factory: storageFactory,
      storage: storage,
      source_db: connection,
      lock,
      probe: createFSProbe(),
      errorReporter
    });
    console.log('terminating', stream.slot_name);
    await stream.terminate();
    console.log('terminated', stream.slot_name);
  } finally {
    await lock.release();
  }
}

/**
 * Terminate all replicating sync rules, deleting the replication slots.
 *
 * Retries lock and other errors for up to two minutes.
 *
 * This is a best-effot attempt. In some cases it may not be possible to delete the replication
 * slot, such as when the postgres instance is unreachable.
 */
async function terminateReplicators(
  storageFactory: storage.BucketStorageFactory,
  connection: utils.ResolvedConnection,
  errorReporter: ErrorReporter
) {
  const start = Date.now();
  while (Date.now() - start < 12_000) {
    let retry = false;
    const replicationRules = await storageFactory.getReplicatingSyncRules();
    for (let syncRules of replicationRules) {
      try {
        await terminateReplicator(storageFactory, connection, syncRules, errorReporter);
      } catch (e) {
        retry = true;
        console.error(e);
        logger.warn(`Failed to terminate ${syncRules.slot_name}`, e);
      }
    }
    if (!retry) {
      break;
    }
    await timers.setTimeout(5_000);
  }
}

// TODO should there be a global context for things like alerting?

export async function teardown(runnerConfig: utils.RunnerConfig, errorReporter?: ErrorReporter) {
  const config = await utils.loadConfig(runnerConfig);
  const mongoDB = storage.createPowerSyncMongo(config.storage);
  await db.mongo.waitForAuth(mongoDB.db);

  const resolvedAlerting = errorReporter ?? NoOpReporter;

  const bucketStorage = new storage.MongoBucketStorage(mongoDB, {
    slot_name_prefix: config.slot_name_prefix,
    errorReporter: resolvedAlerting
  });
  const connection = config.connection;

  if (connection) {
    await terminateReplicators(bucketStorage, connection, resolvedAlerting);
  }

  const database = mongoDB.db;
  await database.dropDatabase();
  await mongoDB.client.close();
}
