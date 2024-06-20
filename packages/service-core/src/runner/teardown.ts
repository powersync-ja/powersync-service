// Script to tear down the data when deleting an instance.
// This deletes:
// 1. The replication slots on the source postgres instance (if available).
// 2. The mongo database.

import * as timers from 'timers/promises';

import { container } from '@powersync/service-framework';

import * as db from '../db/db-index.js';
import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';
import * as replication from '../replication/replication-index.js';

/**
 * Attempt to terminate a single sync rules instance.
 *
 * This may fail with a lock error.
 */
async function terminateReplicator(
  storageFactory: storage.BucketStorageFactory,
  connection: utils.ResolvedConnection,
  syncRules: storage.PersistedSyncRulesContent
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
      lock
    });

    container.logger.info(`Terminating replication slot ${stream.slot_name}`);
    await stream.terminate();
    container.logger.info(`Terminated replication slot ${stream.slot_name}`);
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
  connection: utils.ResolvedConnection
) {
  const start = Date.now();
  while (Date.now() - start < 12_000) {
    let retry = false;
    const replicationRules = await storageFactory.getReplicatingSyncRules();
    for (let syncRules of replicationRules) {
      try {
        await terminateReplicator(storageFactory, connection, syncRules);
      } catch (e) {
        retry = true;
        console.error(e);
        container.logger.warn(`Failed to terminate ${syncRules.slot_name}`, e);
      }
    }
    if (!retry) {
      break;
    }
    await timers.setTimeout(5_000);
  }
}

// TODO should there be a global context for things like alerting?

export async function teardown(runnerConfig: utils.RunnerConfig) {
  const config = await utils.loadConfig(runnerConfig);
  const mongoDB = storage.createPowerSyncMongo(config.storage);
  try {
    container.logger.info(`Waiting for auth`);
    await db.mongo.waitForAuth(mongoDB.db);

    const bucketStorage = new storage.MongoBucketStorage(mongoDB, { slot_name_prefix: config.slot_name_prefix });
    const connection = config.connection;

    container.logger.info(`Terminating replication slots`);

    if (connection) {
      await terminateReplicators(bucketStorage, connection);
    }

    const database = mongoDB.db;
    container.logger.info(`Dropping database ${database.namespace}`);
    await database.dropDatabase();
    container.logger.info(`Done`);
    await mongoDB.client.close();

    // If there was an error connecting to postgress, the process may stay open indefinitely.
    // This forces an exit.
    // We do not consider those errors a teardown failure.
    process.exit(0);
  } catch (e) {
    container.logger.error(`Teardown failure`, e);
    await mongoDB.client.close();
    process.exit(1);
  }
}
