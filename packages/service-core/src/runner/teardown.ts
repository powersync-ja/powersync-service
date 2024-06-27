// Script to tear down the data when deleting an instance.
// This deletes:
// 1. The replication slots on the source postgres instance (if available).
// 2. The mongo database.

import * as timers from 'timers/promises';

import { logger } from '@powersync/lib-services-framework';

import * as storage from '../storage/storage-index.js';
import * as replication from '../replication/replication-index.js';
import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';

/**
 * Attempt to terminate a single sync rules instance.
 *
 * This may fail with a lock error.
 */
async function terminateReplicator(system: CorePowerSyncSystem, syncRules: storage.PersistedSyncRulesContent) {
  // The lock may still be active if the current replication instance
  // hasn't stopped yet.
  const lock = await syncRules.lock();
  try {
    const parsed = syncRules.parsed();
    const storage = system.storageFactory.getInstance(parsed);

    const connectionConfig = system.config.connection!;
    const factory = system.streamManager.getStreamRunnerFactory(connectionConfig.type);
    if (!factory) {
      throw new Error(`Could not find factory for connection type: ${connectionConfig.type}`);
    }

    const stream = await factory!.generate({
      config: connectionConfig,
      storage_factory: system.storageFactory,
      lock,
      storage,
      rate_limiter: new replication.DefaultErrorRateLimiter()
    });

    logger.info(`Terminating replication slot ${stream.slot_name}`);
    await stream.terminate();
    logger.info(`Terminated replication slot ${stream.slot_name}`);
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
async function terminateReplicators(system: CorePowerSyncSystem) {
  const start = Date.now();
  while (Date.now() - start < 12_000) {
    let retry = false;
    const replicationRules = await system.storageFactory.getReplicatingSyncRules();
    for (let syncRules of replicationRules) {
      try {
        await terminateReplicator(system, syncRules);
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

export async function teardown(system: CorePowerSyncSystem) {
  try {
    await system.storageFactory.teardown(async () => {
      if (system.config.connection) {
        await terminateReplicators(system);
      }
    });
    // If there was an error connecting to postgress, the process may stay open indefinitely.
    // This forces an exit.
    // We do not consider those errors a teardown failure.
    process.exit(0);
  } catch (e) {
    logger.error(`Teardown failure`, e);
    process.exit(1);
  }
}
