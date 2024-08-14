import { logger } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as timers from 'timers/promises';
import { ResolvedConnectionConfig } from '../types/types.js';

/**
 * Terminate all replicating sync rules, deleting the replication slots.
 *
 * Retries lock and other errors for up to two minutes.
 *
 * This is a best-effort attempt. In some cases it may not be possible to delete the replication
 * slot, such as when the postgres instance is unreachable.
 */
export async function terminateReplicators(
  storageFactory: storage.BucketStorageFactory,
  connection: ResolvedConnectionConfig
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
        logger.warn(`Failed to terminate ${syncRules.slot_name}`, e);
      }
    }
    if (!retry) {
      break;
    }
    await timers.setTimeout(5_000);
  }
}

/**
 * Attempt to terminate a single sync rules instance.
 *
 * This may fail with a lock error.
 */
async function terminateReplicator(
  storageFactory: storage.BucketStorageFactory,
  connection: ResolvedConnectionConfig,
  syncRules: storage.PersistedSyncRulesContent
) {
  // The lock may still be active if the current replication instance
  // hasn't stopped yet.
  const lock = await syncRules.lock();
  try {
    const parsed = syncRules.parsed();
    const storage = storageFactory.getInstance(parsed);

    //   const stream = new replication.WalStreamRunner({
    //     factory: storageFactory,
    //     storage: storage,
    //     source_db: connection,
    //     lock
    //   });

    // logger.info(`Terminating replication slot ${stream.slot_name}`);
    // // await stream.terminate();
    // logger.info(`Terminated replication slot ${stream.slot_name}`);
  } finally {
    await lock.release();
  }
}
