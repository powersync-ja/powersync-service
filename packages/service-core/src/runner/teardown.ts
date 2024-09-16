// Script to tear down the data when deleting an instance.
// This should:
// 1. Attempt to clean up any remote configuration of data sources that was set up.
// 2. Delete the storage

import { container, logger } from '@powersync/lib-services-framework';
import * as modules from '../modules/modules-index.js';
import * as system from '../system/system-index.js';
import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';
import timers from 'timers/promises';

export async function teardown(runnerConfig: utils.RunnerConfig) {
  try {
    logger.info(`Tearing down PowerSync instance...`);
    const config = await utils.loadConfig(runnerConfig);
    const serviceContext = new system.ServiceContextContainer(config);
    const moduleManager = container.getImplementation(modules.ModuleManager);
    await moduleManager.initialize(serviceContext);
    // This is mostly done to ensure that the storage is ready
    await serviceContext.lifeCycleEngine.start();

    await terminateSyncRules(serviceContext.storageEngine.activeBucketStorage, moduleManager);
    await serviceContext.storageEngine.activeStorage.tearDown();
    logger.info(`Teardown complete.`);
    process.exit(0);
  } catch (e) {
    logger.error(`Teardown failure`, e);
    process.exit(1);
  }
}

async function terminateSyncRules(storageFactory: storage.BucketStorageFactory, moduleManager: modules.ModuleManager) {
  logger.info(`Terminating sync rules...`);
  const start = Date.now();
  const locks: storage.ReplicationLock[] = [];
  while (Date.now() - start < 120_000) {
    let retry = false;
    const replicatingSyncRules = await storageFactory.getReplicatingSyncRules();
    // Lock all the replicating sync rules
    for (const replicatingSyncRule of replicatingSyncRules) {
      const lock = await replicatingSyncRule.lock();
      locks.push(lock);
    }

    const stoppedSyncRules = await storageFactory.getStoppedSyncRules();
    const combinedSyncRules = [...replicatingSyncRules, ...stoppedSyncRules];
    try {
      // Clean up any module specific configuration for the sync rules
      await moduleManager.tearDown({ syncRules: combinedSyncRules });

      // Mark the sync rules as terminated
      for (let syncRules of combinedSyncRules) {
        const syncRulesStorage = storageFactory.getInstance(syncRules.parsed());
        // The storage will be dropped at the end of the teardown, so we don't need to clear it here
        await syncRulesStorage.terminate({ clearStorage: false });
      }
    } catch (e) {
      retry = true;
      for (const lock of locks) {
        await lock.release();
      }
    }

    if (!retry) {
      break;
    }
    await timers.setTimeout(5_000);
  }
}
