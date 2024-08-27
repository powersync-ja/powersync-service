// Script to tear down the data when deleting an instance.
// This deletes:
// 1. The replication slots on the source postgres instance (if available).
// 2. The mongo database.

import { container, logger } from '@powersync/lib-services-framework';
import * as modules from '../modules/modules-index.js';
import * as system from '../system/system-index.js';
import * as utils from '../util/util-index.js';

export async function teardown(runnerConfig: utils.RunnerConfig) {
  try {
    const config = await utils.loadConfig(runnerConfig);
    const serviceContext = new system.ServiceContextContainer(config);

    // TODO Teardown stopped replication, clean up all replication slots and then wiped the MongoDB
    // This will now have to take into account multiple modules
    // const moduleManager = container.getImplementation(modules.ModuleManager);
    // await moduleManager.initialize(serviceContext);
    // await moduleManager.tearDown();
    process.exit(0);
  } catch (e) {
    logger.error(`Teardown failure`, e);
    process.exit(1);
  }
}
