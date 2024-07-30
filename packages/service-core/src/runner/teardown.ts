// Script to tear down the data when deleting an instance.
// This deletes:
// 1. The replication slots on the source postgres instance (if available).
// 2. The mongo database.

import { logger } from '@powersync/lib-services-framework';
import * as modules from '../modules/modules-index.js';

export async function teardown(moduleManager: modules.ModuleManager) {
  try {
    // teardown all modules
    await moduleManager.tearDown();
    process.exit(0);
  } catch (e) {
    logger.error(`Teardown failure`, e);
    process.exit(1);
  }
}
