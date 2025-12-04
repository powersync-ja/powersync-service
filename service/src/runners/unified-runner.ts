import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { logBooting } from '../util/version.js';
import { registerReplicationServices } from './stream-worker.js';
import { DYNAMIC_MODULES } from '../util/modules.js';

/**
 * Starts an API server
 */
export const startUnifiedRunner = async (runnerConfig: core.utils.RunnerConfig) => {
  logBooting('Unified Container');

  const config = await core.utils.loadConfig(runnerConfig);

  const moduleManager = container.getImplementation(core.modules.ModuleManager);
  const modules = await core.loadModules(config, DYNAMIC_MODULES);
  if (modules.length > 0) {
    moduleManager.register(modules);
  }

  const serviceContext = new core.system.ServiceContextContainer({
    serviceMode: core.system.ServiceContextMode.UNIFIED,
    configuration: config
  });
  registerReplicationServices(serviceContext);

  await moduleManager.initialize(serviceContext);

  await core.migrations.ensureAutomaticMigrations({
    serviceContext
  });

  logger.info('Starting service...');
  await serviceContext.lifeCycleEngine.start();
  logger.info('Service started');

  await container.probes.ready();

  // Enable in development to track memory usage:
  // trackMemoryUsage();
};
