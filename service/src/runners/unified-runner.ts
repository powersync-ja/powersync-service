import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { registerServerServices } from './server.js';
import { registerSharedRunnerServices } from './shared-runner.js';
import { registerReplicationServices } from './stream-worker.js';

/**
 * Starts an API server
 */
export const startUnifiedRunner = async (runnerConfig: core.utils.RunnerConfig) => {
  logger.info('Booting');

  const config = await core.utils.loadConfig(runnerConfig);

  await core.migrations.ensureAutomaticMigrations({
    config,
    runner_config: runnerConfig
  });

  const serviceContext = new core.system.ServiceContextContainer(config);

  await registerSharedRunnerServices(serviceContext);
  registerServerServices(serviceContext);
  registerReplicationServices(serviceContext);

  const moduleManager = container.getImplementation(core.modules.ModuleManager);
  await moduleManager.initialize(serviceContext);

  logger.info('Starting service');
  await serviceContext.lifeCycleEngine.start();
  logger.info('service started');

  await container.probes.ready();

  // Enable in development to track memory usage:
  // trackMemoryUsage();
};
