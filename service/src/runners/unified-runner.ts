import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { MetricModes, registerMetrics } from '../metrics.js';
import { registerServerServices } from './server.js';
import { registerReplicationServices } from './stream-worker.js';

/**
 * Starts an API server
 */
export const startUnifiedRunner = async (runnerConfig: core.utils.RunnerConfig) => {
  logger.info('Booting');

  const config = await core.utils.loadConfig(runnerConfig);

  const serviceContext = new core.system.ServiceContextContainer(config);

  registerServerServices(serviceContext);
  registerReplicationServices(serviceContext);

  await registerMetrics({
    service_context: serviceContext,
    modes: [MetricModes.API, MetricModes.REPLICATION]
  });

  const moduleManager = container.getImplementation(core.modules.ModuleManager);
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
