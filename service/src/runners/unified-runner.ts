import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { registerServerServices } from './server.js';
import { registerReplicationServices } from './stream-worker.js';
import { logBooting } from '../util/version.js';

/**
 * Starts an API server
 */
export const startUnifiedRunner = async (runnerConfig: core.utils.RunnerConfig) => {
  logBooting('Unified Container');

  const config = await core.utils.loadConfig(runnerConfig);
  core.utils.setTags(config.metadata);
  const serviceContext = new core.system.ServiceContextContainer(config);

  registerServerServices(serviceContext);
  registerReplicationServices(serviceContext);

  await core.metrics.registerMetrics({
    service_context: serviceContext,
    modes: [core.metrics.MetricModes.API, core.metrics.MetricModes.REPLICATION, core.metrics.MetricModes.STORAGE]
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
