import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { loadModules } from '../util/module-loader.js';
import { logBooting } from '../util/version.js';

/**
 * Configures the replication portion on a {@link serviceContext}
 */
export const registerReplicationServices = (serviceContext: core.system.ServiceContextContainer) => {
  // Needs to be executed after shared registrations
  const replication = new core.replication.ReplicationEngine();

  serviceContext.register(core.replication.ReplicationEngine, replication);
  serviceContext.lifeCycleEngine.withLifecycle(replication, {
    start: (replication) => replication.start(),
    stop: (replication) => replication.shutDown()
  });
};

export const startStreamRunner = async (runnerConfig: core.utils.RunnerConfig) => {
  logBooting('Replication Container');

  const config = await core.utils.loadConfig(runnerConfig);

  const moduleManager = container.getImplementation(core.modules.ModuleManager);
  const modules = await loadModules(config);
  if (modules.length > 0) {
    moduleManager.register(modules);
  }

  // Self-hosted version allows for automatic migrations
  const serviceContext = new core.system.ServiceContextContainer({
    serviceMode: core.system.ServiceContextMode.SYNC,
    configuration: config
  });
  registerReplicationServices(serviceContext);

  await moduleManager.initialize(serviceContext);

  // Ensure automatic migrations
  await core.migrations.ensureAutomaticMigrations({
    serviceContext
  });

  logger.info('Starting system');
  await serviceContext.lifeCycleEngine.start();
  logger.info('System started');

  await container.probes.ready();
};
