import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';

import { logBooting } from '../util/version.js';

/**
 * Configures the replication portion on a {@link serviceContext}
 */
export const registerReplicationServices = (serviceContext: core.system.ServiceContextContainer) => {
  // Needs to be executed after shared registrations
  const replication = new core.replication.ReplicationEngine();
  const lifecycle: core.framework.PartialLifecycle<core.replication.ReplicationEngine> = {
    start: (replication) => replication.start(),
    stop: (replication) => replication.shutDown(),
    stopAccepting: (replication) => replication.stopAcceptingReplicationJobs()
  };

  if (serviceContext.serviceMode == core.system.ServiceContextMode.SYNC) {
    lifecycle.completed = (replication) => replication.completed;
  }

  serviceContext.register(core.replication.ReplicationEngine, replication);
  serviceContext.lifeCycleEngine.withLifecycle(replication, lifecycle);
};

export const startStreamRunner = async (runnerConfig: core.utils.RunnerConfig) => {
  logBooting('Replication Container');

  const config = await core.utils.loadConfig(runnerConfig);

  const moduleManager = container.getImplementation(core.modules.ModuleManager);

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
