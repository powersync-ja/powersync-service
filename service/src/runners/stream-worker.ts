import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';
import { MetricModes, registerMetrics } from '../metrics.js';

/**
 * Configures the replication portion on a {@link serviceContext}
 */
export const registerReplicationServices = (serviceContext: core.system.ServiceContextContainer) => {
  // Needs to be executed after shared registrations
  const replication = new core.replication.ReplicationEngine({
    config: serviceContext.configuration.sync_rules,
    storage: serviceContext.storage
  });

  serviceContext.register(core.replication.ReplicationEngine, replication);
  serviceContext.lifeCycleEngine.withLifecycle(replication, {
    start: (replication) => replication.start(),
    stop: (replication) => replication.stop()
  });
};

export const startStreamRunner = async (runnerConfig: core.utils.RunnerConfig) => {
  logger.info('Booting');

  const config = await core.utils.loadConfig(runnerConfig);

  await core.migrations.ensureAutomaticMigrations({
    config,
    runner_config: runnerConfig
  });

  // Self hosted version allows for automatic migrations
  const serviceContext = new core.system.ServiceContextContainer(config);

  registerReplicationServices(serviceContext);

  await registerMetrics({
    service_context: serviceContext,
    modes: [MetricModes.REPLICATION]
  });

  const moduleManager = container.getImplementation(core.modules.ModuleManager);
  await moduleManager.initialize(serviceContext);

  logger.info('Starting system');
  await serviceContext.lifeCycleEngine.start();
  logger.info('System started');

  await container.probes.ready();
};
