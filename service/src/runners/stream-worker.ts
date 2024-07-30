import { container, logger } from '@powersync/lib-services-framework';
import { migrations, system } from '@powersync/service-core';

export async function startStreamWorker(serviceContext: system.ServiceContext) {
  logger.info('Booting');

  // Self hosted version allows for automatic migrations
  if (!serviceContext.configuration.migrations?.disable_auto_migration) {
    await migrations.migrate({
      direction: migrations.Direction.Up,
      service_context: serviceContext
    });
  }

  logger.info('Starting system');
  await serviceContext.start();
  logger.info('System started');

  // Start the replication engine
  await serviceContext.replicationEngine.start();

  serviceContext.metrics.configureReplicationMetrics(serviceContext);

  await container.probes.ready();
}
