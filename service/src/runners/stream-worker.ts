import { container, logger } from '@powersync/lib-services-framework';
import { Metrics, migrations, replication, system } from '@powersync/service-core';

export async function startStreamWorker(serviceContext: system.ServiceContext) {
  logger.info('Booting');

  // Self hosted version allows for automatic migrations
  if (!serviceContext.configuration.migrations?.disable_auto_migration) {
    await migrations.migrate({
      direction: migrations.Direction.Up,
      service_context: serviceContext
    });
  }

  // TODO use replication engine
  serviceContext.withLifecycle(new replication.WalStreamManager(system), {
    start: (manager) => manager.start(),
    stop: (manager) => manager.stop()
  });

  logger.info('Starting system');
  await serviceContext.start();
  logger.info('System started');

  Metrics.getInstance().configureReplicationMetrics(system);

  await container.probes.ready();
}
