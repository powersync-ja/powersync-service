import { container, logger } from '@powersync/lib-services-framework';
import { Metrics, migrations, system } from '@powersync/service-core';

export async function startStreamWorker(serviceContext: system.ServiceContext) {
  logger.info('Booting');

  serviceContext.withLifecycle(serviceContext.storage, {
    async start(storage) {
      const instanceId = await storage.getPowerSyncInstanceId();
      await Metrics.initialise({
        powersync_instance_id: instanceId,
        disable_telemetry_sharing: serviceContext.configuration.telemetry.disable_telemetry_sharing,
        internal_metrics_endpoint: serviceContext.configuration.telemetry.internal_service_endpoint
      });
    },
    async stop() {
      await Metrics.getInstance().shutdown();
    }
  });

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

  Metrics.getInstance().configureReplicationMetrics(serviceContext);

  await container.probes.ready();
}
