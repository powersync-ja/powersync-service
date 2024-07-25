import { container, logger } from '@powersync/lib-services-framework';
import * as core from '@powersync/service-core';
import { routerSetup } from '../routes/router-config.js';

/**
 * Starts an API server
 */
export async function startServer(serviceContext: core.system.ServiceContext) {
  logger.info('Booting');

  serviceContext.withLifecycle(serviceContext.storage, {
    async start(storage) {
      const instanceId = await storage.getPowerSyncInstanceId();
      await core.Metrics.initialise({
        powersync_instance_id: instanceId,
        disable_telemetry_sharing: serviceContext.configuration.telemetry.disable_telemetry_sharing,
        internal_metrics_endpoint: serviceContext.configuration.telemetry.internal_service_endpoint
      });
    },
    async stop() {
      await core.Metrics.getInstance().shutdown();
    }
  });

  logger.info('Starting service');
  // TODO cleanup the initialization of metrics
  await serviceContext.start();

  core.Metrics.getInstance().configureApiMetrics();

  // Start the router
  await serviceContext.routerEngine.start(routerSetup);

  logger.info('service started');

  await container.probes.ready();

  // Enable in development to track memory usage:
  // trackMemoryUsage();
}
