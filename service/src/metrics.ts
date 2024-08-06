import * as core from '@powersync/service-core';

export async function registerMetrics(serviceContext: core.system.ServiceContextContainer) {
  const instanceId = await serviceContext.storage.getPowerSyncInstanceId();
  await core.metrics.Metrics.initialise({
    powersync_instance_id: instanceId,
    disable_telemetry_sharing: serviceContext.configuration.telemetry.disable_telemetry_sharing,
    internal_metrics_endpoint: serviceContext.configuration.telemetry.internal_service_endpoint
  });

  // TODO remove singleton
  serviceContext.register(core.metrics.Metrics, core.Metrics.getInstance());
}
