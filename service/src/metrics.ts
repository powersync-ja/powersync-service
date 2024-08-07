import * as core from '@powersync/service-core';

export enum MetricModes {
  API = 'api',
  REPLICATION = 'replication'
}

export type MetricsRegistrationOptions = {
  service_context: core.system.ServiceContextContainer;
  modes: MetricModes[];
};

export const registerMetrics = async (options: MetricsRegistrationOptions) => {
  const { service_context, modes } = options;

  // This requires an instantiated bucket storage, which is only created when the lifecycle starts
  service_context.lifeCycleEngine.withLifecycle(null, {
    start: async () => {
      const instanceId = await service_context.storage.bucketStorage.getPowerSyncInstanceId();
      await core.metrics.Metrics.initialise({
        powersync_instance_id: instanceId,
        disable_telemetry_sharing: service_context.configuration.telemetry.disable_telemetry_sharing,
        internal_metrics_endpoint: service_context.configuration.telemetry.internal_service_endpoint
      });

      // TODO remove singleton
      const instance = core.Metrics.getInstance();
      service_context.register(core.metrics.Metrics, instance);

      if (modes.includes(MetricModes.API)) {
        instance.configureApiMetrics();
      }

      if (modes.includes(MetricModes.REPLICATION)) {
        instance.configureReplicationMetrics(service_context.storage.bucketStorage);
      }
    },
    stop: () => service_context.metrics.shutdown()
  });
};
