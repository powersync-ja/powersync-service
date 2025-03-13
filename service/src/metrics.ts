import * as core from '@powersync/service-core';
import {
  createCoreAPIMetrics,
  createCoreReplicationMetrics,
  createCoreStorageMetrics,
  initializeCoreAPIMetrics,
  initializeCoreReplicationMetrics,
  initializeCoreStorageMetrics
} from '@powersync/service-core';

export enum MetricModes {
  API = 'api',
  REPLICATION = 'replication',
  STORAGE = 'storage'
}

export type MetricsRegistrationOptions = {
  service_context: core.system.ServiceContextContainer;
  modes: MetricModes[];
};

export const registerMetrics = async (options: MetricsRegistrationOptions) => {
  const { service_context, modes } = options;

  const metricsFactory = core.metrics.createOpenTelemetryMetricsFactory(service_context);
  const metricsEngine = new core.metrics.MetricsEngine({
    factory: metricsFactory,
    disable_telemetry_sharing: service_context.configuration.telemetry.disable_telemetry_sharing
  });
  service_context.register(core.metrics.MetricsEngine, metricsEngine);

  if (modes.includes(MetricModes.API)) {
    createCoreAPIMetrics(metricsEngine);
    initializeCoreAPIMetrics(metricsEngine);
  }

  if (modes.includes(MetricModes.REPLICATION)) {
    createCoreReplicationMetrics(metricsEngine);
    initializeCoreReplicationMetrics(metricsEngine);
  }

  if (modes.includes(MetricModes.STORAGE)) {
    createCoreStorageMetrics(metricsEngine);

    // This requires an instantiated bucket storage, which is only created when the lifecycle starts
    service_context.storageEngine.registerListener({
      storageActivated: (bucketStorage) => {
        initializeCoreStorageMetrics(metricsEngine, bucketStorage);
      }
    });
  }

  service_context.lifeCycleEngine.withLifecycle(metricsEngine, {
    start: async () => {
      await metricsEngine.start();
    },
    stop: () => metricsEngine.shutdown()
  });
};
