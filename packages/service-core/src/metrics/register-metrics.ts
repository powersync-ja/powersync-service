import { ServiceContextContainer } from '../system/ServiceContext.js';
import { createOpenTelemetryMetricsFactory } from './open-telemetry/util.js';
import { MetricsEngine } from './MetricsEngine.js';
import { createCoreAPIMetrics, initializeCoreAPIMetrics } from '../api/api-metrics.js';
import { createCoreReplicationMetrics, initializeCoreReplicationMetrics } from '../replication/replication-metrics.js';
import { createCoreStorageMetrics, initializeCoreStorageMetrics } from '../storage/storage-metrics.js';

export enum MetricModes {
  API = 'api',
  REPLICATION = 'replication',
  STORAGE = 'storage'
}

export type MetricsRegistrationOptions = {
  service_context: ServiceContextContainer;
  modes: MetricModes[];
};

export const registerMetrics = async (options: MetricsRegistrationOptions) => {
  const { service_context, modes } = options;

  const metricsFactory = createOpenTelemetryMetricsFactory(service_context);
  const metricsEngine = new MetricsEngine({
    factory: metricsFactory,
    disable_telemetry_sharing: service_context.configuration.telemetry.disable_telemetry_sharing
  });
  service_context.register(MetricsEngine, metricsEngine);

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
