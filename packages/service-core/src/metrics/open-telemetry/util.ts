import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { MeterProvider, MetricReader, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { logger } from '@powersync/lib-services-framework';
import { ServiceContext } from '../../system/ServiceContext.js';
import { MetricsFactory } from '../metrics-interfaces.js';
import { OpenTelemetryMetricsFactory } from './OpenTelemetryMetricsFactory.js';

import pkg from '../../../package.json' with { type: 'json' };
import { resourceFromAttributes } from '@opentelemetry/resources';

export function createOpenTelemetryMetricsFactory(context: ServiceContext): MetricsFactory {
  const { configuration, lifeCycleEngine, storageEngine } = context;
  const configuredExporters: MetricReader[] = [];

  if (configuration.telemetry.prometheus_port) {
    const prometheusExporter = new PrometheusExporter({
      port: configuration.telemetry.prometheus_port,
      preventServerStart: true
    });
    configuredExporters.push(prometheusExporter);

    lifeCycleEngine.withLifecycle(prometheusExporter, {
      start: async () => {
        await prometheusExporter.startServer();
        logger.info(`Prometheus metric export enabled on port:${configuration.telemetry.prometheus_port}`);
      }
    });
  }

  if (!configuration.telemetry.disable_telemetry_sharing) {
    const periodicExporter = new PeriodicExportingMetricReader({
      exporter: new OTLPMetricExporter({
        url: configuration.telemetry.internal_service_endpoint
      }),
      exportIntervalMillis: 1000 * 60 * 5 // 5 minutes
    });

    configuredExporters.push(periodicExporter);
  }

  let resolvedInstanceId: (id: string) => void;
  const instanceIdPromise = new Promise<string>((resolve) => {
    resolvedInstanceId = resolve;
  });

  lifeCycleEngine.withLifecycle(null, {
    start: async () => {
      const bucketStorage = storageEngine.activeBucketStorage;
      try {
        const instanceId = await bucketStorage.getPowerSyncInstanceId();
        resolvedInstanceId(instanceId);
      } catch (err) {
        resolvedInstanceId('Unknown');
      }
    }
  });

  const resource = resourceFromAttributes({
    ['service']: 'PowerSync',
    ['service.version']: pkg.version,
    ['instance_id']: instanceIdPromise
  });

  // This triggers OpenTelemetry to resolve the async attributes (instanceIdPromise).
  // This will never reject, and we don't specifically need to wait for it.
  resource.waitForAsyncAttributes?.();

  const meterProvider = new MeterProvider({
    resource,

    readers: configuredExporters
  });

  lifeCycleEngine.withLifecycle(meterProvider, {
    stop: async () => {
      await meterProvider.shutdown();
    }
  });

  const meter = meterProvider.getMeter('powersync');

  return new OpenTelemetryMetricsFactory(meter);
}
