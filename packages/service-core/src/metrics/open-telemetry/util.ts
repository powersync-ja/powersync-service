import { MeterProvider, MetricReader, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { env } from '../../util/env.js';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { Resource } from '@opentelemetry/resources';
import { ServiceContext } from '../../system/ServiceContext.js';
import { OpenTelemetryMetricsFactory } from './OpenTelemetryMetricsFactory.js';
import { MetricsFactory } from '../metrics-interfaces.js';

export interface RuntimeMetadata {
  [key: string]: string | number | undefined;
}

export function createOpenTelemetryMetricsFactory(context: ServiceContext): MetricsFactory {
  const { configuration, lifeCycleEngine, storageEngine } = context;
  const configuredExporters: MetricReader[] = [];

  if (env.METRICS_PORT) {
    const prometheusExporter = new PrometheusExporter({ port: env.METRICS_PORT, preventServerStart: true });
    configuredExporters.push(prometheusExporter);

    lifeCycleEngine.withLifecycle(prometheusExporter, {
      start: () => prometheusExporter.startServer()
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

  let resolvedMetadata: (metadata: RuntimeMetadata) => void;
  const runtimeMetadata: Promise<RuntimeMetadata> = new Promise((resolve) => {
    resolvedMetadata = resolve;
  });

  lifeCycleEngine.withLifecycle(null, {
    start: async () => {
      const bucketStorage = storageEngine.activeBucketStorage;
      try {
        const instanceId = await bucketStorage.getPowerSyncInstanceId();
        resolvedMetadata({ ['instance_id']: instanceId });
      } catch (err) {
        resolvedMetadata({ ['instance_id']: 'Unknown' });
      }
    }
  });

  const meterProvider = new MeterProvider({
    resource: new Resource(
      {
        ['service']: 'PowerSync'
      },
      runtimeMetadata
    ),
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
