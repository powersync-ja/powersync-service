import { logger } from '@powersync/lib-services-framework';
import { metrics } from '@powersync/service-core';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { MeterProvider, MetricReader, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { Resource } from '@opentelemetry/resources';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';

/**
 * Creates a {@link Metrics} implementation.
 * This version will conditionally enable anonymous telemetry.
 * An Prometheus exporter is created for internal use - its server is not started or exposed.
 */
export async function createMetrics(options: metrics.MetricsOptions) {
  logger.info('Configuring telemetry.');

  logger.info(
    `
    Attention:
    PowerSync collects completely anonymous telemetry regarding usage.
    This information is used to shape our roadmap to better serve our customers.
    You can learn more, including how to opt-out if you'd not like to participate in this anonymous program, by visiting the following URL:
    https://docs.powersync.com/self-hosting/telemetry
    Anonymous telemetry is currently: ${options.disable_telemetry_sharing ? 'disabled' : 'enabled'}
        `.trim()
  );

  const configuredExporters: MetricReader[] = [];

  // This is used internally for tests
  const prometheusExporter = new PrometheusExporter({ preventServerStart: true });
  configuredExporters.push(prometheusExporter);

  if (!options.disable_telemetry_sharing) {
    logger.info('Sharing anonymous telemetry');
    const periodicExporter = new PeriodicExportingMetricReader({
      exporter: new OTLPMetricExporter({
        url: options.internal_metrics_endpoint
      }),
      exportIntervalMillis: 1000 * 60 * 5 // 5 minutes
    });

    configuredExporters.push(periodicExporter);
  }

  const meterProvider = new MeterProvider({
    resource: new Resource({
      ['service']: 'PowerSync',
      ['instance_id']: options.powersync_instance_id
    }),
    readers: configuredExporters
  });

  logger.info('Telemetry configuration complete.');
  return new metrics.Metrics(meterProvider, prometheusExporter);
}
