import { MetricsEngine, MetricsFactory, OpenTelemetryMetricsFactory } from '@powersync/service-core';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader
} from '@opentelemetry/sdk-metrics';

export class MetricsHelper {
  public factory: MetricsFactory;
  public metricsEngine: MetricsEngine;
  private meterProvider: MeterProvider;
  private exporter: InMemoryMetricExporter;
  private metricReader: PeriodicExportingMetricReader;

  constructor() {
    this.exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);

    this.metricReader = new PeriodicExportingMetricReader({
      exporter: this.exporter,
      exportIntervalMillis: 100 // Export quickly for tests
    });
    this.meterProvider = new MeterProvider({
      readers: [this.metricReader]
    });
    const meter = this.meterProvider.getMeter('powersync-tests');
    this.factory = new OpenTelemetryMetricsFactory(meter);
    this.metricsEngine = new MetricsEngine({
      factory: this.factory,
      disable_telemetry_sharing: true
    });
  }

  resetMetrics() {
    this.exporter.reset();
  }

  async getMetricValueForTests(name: string): Promise<number | undefined> {
    await this.metricReader.forceFlush();
    const metrics = this.exporter.getMetrics();
    // Use the latest reported ResourceMetric
    const scoped = metrics[metrics.length - 1]?.scopeMetrics?.[0]?.metrics;
    const metric = scoped?.find((metric) => metric.descriptor.name == name);
    if (metric == null) {
      throw new Error(
        `Cannot find metric ${name}. Options: ${scoped.map((metric) => metric.descriptor.name).join(',')}`
      );
    }
    const point = metric.dataPoints[metric.dataPoints.length - 1];
    return point?.value as number;
  }
}

export const METRICS_HELPER = new MetricsHelper();
