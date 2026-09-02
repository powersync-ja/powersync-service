import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader
} from '@opentelemetry/sdk-metrics';

import { createCoreAPIMetrics, MetricsEngine, OpenTelemetryMetricsFactory } from '@/index.js';

/** A metrics engine backed by an in-memory exporter, with a lookup for a single labelled series. */
export function recordingMetricsEngine(name = 'route-test') {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 100 });
  const provider = new MeterProvider({ readers: [reader] });
  const engine = new MetricsEngine({
    factory: new OpenTelemetryMetricsFactory(provider.getMeter(name)),
    disable_telemetry_sharing: true
  });
  createCoreAPIMetrics(engine);

  return {
    engine,
    async seriesValue(metricName: string, attributes: Record<string, string>): Promise<number | undefined> {
      await reader.forceFlush();
      const metrics = exporter.getMetrics();
      const scoped = metrics[metrics.length - 1]?.scopeMetrics?.[0]?.metrics;
      const metric = scoped?.find((m) => m.descriptor.name === metricName);
      const point = metric?.dataPoints.find((p) =>
        Object.entries(attributes).every(([k, v]) => (p.attributes as Record<string, unknown>)[k] === v)
      );
      return point?.value as number | undefined;
    }
  };
}
