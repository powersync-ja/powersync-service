import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader
} from '@opentelemetry/sdk-metrics';

import { createCoreAPIMetrics, initializeCoreAPIMetrics, MetricsEngine, OpenTelemetryMetricsFactory } from '@/index.js';

/** Collects real metrics and sums series matching the supplied labels. */
export function recordingMetricsEngine(name = 'route-test') {
  const exporter = new InMemoryMetricExporter(AggregationTemporality.CUMULATIVE);
  const reader = new PeriodicExportingMetricReader({ exporter, exportIntervalMillis: 60_000 });
  const provider = new MeterProvider({ readers: [reader] });
  const engine = new MetricsEngine({
    factory: new OpenTelemetryMetricsFactory(provider.getMeter(name)),
    disable_telemetry_sharing: true
  });
  createCoreAPIMetrics(engine);
  initializeCoreAPIMetrics(engine);

  return {
    engine,
    shutdown: () => provider.shutdown(),
    async seriesValue(metricName: string, attributes: Record<string, string>): Promise<number | undefined> {
      await reader.forceFlush();
      const metrics = exporter.getMetrics();
      const scoped = metrics[metrics.length - 1]?.scopeMetrics?.[0]?.metrics;
      const metric = scoped?.find((m) => m.descriptor.name === metricName);
      const points = metric?.dataPoints.filter((p) =>
        Object.entries(attributes).every(([k, v]) => p.attributes[k] === v)
      );
      return points?.length ? points.reduce((sum, point) => sum + (point.value as number), 0) : undefined;
    }
  };
}
