import { Attributes } from '@opentelemetry/api';
import { ExportResult } from '@opentelemetry/core';
import {
  AggregationSelector,
  AggregationTemporalitySelector,
  DataPoint,
  DataPointType,
  MetricData,
  PushMetricExporter,
  ResourceMetrics
} from '@opentelemetry/sdk-metrics';

/**
 * Decorates a push exporter and removes selected attributes from every metric data point before
 * delegating the export. The input metrics are not mutated, so another reader can export the
 * original attributes.
 *
 * This exporter deliberately does not merge data points when filtering makes their attribute sets
 * identical. Both points are forwarded with the same attributes. There is no generally correct
 * merge operation at this layer: gauges, counters, and histograms have different aggregation
 * semantics. Callers must therefore ensure that an excluded attribute does not distinguish data
 * points for the same metric, resource, and instrumentation scope. Otherwise, the resulting
 * duplicate series identity may be rejected or interpreted unpredictably downstream.
 *
 * Lifecycle methods and aggregation selection are delegated unchanged to the wrapped exporter.
 */
export class MetricAttributeFilteringExporter implements PushMetricExporter {
  readonly selectAggregation?: AggregationSelector;
  readonly selectAggregationTemporality?: AggregationTemporalitySelector;

  constructor(
    /** The exporter that receives the filtered copy of the metrics. */
    private readonly delegate: PushMetricExporter,
    /** Attribute names to remove from every exported data point. */
    private readonly excludedAttributes: ReadonlySet<string>
  ) {
    this.selectAggregation = delegate.selectAggregation?.bind(delegate);
    this.selectAggregationTemporality = delegate.selectAggregationTemporality?.bind(delegate);
  }

  export(metrics: ResourceMetrics, resultCallback: (result: ExportResult) => void): void {
    this.delegate.export(this.filterAttributes(metrics), resultCallback);
  }

  forceFlush(): Promise<void> {
    return this.delegate.forceFlush();
  }

  shutdown(): Promise<void> {
    return this.delegate.shutdown();
  }

  private filterAttributes(metrics: ResourceMetrics): ResourceMetrics {
    return {
      ...metrics,
      scopeMetrics: metrics.scopeMetrics.map((scopeMetrics) => ({
        ...scopeMetrics,
        metrics: scopeMetrics.metrics.map((metric) => this.filterMetricAttributes(metric))
      }))
    };
  }

  private filterMetricAttributes(metric: MetricData): MetricData {
    switch (metric.dataPointType) {
      case DataPointType.SUM:
      case DataPointType.GAUGE:
        return { ...metric, dataPoints: metric.dataPoints.map((dataPoint) => this.filterDataPoint(dataPoint)) };
      case DataPointType.HISTOGRAM:
        return { ...metric, dataPoints: metric.dataPoints.map((dataPoint) => this.filterDataPoint(dataPoint)) };
      case DataPointType.EXPONENTIAL_HISTOGRAM:
        return { ...metric, dataPoints: metric.dataPoints.map((dataPoint) => this.filterDataPoint(dataPoint)) };
    }
  }

  private filterDataPoint<T>(dataPoint: DataPoint<T>): DataPoint<T> {
    return {
      ...dataPoint,
      attributes: this.filterDataPointAttributes(dataPoint.attributes)
    };
  }

  private filterDataPointAttributes(attributes: Attributes): Attributes {
    return Object.fromEntries(Object.entries(attributes).filter(([name]) => !this.excludedAttributes.has(name)));
  }
}
