import { MetricAttributeFilteringExporter } from '@/metrics/open-telemetry/MetricAttributeFilteringExporter.js';
import { Attributes } from '@opentelemetry/api';
import { ExportResult } from '@opentelemetry/core';
import { DataPointType, PushMetricExporter, ResourceMetrics } from '@opentelemetry/sdk-metrics';
import { describe, expect, it, vi } from 'vitest';

type ExportMetrics = (metrics: ResourceMetrics, resultCallback: (result: ExportResult) => void) => void;

describe('MetricAttributeFilteringExporter', () => {
  it('filters configured attributes without changing the collected metrics', () => {
    const exportMetrics = vi.fn<ExportMetrics>();
    const delegate = {
      export: exportMetrics,
      forceFlush: vi.fn(async () => {}),
      shutdown: vi.fn(async () => {})
    } satisfies PushMetricExporter;
    const exporter = new MetricAttributeFilteringExporter(delegate, new Set(['version_label']));
    const attributes: Attributes = {
      sync_config_id: 'config-a',
      sync_config_state: 'ACTIVE',
      version_label: 'customer-version'
    };
    const metrics = {
      resource: {},
      scopeMetrics: [
        {
          scope: {},
          metrics: [
            {
              dataPointType: DataPointType.GAUGE,
              dataPoints: [{ attributes, value: 100 }]
            }
          ]
        }
      ]
    } as unknown as ResourceMetrics;
    const callback = vi.fn();

    exporter.export(metrics, callback);

    expect(exportMetrics).toHaveBeenCalledWith(
      expect.objectContaining({
        scopeMetrics: [
          expect.objectContaining({
            metrics: [
              expect.objectContaining({
                dataPoints: [
                  expect.objectContaining({
                    attributes: { sync_config_id: 'config-a', sync_config_state: 'ACTIVE' }
                  })
                ]
              })
            ]
          })
        ]
      }),
      callback
    );
    expect(metrics.scopeMetrics[0].metrics[0].dataPoints[0].attributes).toBe(attributes);
  });

  it('forwards rather than merges data points whose attributes become identical', () => {
    const exportMetrics = vi.fn<ExportMetrics>();
    const delegate = {
      export: exportMetrics,
      forceFlush: vi.fn(async () => {}),
      shutdown: vi.fn(async () => {})
    } satisfies PushMetricExporter;
    const exporter = new MetricAttributeFilteringExporter(delegate, new Set(['filtered']));
    const metrics = {
      resource: {},
      scopeMetrics: [
        {
          scope: {},
          metrics: [
            {
              dataPointType: DataPointType.GAUGE,
              dataPoints: [
                { attributes: { retained: 'same', filtered: 'a' }, value: 10 },
                { attributes: { retained: 'same', filtered: 'b' }, value: 20 }
              ]
            }
          ]
        }
      ]
    } as unknown as ResourceMetrics;

    exporter.export(metrics, vi.fn());

    const exported = exportMetrics.mock.calls[0][0];
    expect(exported.scopeMetrics[0].metrics[0].dataPoints).toMatchObject([
      { attributes: { retained: 'same' }, value: 10 },
      { attributes: { retained: 'same' }, value: 20 }
    ]);
  });
});
