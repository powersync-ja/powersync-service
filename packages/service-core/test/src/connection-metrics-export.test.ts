import {
  createCoreAPIMetrics,
  initializeCoreAPIMetrics,
  MetricsEngine,
  OpenTelemetryMetricsFactory,
  recordSyncConnection,
  SyncCloseReason,
  SyncTransport
} from '@/index.js';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { MeterProvider } from '@opentelemetry/sdk-metrics';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
import { createServer } from 'node:http';
import { AddressInfo } from 'node:net';
import { expect, it } from 'vitest';

it('scrapes only observed connection series and omits error_code for ok closes', async () => {
  const exporter = new PrometheusExporter({ preventServerStart: true });
  const provider = new MeterProvider({ readers: [exporter] });
  const engine = new MetricsEngine({
    factory: new OpenTelemetryMetricsFactory(provider.getMeter('connection-metrics-test')),
    disable_telemetry_sharing: true
  });
  createCoreAPIMetrics(engine);
  initializeCoreAPIMetrics(engine);
  const server = createServer((request, response) => exporter.getMetricsRequestHandler(request, response));
  try {
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address() as AddressInfo;
    const scrape = async () => {
      const response = await fetch(`http://127.0.0.1:${address.port}/metrics`, { signal: AbortSignal.timeout(2_000) });
      expect(response.status).toBe(200);
      return (await response.text()).split('\n').filter((line) => line.startsWith('powersync_sync_connections_total{'));
    };
    const errorSeries = (lines: string[]) =>
      lines.find((line) =>
        ['outcome="error"', 'close_reason="stream_error"', 'error_code="PSYNC_S2403"', 'transport="http_stream"'].every(
          (label) => line.includes(label)
        )
      );

    expect(await scrape()).toEqual([]);

    recordSyncConnection(engine, {
      transport: SyncTransport.HttpStream,
      closeReason: SyncCloseReason.StreamError,
      error: new ServiceError(ErrorCode.PSYNC_S2403, 'Storage query timed out')
    });
    const afterFailure = await scrape();
    expect(afterFailure).toHaveLength(1);
    expect(errorSeries(afterFailure)).toMatch(/ 1$/);

    for (const [index, error] of [
      undefined,
      new ServiceError(ErrorCode.PSYNC_S2403, 'Ignored on an ok close')
    ].entries()) {
      recordSyncConnection(engine, {
        transport: SyncTransport.HttpStream,
        closeReason: SyncCloseReason.ClientClosed,
        error
      });

      const afterClose = await scrape();
      expect(afterClose).toHaveLength(2);
      expect(errorSeries(afterClose)).toMatch(/ 1$/);
      const okSeries = afterClose.find((line) => line.includes('outcome="ok"'));
      expect(okSeries).toContain('close_reason="client_closed"');
      expect(okSeries).toContain('transport="http_stream"');
      expect(okSeries).not.toContain('error_code=');
      expect(okSeries).toMatch(new RegExp(` ${index + 1}$`));
    }
  } finally {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    await provider.shutdown();
  }
});
