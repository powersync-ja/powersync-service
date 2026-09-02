import { ErrorCode, InternalServerError, ServiceError } from '@powersync/lib-services-framework';
import { APIMetric } from '@powersync/service-types';
import { beforeEach, describe, expect, it } from 'vitest';

import { recordSyncConnection, SyncCloseReason, syncConnectionCloseReasonLogText, SyncTransport } from '@/index.js';
import { recordingMetricsEngine } from './recording-metrics.js';

describe('recordSyncConnection', () => {
  let recorder: ReturnType<typeof recordingMetricsEngine>;

  beforeEach(() => {
    recorder = recordingMetricsEngine('connection-metrics-test');
  });

  function seriesValue(attributes: Record<string, string>): Promise<number | undefined> {
    return recorder.seriesValue(APIMetric.SYNC_CONNECTIONS, attributes);
  }

  it.each([
    [SyncCloseReason.ClientClosed, 'success', SyncTransport.HttpStream],
    [SyncCloseReason.ServiceClosed, 'success', SyncTransport.HttpStream],
    [SyncCloseReason.ProcessShutdown, 'success', SyncTransport.HttpStream],
    [SyncCloseReason.Unknown, 'success', SyncTransport.HttpStream],
    [SyncCloseReason.StreamError, 'error', SyncTransport.HttpStream],
    [SyncCloseReason.ServiceUnavailable, 'rejected', SyncTransport.HttpStream],
    [SyncCloseReason.NoSyncConfig, 'rejected', SyncTransport.HttpStream],
    [SyncCloseReason.StorageError, 'rejected', SyncTransport.HttpStream],
    [SyncCloseReason.ConcurrencyLimit, 'rejected', SyncTransport.RSocket]
  ] as const)('classifies %s as %s', async (closeReason, outcome, transport) => {
    recordSyncConnection(recorder.engine, { transport, closeReason });

    expect(await seriesValue({ close_reason: closeReason, outcome })).toBe(1);
  });

  describe('error_code', () => {
    it('reports other for errors without a recognized PowerSync error code', async () => {
      for (const error of [
        new Error('boom'),
        'not even an error',
        { is_service_error: true, errorData: { code: 'customer-specific-code' } }
      ]) {
        recordSyncConnection(recorder.engine, {
          transport: SyncTransport.RSocket,
          closeReason: SyncCloseReason.StreamError,
          error
        });
      }

      expect(await seriesValue({ close_reason: 'stream_error', error_code: 'other' })).toBe(3);
    });

    it('reports the PowerSync code from a service error', async () => {
      recordSyncConnection(recorder.engine, {
        transport: SyncTransport.RSocket,
        closeReason: SyncCloseReason.StreamError,
        error: new ServiceError({ status: 500, code: ErrorCode.PSYNC_S2305, description: 'x' })
      });
      recordSyncConnection(recorder.engine, {
        transport: SyncTransport.RSocket,
        closeReason: SyncCloseReason.StreamError,
        error: new InternalServerError(new Error('boom'))
      });

      expect(await seriesValue({ close_reason: 'stream_error', error_code: ErrorCode.PSYNC_S2305 })).toBe(1);
      expect(await seriesValue({ close_reason: 'stream_error', error_code: ErrorCode.PSYNC_S2001 })).toBe(1);
    });

    it('reports other when a failure has no error code', async () => {
      recordSyncConnection(recorder.engine, {
        transport: SyncTransport.RSocket,
        closeReason: SyncCloseReason.StreamError
      });

      expect(await seriesValue({ close_reason: 'stream_error', error_code: 'other' })).toBe(1);
    });

    it('passes a PowerSync error code through', async () => {
      recordSyncConnection(recorder.engine, {
        transport: SyncTransport.RSocket,
        closeReason: SyncCloseReason.NoSyncConfig,
        error: new ServiceError({ status: 500, code: ErrorCode.PSYNC_S2302, description: 'x' })
      });

      expect(
        await seriesValue({
          outcome: 'rejected',
          close_reason: 'no_sync_config',
          error_code: ErrorCode.PSYNC_S2302,
          transport: 'rsocket'
        })
      ).toBe(1);
    });

    it('reports none for a success even when the handler collected an error', async () => {
      recordSyncConnection(recorder.engine, {
        transport: SyncTransport.RSocket,
        closeReason: SyncCloseReason.ClientClosed,
        error: new InternalServerError(new Error('ignored'))
      });

      expect(await seriesValue({ close_reason: 'client_closed', error_code: 'none' })).toBe(1);
      expect(await seriesValue({ close_reason: 'client_closed', error_code: ErrorCode.PSYNC_S2001 })).toBeUndefined();
    });
  });

  it('accumulates independently for each transport', async () => {
    for (let i = 0; i < 2; i++) {
      recordSyncConnection(recorder.engine, {
        transport: SyncTransport.HttpStream,
        closeReason: SyncCloseReason.ClientClosed
      });
    }
    recordSyncConnection(recorder.engine, {
      transport: SyncTransport.RSocket,
      closeReason: SyncCloseReason.ClientClosed
    });

    const labels = { outcome: 'success', close_reason: 'client_closed', error_code: 'none' };
    expect(await seriesValue({ ...labels, transport: 'http_stream' })).toBe(2);
    expect(await seriesValue({ ...labels, transport: 'rsocket' })).toBe(1);
  });
});

describe('syncConnectionCloseReasonLogText', () => {
  it('keeps the pre-existing log wording for handler close reasons', () => {
    expect(syncConnectionCloseReasonLogText(SyncCloseReason.ClientClosed)).toBe('client closing stream');
    expect(syncConnectionCloseReasonLogText(SyncCloseReason.ServiceClosed)).toBe('service closing stream');
    expect(syncConnectionCloseReasonLogText(SyncCloseReason.ProcessShutdown)).toBe('process shutdown');
    expect(syncConnectionCloseReasonLogText(SyncCloseReason.StreamError)).toBe('stream error');
    expect(syncConnectionCloseReasonLogText(undefined)).toBe('unknown');
  });
});
