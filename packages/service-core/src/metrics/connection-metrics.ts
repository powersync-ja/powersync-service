import { ErrorCode, errors } from '@powersync/lib-services-framework';
import { APIMetric } from '@powersync/service-types';
import { MetricsEngine } from './MetricsEngine.js';

export enum SyncCloseReason {
  /** Routine client disconnect/reconnect. */
  ClientClosed = 'client_closed',
  /** Server-side close: auth expiry or switch to a new sync config. */
  ServiceClosed = 'service_closed',
  ProcessShutdown = 'process_shutdown',
  StreamError = 'stream_error',
  ServiceUnavailable = 'service_unavailable',
  NoSyncConfig = 'no_sync_config',
  StorageError = 'storage_error',
  ConcurrencyLimit = 'concurrency_limit',
  Unknown = 'unknown'
}

export enum SyncTransport {
  HttpStream = 'http_stream',
  RSocket = 'rsocket'
}

type SyncConnectionReasonPolicy = Readonly<{
  outcome: 'success' | 'error' | 'rejected';
  logText: string;
}>;

const SYNC_CONNECTION_REASON_POLICY = {
  [SyncCloseReason.ClientClosed]: { outcome: 'success', logText: 'client closing stream' },
  [SyncCloseReason.ServiceClosed]: { outcome: 'success', logText: 'service closing stream' },
  [SyncCloseReason.ProcessShutdown]: { outcome: 'success', logText: 'process shutdown' },
  [SyncCloseReason.StreamError]: { outcome: 'error', logText: 'stream error' },
  [SyncCloseReason.ServiceUnavailable]: { outcome: 'rejected', logText: 'service unavailable' },
  [SyncCloseReason.NoSyncConfig]: { outcome: 'rejected', logText: 'no sync config' },
  [SyncCloseReason.StorageError]: { outcome: 'rejected', logText: 'storage error' },
  [SyncCloseReason.ConcurrencyLimit]: { outcome: 'rejected', logText: 'concurrency limit' },
  // Nothing was thrown or reported, so the stream ended cleanly without a specific reason.
  [SyncCloseReason.Unknown]: { outcome: 'success', logText: 'unknown' }
} as const satisfies Readonly<Record<SyncCloseReason, SyncConnectionReasonPolicy>>;

/** Wording used by existing log-based dashboards for the `close_reason` field. */
export function syncConnectionCloseReasonLogText(closeReason?: SyncCloseReason): string {
  return SYNC_CONNECTION_REASON_POLICY[closeReason ?? SyncCloseReason.Unknown].logText;
}

export interface SyncConnectionMetric {
  transport: SyncTransport;
  closeReason: SyncCloseReason;
  /** The original failure, before any transport-specific wrapping. */
  error?: unknown;
}

const ERROR_CODES: ReadonlySet<string> = new Set(Object.values(ErrorCode));

export function recordSyncConnection(engine: MetricsEngine, metric: SyncConnectionMetric): void {
  const { outcome } = SYNC_CONNECTION_REASON_POLICY[metric.closeReason];

  // Keep successful closes in one error_code series, even if an error was supplied.
  let errorCode = 'none';
  if (outcome !== 'success') {
    const code = errors.ServiceError.isServiceError(metric.error) ? metric.error.errorData?.code : undefined;
    errorCode = typeof code == 'string' && ERROR_CODES.has(code) ? code : 'other';
  }

  engine.getCounter(APIMetric.SYNC_CONNECTIONS).add(1, {
    outcome,
    close_reason: metric.closeReason,
    error_code: errorCode,
    transport: metric.transport
  });
}
