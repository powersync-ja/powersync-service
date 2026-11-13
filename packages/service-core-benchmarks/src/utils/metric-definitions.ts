import type { BenchmarkLayer } from '../types/BenchmarkScenario.js';

// Explicit units keep new counters from silently acquiring an incorrect unit or rate.
export const counterUnits: Record<string, string> = {
  source_rows: 'rows',
  source_transactions: 'transactions',
  source_logical_bytes: 'bytes',
  payload_bytes: 'bytes',
  read_data_bytes: 'bytes',
  bucket_data_bytes: 'bytes',
  writer_save_calls: 'calls',
  writer_flushes: 'flushes',
  bucket_operations: 'ops',
  parameter_operations: 'ops',
  storage_put_operations: 'ops',
  client_operations: 'ops',
  client_put_operations: 'ops',
  distinct_buckets: 'buckets',
  visible_checkpoints: 'checkpoints',
  target_markers: 'markers',
  client_count: 'clients',
  protocol_plaintext_bytes: 'bytes',
  response_wire_bytes: 'bytes',
  response_lines: 'lines',
  keepalives: 'keepalives',
  retries: 'retries',
  restarts: 'restarts'
};

export const resourceUnits: Record<string, string> = {
  cpu_user_ms: 'ms',
  cpu_system_ms: 'ms',
  cpu_total_ms: 'ms',
  rss_baseline_bytes: 'bytes',
  rss_average_bytes: 'bytes',
  rss_peak_bytes: 'bytes',
  rss_delta_bytes: 'bytes',
  memory_baseline_bytes: 'bytes',
  memory_average_bytes: 'bytes',
  memory_peak_bytes: 'bytes',
  memory_delta_bytes: 'bytes',
  sample_interval_ms: 'ms',
  sample_count: 'samples'
};

const replicationCounters = [
  'source_rows',
  'source_logical_bytes',
  'payload_bytes',
  'bucket_data_bytes',
  'writer_save_calls',
  'bucket_operations',
  'parameter_operations'
];

export function rateDefinitions(layer: BenchmarkLayer): Record<string, readonly string[]> {
  switch (layer) {
    case 'storage':
      return {
        storage_write: replicationCounters,
        storage_read: ['bucket_operations', 'read_data_bytes'],
        storage_write_read: ['bucket_operations', 'read_data_bytes']
      };
    case 'replication':
      return {
        replication_snapshot: replicationCounters,
        replication_streaming: [...replicationCounters, 'source_transactions']
      };
    case 'api':
      return {
        http_read: ['response_wire_bytes', 'response_lines', 'bucket_operations'],
        end_to_end_streaming: ['response_wire_bytes', 'bucket_operations']
      };
    case 'combined':
      return {
        replication_snapshot: [...replicationCounters, 'storage_put_operations'],
        replication_streaming: [...replicationCounters, 'storage_put_operations', 'source_transactions'],
        http_read: [
          'response_wire_bytes',
          'protocol_plaintext_bytes',
          'response_lines',
          'client_operations',
          'client_put_operations'
        ],
        end_to_end_snapshot: ['client_operations'],
        end_to_end_streaming: ['client_operations']
      };
  }
}
