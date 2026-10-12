import type { ReplicationBenchmarkManifest, ReplicationBenchmarkPhase } from '../types/ReplicationBenchmark.js';
import type { OperationEvidence } from './operation-evidence.js';

/** Counters for the timed phase. Storage evidence includes the untimed snapshot. */
export function replicationWorkCounters(
  phase: ReplicationBenchmarkPhase,
  manifest: ReplicationBenchmarkManifest,
  evidence: OperationEvidence
): { source_rows: number; writer_save_calls: number; bucket_operations: number } {
  const rows = phase === 'snapshot' ? manifest.snapshotRowCount : manifest.streamingRowCount;
  return {
    source_rows: rows,
    writer_save_calls: rows,
    bucket_operations: evidence.byPhase[phase === 'snapshot' ? 'snapshot' : 'streaming'] ?? 0
  };
}
