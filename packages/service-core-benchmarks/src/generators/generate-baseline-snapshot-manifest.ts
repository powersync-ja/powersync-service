import { SnapshotBenchmarkItem, SnapshotBenchmarkManifest } from '../types/SnapshotBenchmark.js';

const START_TIME = Date.parse('2020-01-01T00:00:00.000Z');

export interface SnapshotBenchmarkWorkload {
  readonly snapshot_row_count: number;
  readonly payload_bytes: number;
}

export function generateBaselineSnapshotManifest(
  workload: SnapshotBenchmarkWorkload,
  markTarget = true
): SnapshotBenchmarkManifest {
  validateWorkload(workload);
  const snapshotRows = Array.from({ length: workload.snapshot_row_count }, (_, index) =>
    createRow(index, workload.payload_bytes, markTarget && index === workload.snapshot_row_count - 1)
  );

  return {
    snapshotRows,
    target: {
      markerId: snapshotRows.at(-1)!.id,
      nativePosition: positionFor(1)
    },
    sourceLogicalBytes: snapshotRows.reduce((total, row) => total + Buffer.byteLength(JSON.stringify(row), 'utf8'), 0),
    payloadBytes: snapshotRows.length * workload.payload_bytes,
    expectedPutCount: snapshotRows.length
  };
}

function validateWorkload(workload: SnapshotBenchmarkWorkload): void {
  const { snapshot_row_count, payload_bytes } = workload;
  if (![snapshot_row_count, payload_bytes].every(Number.isInteger)) {
    throw new Error('Snapshot workload values must be integers');
  }
  if (snapshot_row_count <= 0 || payload_bytes <= 0) {
    throw new Error('Snapshot workload values must be positive');
  }
}

function createRow(index: number, payloadBytes: number, target: boolean): SnapshotBenchmarkItem {
  return {
    id: `snapshot-${index.toString().padStart(8, '0')}`,
    owner_id: `owner-${(index % 100).toString().padStart(3, '0')}`,
    category: `category-${index % 10}`,
    version: 1,
    updated_at: new Date(START_TIME + index * 1_000).toISOString(),
    payload: String.fromCharCode(97 + (index % 26)).repeat(payloadBytes),
    is_target: target ? 1 : 0
  };
}

function positionFor(index: number): string {
  return index.toString().padStart(20, '0');
}
