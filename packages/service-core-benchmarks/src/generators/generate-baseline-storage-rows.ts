import { StorageBenchmarkItem, StorageBenchmarkManifest, StorageBenchmarkWorkload } from '../types/StorageBenchmark.js';

const BASE_TIMESTAMP_MS = Date.parse('2020-01-01T00:00:00.000Z');

export function generateBaselineStorageRows(workload: StorageBenchmarkWorkload): StorageBenchmarkManifest {
  validateWorkload(workload);

  const rows = Array.from({ length: workload.row_count }, (_, index) => createRow(index, workload.payload_bytes));
  return {
    rows,
    sourceLogicalBytes: rows.reduce((total, row) => total + Buffer.byteLength(JSON.stringify(row), 'utf8'), 0),
    payloadBytes: rows.reduce((total, row) => total + Buffer.byteLength(row.payload, 'utf8'), 0)
  };
}

function createRow(index: number, payloadBytes: number): StorageBenchmarkItem {
  const id = `item-${index.toString().padStart(6, '0')}`;
  const payloadPrefix = `${id}|`;
  return {
    id,
    owner_id: `owner-${index % 10}`,
    category: `category-${index % 5}`,
    version: 1,
    updated_at: new Date(BASE_TIMESTAMP_MS + index * 1_000).toISOString(),
    payload: (payloadPrefix + 'x'.repeat(payloadBytes)).slice(0, payloadBytes)
  };
}

function validateWorkload(workload: StorageBenchmarkWorkload): void {
  if (!Number.isInteger(workload.row_count) || workload.row_count <= 0) {
    throw new RangeError('Storage benchmark row_count must be a positive integer');
  }
  if (!Number.isInteger(workload.payload_bytes) || workload.payload_bytes <= 0) {
    throw new RangeError('Storage benchmark payload_bytes must be a positive integer');
  }
}
