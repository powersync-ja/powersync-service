import {
  ReplicationBenchmarkItem,
  ReplicationBenchmarkManifest,
  ReplicationBenchmarkScenario,
  ReplicationBenchmarkTransaction
} from '../types/ReplicationBenchmark.js';
import { generateBaselineSnapshotManifest } from './generate-baseline-snapshot-manifest.js';

const START_TIME = Date.parse('2020-01-01T00:00:00.000Z');

export function generateBaselineReplicationManifest(
  scenario: ReplicationBenchmarkScenario
): ReplicationBenchmarkManifest {
  validateWorkload(scenario);
  const snapshotManifest = generateBaselineSnapshotManifest(scenario.workload, scenario.phase === 'snapshot');
  const snapshotRows = snapshotManifest.snapshotRows;
  const mutationsPerTransaction = scenario.workload.streaming_mutation_count / scenario.workload.transaction_count;
  const transactions: ReplicationBenchmarkTransaction[] = [];
  for (let transactionIndex = 0; transactionIndex < scenario.workload.transaction_count; transactionIndex++) {
    const mutations = Array.from({ length: mutationsPerTransaction }, (_, mutationIndex) => {
      const rowIndex = transactionIndex * mutationsPerTransaction + mutationIndex;
      return {
        tag: 'insert' as const,
        row: createStreamingRow(
          rowIndex,
          scenario.workload.payload_bytes,
          mutationIndex === mutationsPerTransaction - 1
        )
      };
    });
    transactions.push({
      id: `transaction-${transactionIndex.toString().padStart(4, '0')}`,
      position: positionFor(transactionIndex + 2),
      mutations
    });
  }

  const targetRow =
    scenario.phase === 'snapshot' ? snapshotManifest.snapshotRows.at(-1)! : transactions.at(-1)!.mutations.at(-1)!.row;
  const streamingRows = transactions.flatMap((transaction) => transaction.mutations.map((item) => item.row));
  const measuredRows = scenario.phase === 'snapshot' ? snapshotManifest.snapshotRows : streamingRows;
  return {
    snapshotRows,
    transactions,
    target: {
      markerId: targetRow.id,
      nativePosition:
        scenario.phase === 'snapshot' ? snapshotManifest.target.nativePosition : transactions.at(-1)!.position
    },
    sourceLogicalBytes:
      scenario.phase === 'snapshot'
        ? snapshotManifest.sourceLogicalBytes
        : measuredRows.reduce((total, row) => total + Buffer.byteLength(JSON.stringify(row), 'utf8'), 0),
    payloadBytes:
      scenario.phase === 'snapshot'
        ? snapshotManifest.payloadBytes
        : measuredRows.length * scenario.workload.payload_bytes,
    expectedPutCount:
      scenario.phase === 'snapshot'
        ? snapshotManifest.expectedPutCount
        : snapshotManifest.expectedPutCount + streamingRows.length
  };
}

function validateWorkload(scenario: ReplicationBenchmarkScenario): void {
  const { snapshot_row_count, streaming_mutation_count, transaction_count, payload_bytes } = scenario.workload;
  if (![snapshot_row_count, streaming_mutation_count, transaction_count, payload_bytes].every(Number.isInteger)) {
    throw new Error('Replication workload values must be integers');
  }
  if (snapshot_row_count <= 0 || streaming_mutation_count <= 0 || transaction_count <= 0 || payload_bytes <= 0) {
    throw new Error('Replication workload values must be positive');
  }
  if (streaming_mutation_count % transaction_count !== 0) {
    throw new Error('streaming_mutation_count must be divisible by transaction_count');
  }
}

function createStreamingRow(index: number, payloadBytes: number, target: boolean): ReplicationBenchmarkItem {
  return {
    id: `stream-${index.toString().padStart(8, '0')}`,
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
