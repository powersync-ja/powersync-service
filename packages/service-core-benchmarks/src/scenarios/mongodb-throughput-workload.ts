import type {
  ReplicationBenchmarkItem,
  ReplicationBenchmarkManifest,
  ReplicationBenchmarkScenario,
  ReplicationBenchmarkTransaction
} from '../types/ReplicationBenchmark.js';
import { generatedSequence } from '../utils/generated-sequence.js';
import { sampleOpDocument } from './sample-op-document.js';

/** Edit this function to model your documents. Keep id/is_target for verification.
 * It is evaluated repeatedly: use deterministic values, not randomUUID() or Date.now().
 * MongoDB _id is set to id by the source adapter.
 */
export function createDocument(index: number, revision: number, payloadBytes: number): ReplicationBenchmarkItem {
  return {
    id: `row-${index}`,
    owner_id: `owner-${index % 100}`,
    category: `category-${index % 10}`,
    version: revision,
    updated_at: new Date(Date.UTC(2026, 0, 1) + index * 1000 + revision).toISOString(),
    payload: process.env.BENCHMARK_SHAPE === 'synthetic' ? 'x'.repeat(payloadBytes) : '',
    ...(process.env.BENCHMARK_SHAPE === 'synthetic' ? {} : sampleOpDocument(index, revision)),
    benchmark_user: `user-${index % Number(process.env.BENCHMARK_USERS ?? 100)}`,
    metadata: { enabled: index % 2 === 0, tags: ['benchmark', `group-${index % 10}`] },
    is_target: 0
  };
}

export type MutationMode = 'insert' | 'update' | 'delete' | 'mixed';

/** Snapshot and transaction data are regenerated in bounded batches, never retained as a full dataset. */
export function createMongoThroughputManifest(
  scenario: ReplicationBenchmarkScenario,
  mode: MutationMode
): ReplicationBenchmarkManifest {
  const {
    snapshot_row_count: rows,
    streaming_mutation_count: mutations,
    transaction_count: transactions,
    payload_bytes: bytes
  } = scenario.workload;
  if (mutations % transactions !== 0) throw new Error('Mutation count must be divisible by transaction count');
  if (mode !== 'insert' && mutations > rows)
    throw new Error('Update/delete workloads need at least as many snapshot rows as mutations');
  const snapshotRows = generatedSequence(rows, (index) => ({
    ...createDocument(index, 0, bytes),
    is_target: scenario.phase === 'snapshot' && index === rows - 1 ? 1 : 0
  }));
  const perTransaction = mutations / transactions;
  const batches = generatedSequence<ReplicationBenchmarkTransaction>(transactions, (batch) => ({
    id: `transaction-${batch}`,
    position: String(batch + 2).padStart(20, '0'),
    mutations: Array.from({ length: perTransaction }, (_, offset) => {
      const index = batch * perTransaction + offset;
      // Each transaction ends with an inserted target, even for delete-only workloads.
      const target = offset === perTransaction - 1;
      const tag = target ? 'insert' : mode === 'mixed' ? (['insert', 'update', 'delete'] as const)[index % 3] : mode;
      return {
        tag,
        row: { ...createDocument(tag === 'insert' ? rows + index : index, 1, bytes), is_target: target ? 1 : 0 }
      };
    })
  }));
  let sourceLogicalBytes = 0;
  let expectedPutCount = rows;
  if (scenario.phase === 'snapshot') {
    for (const row of snapshotRows) sourceLogicalBytes += Buffer.byteLength(JSON.stringify(row));
  } else {
    for (const transaction of batches)
      for (const mutation of transaction.mutations) {
        sourceLogicalBytes += Buffer.byteLength(JSON.stringify(mutation.row));
        if (mutation.tag !== 'delete') expectedPutCount++;
      }
  }
  return {
    snapshotRows,
    transactions: batches,
    target: {
      markerId: scenario.phase === 'snapshot' ? snapshotRows.at(-1)!.id : batches.at(-1)!.mutations.at(-1)!.row.id,
      nativePosition: null
    },
    sourceLogicalBytes,
    payloadBytes:
      process.env.BENCHMARK_SHAPE !== 'synthetic' ? 0 : (scenario.phase === 'snapshot' ? rows : mutations) * bytes,
    expectedPutCount
  };
}
