import { storage } from '@powersync/service-core';
import type { BenchmarkRow } from '../generators/benchmark-data.js';

export interface ObservedOperation {
  readonly op: string;
  readonly object_id?: string;
  readonly data?: unknown;
}
export interface MarkerEvidence {
  count: number;
  byStream: Record<string, number>;
  operation: ObservedOperation;
  operationsByStream: Record<string, ObservedOperation>;
}
export interface OperationEvidence {
  operationCount: number;
  putCount: number;
  dataBytes: number;
  byBucket: Record<string, number>;
  byTable: Record<string, number>;
  byStream: Record<string, number>;
  byPhase: Record<string, number>;
  phaseDataBytes: Record<string, number>;
  markers: Record<string, MarkerEvidence>;
  first?: ObservedOperation;
  last?: ObservedOperation;
}

export class OperationAccumulator {
  private readonly markerIds: Set<string>;
  private readonly result: OperationEvidence = {
    operationCount: 0,
    putCount: 0,
    dataBytes: 0,
    byBucket: Object.create(null),
    byTable: Object.create(null),
    byStream: Object.create(null),
    byPhase: Object.create(null),
    phaseDataBytes: Object.create(null),
    markers: Object.create(null)
  };
  constructor(markerIds: readonly string[] = []) {
    this.markerIds = new Set(markerIds);
  }
  add(bucket: string, operation: ObservedOperation): void {
    const result = this.result;
    // Retain only the fields used for verification; bucket APIs add large or non-JSON metadata.
    const sample: ObservedOperation = { op: operation.op, object_id: operation.object_id, data: operation.data };
    const bytes = typeof operation.data === 'string' ? Buffer.byteLength(operation.data, 'utf8') : 0;
    result.operationCount++;
    result.putCount += operation.op === 'PUT' ? 1 : 0;
    result.dataBytes += bytes;
    result.first ??= sample;
    result.last = sample;
    const stream = streamNameForBucket(bucket);
    increment(result.byBucket, bucket, 1);
    increment(result.byStream, stream, 1);
    const [table, phase] = operation.object_id?.split(':') ?? [];
    if (table) increment(result.byTable, table, 1);
    if (phase) {
      increment(result.byPhase, phase, 1);
      increment(result.phaseDataBytes, phase, bytes);
    }
    if (operation.object_id && this.markerIds.has(operation.object_id)) {
      const marker = (result.markers[operation.object_id] ??= {
        count: 0,
        byStream: Object.create(null),
        operation: sample,
        operationsByStream: Object.create(null)
      });
      marker.count++;
      increment(marker.byStream, stream, 1);
      marker.operationsByStream[stream] ??= sample;
    }
  }
  merge(evidence: OperationEvidence): void {
    const result = this.result;
    result.operationCount += evidence.operationCount;
    result.putCount += evidence.putCount;
    result.dataBytes += evidence.dataBytes;
    result.first ??= evidence.first;
    result.last = evidence.last ?? result.last;
    for (const field of ['byBucket', 'byTable', 'byStream', 'byPhase', 'phaseDataBytes'] as const) {
      for (const [key, count] of Object.entries(evidence[field])) increment(result[field], key, count);
    }
    for (const [id, incoming] of Object.entries(evidence.markers)) {
      const marker = (result.markers[id] ??= {
        count: 0,
        byStream: Object.create(null),
        operation: incoming.operation,
        operationsByStream: Object.create(null)
      });
      marker.count += incoming.count;
      for (const [stream, count] of Object.entries(incoming.byStream)) increment(marker.byStream, stream, count);
      for (const [stream, operation] of Object.entries(incoming.operationsByStream))
        marker.operationsByStream[stream] ??= operation;
    }
  }
  summary(): OperationEvidence {
    return this.result;
  }
}

/** Remove storage hydration IDs and Sync Stream query IDs from the logical definition name. */
export function streamNameForBucket(bucket: string): string {
  // MongoDB v3+ appends hexadecimal replication stream and bucket definition IDs.
  return bucket
    .replace(/^\d+#/, '')
    .split(/[|\[]/, 1)[0]
    .replace(/\.[0-9a-f]+\.[0-9a-f]+$/, '');
}

function increment(counts: Record<string, number>, key: string, count: number): void {
  counts[key] = (counts[key] ?? 0) + count;
}

export function operationMatchesRow(operation: ObservedOperation | undefined, expected: BenchmarkRow): boolean {
  if (operation?.op !== 'PUT' || typeof operation.data !== 'string') return false;
  try {
    const actual = JSON.parse(operation.data);
    return Object.entries(expected).every(([key, value]) => actual[key] === value);
  } catch {
    return false;
  }
}

/** Incrementally traverse the existing bucket API, including its batch-end pagination marker. */
export async function readBucketEvidence(options: {
  storage: storage.SyncRulesBucketStorage;
  checkpoint: storage.ReplicationCheckpoint;
  requests: readonly storage.BucketDataRequest[];
  markerIds: readonly string[];
  signal?: AbortSignal;
}): Promise<OperationEvidence> {
  const accumulator = new OperationAccumulator(options.markerIds);
  const remaining = new Map(options.requests.map((request) => [request.bucket, request]));
  while (remaining.size) {
    options.signal?.throwIfAborted();
    let hasMore = false;
    for await (const chunk of options.storage.getBucketDataBatch(options.checkpoint, [...remaining.values()])) {
      options.signal?.throwIfAborted();
      if (storage.isBatchEnd(chunk)) {
        if (!chunk.hasMore) return accumulator.summary();
        hasMore = true;
        break;
      }
      for (const operation of chunk.chunkData.data) accumulator.add(chunk.chunkData.bucket, operation);
      if (chunk.chunkData.has_more) {
        const request = remaining.get(chunk.chunkData.bucket)!;
        remaining.set(chunk.chunkData.bucket, { ...request, start: BigInt(chunk.chunkData.next_after) });
        hasMore = true;
      } else remaining.delete(chunk.chunkData.bucket);
    }
    if (!hasMore) break;
  }
  return accumulator.summary();
}
