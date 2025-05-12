import { MetricsEngine } from '../metrics/MetricsEngine.js';

import { APIMetric } from '@powersync/service-types';
import { SyncBucketData } from '../util/protocol-types.js';

/**
 * Record sync stats per request stream.
 */
export class RequestTracker {
  operationsSynced = 0;
  dataSyncedBytes = 0;
  operationCounts: OperationCounts = { put: 0, remove: 0, move: 0, clear: 0 };
  largeBuckets: Record<string, number> = {};

  constructor(private metrics: MetricsEngine) {
    this.metrics = metrics;
  }

  addOperationsSynced(operations: OperationsSentStats) {
    this.operationsSynced += operations.total;
    this.operationCounts.put += operations.operations.put;
    this.operationCounts.remove += operations.operations.remove;
    this.operationCounts.move += operations.operations.move;
    this.operationCounts.clear += operations.operations.clear;
    if (operations.total > 100 || operations.bucket in this.largeBuckets) {
      this.largeBuckets[operations.bucket] = (this.largeBuckets[operations.bucket] ?? 0) + operations.total;
    }

    this.metrics.getCounter(APIMetric.OPERATIONS_SYNCED).add(operations.total);
  }

  addDataSynced(bytes: number) {
    this.dataSyncedBytes += bytes;

    this.metrics.getCounter(APIMetric.DATA_SYNCED_BYTES).add(bytes);
  }

  getLogMeta() {
    return {
      operations_synced: this.operationsSynced,
      data_synced_bytes: this.dataSyncedBytes,
      operation_counts: this.operationCounts,
      large_buckets: this.largeBuckets
    };
  }
}

export interface OperationCounts {
  put: number;
  remove: number;
  move: number;
  clear: number;
}

export interface OperationsSentStats {
  bucket: string;
  operations: OperationCounts;
  total: number;
}

export function statsForBatch(batch: SyncBucketData): OperationsSentStats {
  let put = 0;
  let remove = 0;
  let move = 0;
  let clear = 0;

  for (const entry of batch.data) {
    switch (entry.op) {
      case 'PUT':
        put++;
        break;
      case 'REMOVE':
        remove++;
        break;
      case 'MOVE':
        move++;
        break;
      case 'CLEAR':
        clear++;
        break;
    }
  }

  return {
    bucket: batch.bucket,
    operations: {
      put,
      remove,
      move,
      clear
    },
    total: put + remove + move + clear
  };
}
