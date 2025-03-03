import { MetricsEngine } from '../metrics/MetricsEngine.js';
import { APIMetricType } from '../api/api-metrics.js';

/**
 * Record sync stats per request stream.
 */
export class RequestTracker {
  operationsSynced = 0;
  dataSyncedBytes = 0;

  constructor(private metrics: MetricsEngine) {
    this.metrics = metrics;
  }

  addOperationsSynced(operations: number) {
    this.operationsSynced += operations;

    this.metrics.getCounter(APIMetricType.OPERATIONS_SYNCED_TOTAL).add(operations);
  }

  addDataSynced(bytes: number) {
    this.dataSyncedBytes += bytes;

    this.metrics.getCounter(APIMetricType.DATA_SYNCED_BYTES).add(bytes);
  }
}
