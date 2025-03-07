import { MetricsEngine } from '../metrics/MetricsEngine.js';

import { APIMetric } from '@powersync/service-types';

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

    this.metrics.getCounter(APIMetric.OPERATIONS_SYNCED).add(operations);
  }

  addDataSynced(bytes: number) {
    this.dataSyncedBytes += bytes;

    this.metrics.getCounter(APIMetric.DATA_SYNCED_BYTES).add(bytes);
  }
}
