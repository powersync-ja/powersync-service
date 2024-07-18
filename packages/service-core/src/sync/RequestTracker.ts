import { container } from '@powersync/lib-services-framework';
import { Metrics } from '../metrics/Metrics.js';

/**
 * Record sync stats per request stream.
 */
export class RequestTracker {
  operationsSynced = 0;
  dataSyncedBytes = 0;

  addOperationsSynced(operations: number) {
    this.operationsSynced += operations;
    container.getImplementation(Metrics).operations_synced_total.add(operations);
  }

  addDataSynced(bytes: number) {
    this.dataSyncedBytes += bytes;

    container.getImplementation(Metrics).data_synced_bytes.add(bytes);
  }
}
