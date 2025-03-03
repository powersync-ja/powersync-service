import { MetricsEngine } from '../metrics/metrics-index.js';

export enum ReplicationMetricType {
  // Uncompressed size of replicated data from data source to PowerSync
  DATA_REPLICATED_BYTES = 'powersync_data_replicated_bytes_total',
  // Total number of replicated rows. Not used for pricing.
  ROWS_REPLICATED_TOTAL = 'powersync_rows_replicated_total',
  // Total number of replicated transactions. Not used for pricing.
  TRANSACTIONS_REPLICATED_TOTAL = 'powersync_transactions_replicated_total',
  // Total number of replication chunks. Not used for pricing.
  CHUNKS_REPLICATED_TOTAL = 'powersync_chunks_replicated_total'
}

/**
 *  Create and register the core replication metrics.
 *  @param engine
 */
export function createCoreReplicationMetrics(engine: MetricsEngine): void {
  engine.createCounter({
    name: ReplicationMetricType.DATA_REPLICATED_BYTES,
    description: 'Uncompressed size of replicated data',
    unit: 'bytes'
  });

  engine.createCounter({
    name: ReplicationMetricType.ROWS_REPLICATED_TOTAL,
    description: 'Total number of replicated rows'
  });

  engine.createCounter({
    name: ReplicationMetricType.TRANSACTIONS_REPLICATED_TOTAL,
    description: 'Total number of replicated transactions'
  });

  engine.createCounter({
    name: ReplicationMetricType.CHUNKS_REPLICATED_TOTAL,
    description: 'Total number of replication chunks'
  });
}

/**
 *  Initialise the core replication metrics. This should be called after the metrics have been created.
 *  @param engine
 */
export function initializeCoreReplicationMetrics(engine: MetricsEngine): void {
  const data_replicated_bytes = engine.getCounter(ReplicationMetricType.DATA_REPLICATED_BYTES);
  const rows_replicated_total = engine.getCounter(ReplicationMetricType.ROWS_REPLICATED_TOTAL);
  const transactions_replicated_total = engine.getCounter(ReplicationMetricType.TRANSACTIONS_REPLICATED_TOTAL);
  const chunks_replicated_total = engine.getCounter(ReplicationMetricType.CHUNKS_REPLICATED_TOTAL);

  data_replicated_bytes.add(0);
  rows_replicated_total.add(0);
  transactions_replicated_total.add(0);
  chunks_replicated_total.add(0);
}
