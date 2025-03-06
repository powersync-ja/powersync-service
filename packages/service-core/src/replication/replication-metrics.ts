import { MetricsEngine } from '../metrics/metrics-index.js';
import { ReplicationMetric } from '@powersync/service-types';

/**
 *  Create and register the core replication metrics.
 *  @param engine
 */
export function createCoreReplicationMetrics(engine: MetricsEngine): void {
  engine.createCounter({
    name: ReplicationMetric.DATA_REPLICATED_BYTES,
    description: 'Uncompressed size of replicated data',
    unit: 'bytes'
  });

  engine.createCounter({
    name: ReplicationMetric.ROWS_REPLICATED_TOTAL,
    description: 'Total number of replicated rows'
  });

  engine.createCounter({
    name: ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL,
    description: 'Total number of replicated transactions'
  });

  engine.createCounter({
    name: ReplicationMetric.CHUNKS_REPLICATED_TOTAL,
    description: 'Total number of replication chunks'
  });
}

/**
 *  Initialise the core replication metrics. This should be called after the metrics have been created.
 *  @param engine
 */
export function initializeCoreReplicationMetrics(engine: MetricsEngine): void {
  const data_replicated_bytes = engine.getCounter(ReplicationMetric.DATA_REPLICATED_BYTES);
  const rows_replicated_total = engine.getCounter(ReplicationMetric.ROWS_REPLICATED_TOTAL);
  const transactions_replicated_total = engine.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED_TOTAL);
  const chunks_replicated_total = engine.getCounter(ReplicationMetric.CHUNKS_REPLICATED_TOTAL);

  data_replicated_bytes.add(0);
  rows_replicated_total.add(0);
  transactions_replicated_total.add(0);
  chunks_replicated_total.add(0);
}
