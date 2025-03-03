import { MetricsEngine } from '../metrics/MetricsEngine.js';

export enum APIMetricType {
  // Uncompressed size of synced data from PowerSync to Clients
  DATA_SYNCED_BYTES = 'powersync_data_synced_bytes_total',
  // Number of operations synced
  OPERATIONS_SYNCED_TOTAL = 'powersync_operations_synced_total',
  // Number of concurrent sync connections
  CONCURRENT_CONNECTIONS = 'powersync_concurrent_connections'
}

/**
 *  Create and register the core API metrics.
 *  @param engine
 */
export function createCoreAPIMetrics(engine: MetricsEngine): void {
  engine.createCounter({
    name: APIMetricType.DATA_SYNCED_BYTES,
    description: 'Uncompressed size of synced data',
    unit: 'bytes'
  });

  engine.createCounter({
    name: APIMetricType.OPERATIONS_SYNCED_TOTAL,
    description: 'Number of operations synced'
  });

  engine.createUpDownCounter({
    name: APIMetricType.CONCURRENT_CONNECTIONS,
    description: 'Number of concurrent sync connections'
  });
}

/**
 *  Initialise the core API metrics. This should be called after the metrics have been created.
 *  @param engine
 */
export function initializeCoreAPIMetrics(engine: MetricsEngine): void {
  const concurrent_connections = engine.getUpDownCounter(APIMetricType.CONCURRENT_CONNECTIONS);

  // Initialize the metric, so that it reports a value before connections have been opened.
  concurrent_connections.add(0);
}
