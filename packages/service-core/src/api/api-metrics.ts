import { MetricsEngine } from '../metrics/MetricsEngine.js';
import { APIMetric } from '@powersync/service-types';

/**
 *  Create and register the core API metrics.
 *  @param engine
 */
export function createCoreAPIMetrics(engine: MetricsEngine): void {
  engine.createCounter({
    name: APIMetric.DATA_SYNCED_BYTES,
    description: 'Uncompressed size of synced data',
    unit: 'bytes'
  });

  engine.createCounter({
    name: APIMetric.OPERATIONS_SYNCED,
    description: 'Number of operations synced'
  });

  engine.createUpDownCounter({
    name: APIMetric.CONCURRENT_CONNECTIONS,
    description: 'Number of concurrent sync connections'
  });
}

/**
 *  Initialise the core API metrics. This should be called after the metrics have been created.
 *  @param engine
 */
export function initializeCoreAPIMetrics(engine: MetricsEngine): void {
  const concurrent_connections = engine.getUpDownCounter(APIMetric.CONCURRENT_CONNECTIONS);

  // Initialize the metric, so that it reports a value before connections have been opened.
  concurrent_connections.add(0);
}
