import { MetricsEngine } from '../metrics/MetricsEngine.js';
import { logger } from '@powersync/lib-services-framework';
import { BucketStorageFactory, StorageMetrics } from './BucketStorageFactory.js';
import { StorageMetric } from '@powersync/service-types';

export function createCoreStorageMetrics(engine: MetricsEngine): void {
  engine.createObservableGauge({
    name: StorageMetric.REPLICATION_SIZE_BYTES,
    description: 'Size of current data stored in PowerSync',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.OPERATION_SIZE_BYTES,
    description: 'Size of operations stored in PowerSync',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.PARAMETER_SIZE_BYTES,
    description: 'Size of parameter data stored in PowerSync',
    unit: 'bytes'
  });
}

export function initializeCoreStorageMetrics(engine: MetricsEngine, storage: BucketStorageFactory): void {
  const replication_storage_size_bytes = engine.getObservableGauge(StorageMetric.REPLICATION_SIZE_BYTES);
  const operation_storage_size_bytes = engine.getObservableGauge(StorageMetric.OPERATION_SIZE_BYTES);
  const parameter_storage_size_bytes = engine.getObservableGauge(StorageMetric.PARAMETER_SIZE_BYTES);

  const MINIMUM_INTERVAL = 60_000;

  let cachedRequest: Promise<StorageMetrics | null> | undefined = undefined;
  let cacheTimestamp = 0;

  const getMetrics = () => {
    if (!cachedRequest || Date.now() - cacheTimestamp > MINIMUM_INTERVAL) {
      cachedRequest = storage.getStorageMetrics().catch((e) => {
        logger.error(`Failed to get storage metrics`, e);
        return null;
      });
      cacheTimestamp = Date.now();
    }
    return cachedRequest;
  };

  replication_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return metrics.replication_size_bytes ?? 0;
    }
  });

  operation_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return metrics.operations_size_bytes ?? 0;
    }
  });

  parameter_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return metrics.parameters_size_bytes ?? 0;
    }
  });
}
