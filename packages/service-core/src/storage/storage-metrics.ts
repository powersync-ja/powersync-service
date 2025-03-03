import { MetricsEngine } from '../metrics/MetricsEngine.js';
import { logger } from '@powersync/lib-services-framework';
import { BucketStorageFactory, StorageMetrics } from './BucketStorageFactory.js';

export enum StorageMetricType {
  // Size of current replication data stored in PowerSync
  REPLICATION_SIZE_BYTES = 'powersync_replication_storage_size_bytes',
  // Size of operations data stored in PowerSync
  OPERATION_SIZE_BYTES = 'powersync_operation_storage_size_bytes',
  // Size of parameter data stored in PowerSync
  PARAMETER_SIZE_BYTES = 'powersync_parameter_storage_size_bytes'
}

export function createCoreStorageMetrics(engine: MetricsEngine): void {
  engine.createObservableGauge({
    name: StorageMetricType.REPLICATION_SIZE_BYTES,
    description: 'Size of current data stored in PowerSync',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetricType.OPERATION_SIZE_BYTES,
    description: 'Size of operations stored in PowerSync',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetricType.PARAMETER_SIZE_BYTES,
    description: 'Size of parameter data stored in PowerSync',
    unit: 'bytes'
  });
}

export function initializeCoreStorageMetrics(engine: MetricsEngine, storage: BucketStorageFactory): void {
  const replication_storage_size_bytes = engine.getObservableGauge(StorageMetricType.REPLICATION_SIZE_BYTES);
  const operation_storage_size_bytes = engine.getObservableGauge(StorageMetricType.OPERATION_SIZE_BYTES);
  const parameter_storage_size_bytes = engine.getObservableGauge(StorageMetricType.PARAMETER_SIZE_BYTES);

  const MINIMUM_INTERVAL = 60_000;

  let cachedRequest: Promise<StorageMetrics | null> | undefined = undefined;
  let cacheTimestamp = 0;

  const getMetrics = () => {
    if (cachedRequest == null || Date.now() - cacheTimestamp > MINIMUM_INTERVAL) {
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
      return metrics.replication_size_bytes;
    }
  });

  operation_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return metrics.operations_size_bytes;
    }
  });

  parameter_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return metrics.parameters_size_bytes;
    }
  });
}
