import { logger } from '@powersync/lib-services-framework';
import { StorageMetric } from '@powersync/service-types';
import { MetricsEngine } from '../metrics/MetricsEngine.js';
import { BucketStorageFactory, StorageMetrics } from './BucketStorageFactory.js';

export function createCoreStorageMetrics(engine: MetricsEngine): void {
  engine.createObservableGauge({
    name: StorageMetric.REPLICATION_SIZE_BYTES,
    description: 'Size of current data stored in PowerSync',
    unit: 'bytes'
  });
  engine.createObservableGauge({
    name: StorageMetric.ATTRIBUTED_SOURCE_RECORDS_BYTES,
    description: 'Size of current data stored in PowerSync by sync config',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.OPERATION_SIZE_BYTES,
    description: 'Size of operations stored in PowerSync',
    unit: 'bytes'
  });
  engine.createObservableGauge({
    name: StorageMetric.ATTRIBUTED_BUCKET_DATA_BYTES,
    description: 'Size of operations stored in PowerSync by sync config',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.PARAMETER_SIZE_BYTES,
    description: 'Size of parameter data stored in PowerSync',
    unit: 'bytes'
  });
  engine.createObservableGauge({
    name: StorageMetric.ATTRIBUTED_PARAMETER_INDEXES_BYTES,
    description: 'Size of parameter data stored in PowerSync by sync config',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.OBJECT_STORAGE_SIZE_BYTES,
    description: 'Size of active object-storage references stored in PowerSync',
    unit: 'bytes'
  });
  engine.createObservableGauge({
    name: StorageMetric.ATTRIBUTED_OBJECT_STORAGE_BYTES,
    description: 'Size of active object-storage references by sync config',
    unit: 'bytes'
  });
}

export function initializeCoreStorageMetrics(engine: MetricsEngine, storage: BucketStorageFactory): void {
  const replication_storage_size_bytes = engine.getObservableGauge(StorageMetric.REPLICATION_SIZE_BYTES);
  const attributed_source_records_bytes = engine.getObservableGauge(StorageMetric.ATTRIBUTED_SOURCE_RECORDS_BYTES);
  const operation_storage_size_bytes = engine.getObservableGauge(StorageMetric.OPERATION_SIZE_BYTES);
  const attributed_bucket_data_bytes = engine.getObservableGauge(StorageMetric.ATTRIBUTED_BUCKET_DATA_BYTES);
  const parameter_storage_size_bytes = engine.getObservableGauge(StorageMetric.PARAMETER_SIZE_BYTES);
  const attributed_parameter_indexes_bytes = engine.getObservableGauge(
    StorageMetric.ATTRIBUTED_PARAMETER_INDEXES_BYTES
  );
  const object_storage_size_bytes = engine.getObservableGauge(StorageMetric.OBJECT_STORAGE_SIZE_BYTES);
  const attributed_object_storage_bytes = engine.getObservableGauge(StorageMetric.ATTRIBUTED_OBJECT_STORAGE_BYTES);

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

  type StorageSizeMetricKey =
    | 'attributed_bucket_data_bytes'
    | 'attributed_parameter_indexes_bytes'
    | 'attributed_source_records_bytes'
    | 'attributed_object_storage_bytes';

  function nonNegative(value: number): number;
  function nonNegative(value: number | undefined): number | undefined;
  function nonNegative(value: number | undefined) {
    return value == null ? value : Math.max(0, value);
  }

  const observationsForSyncConfigs = (metrics: StorageMetrics, key: StorageSizeMetricKey) => {
    const observations: { value: number; attributes?: Record<string, string> }[] = [];
    for (const syncConfig of metrics.sync_config_metrics ?? []) {
      observations.push({
        value: nonNegative(syncConfig[key]),
        attributes: {
          sync_config_id: syncConfig.sync_config_id,
          sync_config_state: syncConfig.sync_config_state
        }
      });
    }
    return observations;
  };

  replication_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return nonNegative(metrics?.replication_size_bytes);
  });

  attributed_source_records_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForSyncConfigs(metrics, 'attributed_source_records_bytes');
    }
  });

  operation_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return nonNegative(metrics?.operations_size_bytes);
  });

  attributed_bucket_data_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForSyncConfigs(metrics, 'attributed_bucket_data_bytes');
    }
  });

  parameter_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return nonNegative(metrics?.parameters_size_bytes);
  });

  attributed_parameter_indexes_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForSyncConfigs(metrics, 'attributed_parameter_indexes_bytes');
    }
  });

  object_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return nonNegative(metrics?.object_storage_size_bytes);
  });

  attributed_object_storage_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForSyncConfigs(metrics, 'attributed_object_storage_bytes');
    }
  });
}
