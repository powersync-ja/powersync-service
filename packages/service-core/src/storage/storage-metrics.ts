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
    name: StorageMetric.REPLICATION_SIZE_BYTES_BY_STREAM,
    description: 'Size of current data stored in PowerSync by replication stream',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.OPERATION_SIZE_BYTES,
    description: 'Size of operations stored in PowerSync',
    unit: 'bytes'
  });
  engine.createObservableGauge({
    name: StorageMetric.OPERATION_SIZE_BYTES_BY_STREAM,
    description: 'Size of operations stored in PowerSync by replication stream',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.PARAMETER_SIZE_BYTES,
    description: 'Size of parameter data stored in PowerSync',
    unit: 'bytes'
  });
  engine.createObservableGauge({
    name: StorageMetric.PARAMETER_SIZE_BYTES_BY_STREAM,
    description: 'Size of parameter data stored in PowerSync by replication stream',
    unit: 'bytes'
  });

  engine.createObservableGauge({
    name: StorageMetric.OBJECT_STORAGE_SIZE_BYTES,
    description: 'Size of active object-storage references stored in PowerSync',
    unit: 'bytes'
  });
  engine.createObservableGauge({
    name: StorageMetric.OBJECT_STORAGE_SIZE_BYTES_BY_STREAM,
    description: 'Size of active object-storage references by replication stream',
    unit: 'bytes'
  });
}

export function initializeCoreStorageMetrics(engine: MetricsEngine, storage: BucketStorageFactory): void {
  const replication_storage_size_bytes = engine.getObservableGauge(StorageMetric.REPLICATION_SIZE_BYTES);
  const replication_storage_size_bytes_by_stream = engine.getObservableGauge(
    StorageMetric.REPLICATION_SIZE_BYTES_BY_STREAM
  );
  const operation_storage_size_bytes = engine.getObservableGauge(StorageMetric.OPERATION_SIZE_BYTES);
  const operation_storage_size_bytes_by_stream = engine.getObservableGauge(
    StorageMetric.OPERATION_SIZE_BYTES_BY_STREAM
  );
  const parameter_storage_size_bytes = engine.getObservableGauge(StorageMetric.PARAMETER_SIZE_BYTES);
  const parameter_storage_size_bytes_by_stream = engine.getObservableGauge(
    StorageMetric.PARAMETER_SIZE_BYTES_BY_STREAM
  );
  const object_storage_size_bytes = engine.getObservableGauge(StorageMetric.OBJECT_STORAGE_SIZE_BYTES);
  const object_storage_size_bytes_by_stream = engine.getObservableGauge(
    StorageMetric.OBJECT_STORAGE_SIZE_BYTES_BY_STREAM
  );

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
    | 'operations_size_bytes'
    | 'parameters_size_bytes'
    | 'replication_size_bytes'
    | 'object_storage_size_bytes';

  const observationsForStreams = (metrics: StorageMetrics, key: StorageSizeMetricKey) => {
    const observations: { value: number; attributes?: Record<string, string> }[] = [];
    for (const stream of metrics.stream_metrics ?? []) {
      observations.push({
        value: stream[key],
        attributes: {
          replication_stream_id: String(stream.replication_stream_id),
          stream_state: stream.stream_state
        }
      });
    }
    return observations;
  };

  replication_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return metrics?.replication_size_bytes;
  });

  replication_storage_size_bytes_by_stream.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForStreams(metrics, 'replication_size_bytes');
    }
  });

  operation_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return metrics?.operations_size_bytes;
  });

  operation_storage_size_bytes_by_stream.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForStreams(metrics, 'operations_size_bytes');
    }
  });

  parameter_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return metrics?.parameters_size_bytes;
  });

  parameter_storage_size_bytes_by_stream.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForStreams(metrics, 'parameters_size_bytes');
    }
  });

  object_storage_size_bytes.setValueProvider(async () => {
    const metrics = await getMetrics();
    return metrics?.object_storage_size_bytes;
  });

  object_storage_size_bytes_by_stream.setValueProvider(async () => {
    const metrics = await getMetrics();
    if (metrics) {
      return observationsForStreams(metrics, 'object_storage_size_bytes');
    }
  });
}
