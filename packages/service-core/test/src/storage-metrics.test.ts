import { MetricsEngine } from '@/metrics/MetricsEngine.js';
import {
  MetricMetadata,
  MetricsFactory,
  ObservableGauge,
  ObservableGaugeObservation
} from '@/metrics/metrics-interfaces.js';
import { BucketStorageFactory, StorageMetrics, StorageSyncConfigMetrics } from '@/storage/BucketStorageFactory.js';
import { createCoreStorageMetrics, initializeCoreStorageMetrics } from '@/storage/storage-metrics.js';
import { StorageMetric } from '@powersync/service-types';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

type ValueProvider = () => Promise<number | ObservableGaugeObservation[] | undefined>;

function testEngine() {
  const providers = new Map<string, ValueProvider>();
  const factory: MetricsFactory = {
    createCounter: () => ({ add: () => {} }),
    createUpDownCounter: () => ({ add: () => {} }),
    createObservableGauge: (metadata: MetricMetadata): ObservableGauge => ({
      setValueProvider: (valueProvider) => providers.set(metadata.name, valueProvider)
    })
  };
  const engine = new MetricsEngine({ factory, disable_telemetry_sharing: true });
  createCoreStorageMetrics(engine);
  return { engine, providers };
}

function syncConfigMetrics(id: string, state: string, bytes: number): StorageSyncConfigMetrics {
  return {
    sync_config_id: id,
    sync_config_state: state,
    attributed_bucket_data_bytes: bytes,
    attributed_parameter_indexes_bytes: bytes,
    attributed_source_records_bytes: bytes,
    attributed_object_storage_bytes: bytes
  };
}

function storageMetrics(sync_config_metrics: StorageSyncConfigMetrics[]): StorageMetrics {
  return {
    operations_size_bytes: 0,
    parameters_size_bytes: 0,
    replication_size_bytes: 0,
    object_storage_size_bytes: 0,
    sync_config_metrics
  };
}

describe('storage metrics', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(0);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('reports retired sync configs as zero, then stops reporting them', async () => {
    let current = storageMetrics([syncConfigMetrics('config-a', 'ACTIVE', 100)]);
    const storage = {
      getStorageMetrics: async () => current
    } as unknown as BucketStorageFactory;

    const { engine, providers } = testEngine();
    initializeCoreStorageMetrics(engine, storage);
    const provider = providers.get(StorageMetric.ATTRIBUTED_BUCKET_DATA_BYTES)!;

    expect(await provider()).toEqual([
      { value: 100, attributes: { sync_config_id: 'config-a', sync_config_state: 'ACTIVE' } }
    ]);

    // The sync config is pruned. Its series must go to zero rather than keep its last value,
    // since the SDK carries the last reported value forward under cumulative temporality.
    current = storageMetrics([syncConfigMetrics('config-b', 'ACTIVE', 50)]);
    vi.setSystemTime(61_000);

    expect(await provider()).toEqual([
      { value: 50, attributes: { sync_config_id: 'config-b', sync_config_state: 'ACTIVE' } },
      { value: 0, attributes: { sync_config_id: 'config-a', sync_config_state: 'ACTIVE' } }
    ]);

    // Once every reader has had a chance to observe the zero, stop reporting it entirely.
    vi.setSystemTime(61_000 + 16 * 60_000);

    expect(await provider()).toEqual([
      { value: 50, attributes: { sync_config_id: 'config-b', sync_config_state: 'ACTIVE' } }
    ]);
  });

  it('treats a state transition as a retired attribute set', async () => {
    let current = storageMetrics([syncConfigMetrics('config-a', 'PROCESSING', 100)]);
    const storage = {
      getStorageMetrics: async () => current
    } as unknown as BucketStorageFactory;

    const { engine, providers } = testEngine();
    initializeCoreStorageMetrics(engine, storage);
    const provider = providers.get(StorageMetric.ATTRIBUTED_SOURCE_RECORDS_BYTES)!;

    await provider();

    current = storageMetrics([syncConfigMetrics('config-a', 'ACTIVE', 120)]);
    vi.setSystemTime(61_000);

    expect(await provider()).toEqual([
      { value: 120, attributes: { sync_config_id: 'config-a', sync_config_state: 'ACTIVE' } },
      { value: 0, attributes: { sync_config_id: 'config-a', sync_config_state: 'PROCESSING' } }
    ]);
  });

  it('does not report anything as retired when reading the metrics fails', async () => {
    let fail = false;
    const storage = {
      getStorageMetrics: async () => {
        if (fail) {
          throw new Error('storage unavailable');
        }
        return storageMetrics([syncConfigMetrics('config-a', 'ACTIVE', 100)]);
      }
    } as unknown as BucketStorageFactory;

    const { engine, providers } = testEngine();
    initializeCoreStorageMetrics(engine, storage);
    const provider = providers.get(StorageMetric.ATTRIBUTED_OBJECT_STORAGE_BYTES)!;

    await provider();

    fail = true;
    vi.setSystemTime(61_000);
    expect(await provider()).toBeUndefined();

    // The failure must not be interpreted as the sync config having been pruned.
    fail = false;
    vi.setSystemTime(122_000);
    expect(await provider()).toEqual([
      { value: 100, attributes: { sync_config_id: 'config-a', sync_config_state: 'ACTIVE' } }
    ]);
  });
});
