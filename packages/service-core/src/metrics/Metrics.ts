import { Attributes, Counter, ObservableGauge, UpDownCounter, ValueType } from '@opentelemetry/api';
import { PrometheusExporter } from '@opentelemetry/exporter-prometheus';
import { MeterProvider, MetricReader, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import * as jpgwire from '@powersync/service-jpgwire';
import * as util from '../util/util-index.js';
import * as storage from '../storage/storage-index.js';
import { CorePowerSyncSystem } from '../system/CorePowerSyncSystem.js';
import { IResource, Resource } from '@opentelemetry/resources';
import { logger } from '@powersync/lib-services-framework';

export interface MetricsOptions {
  disable_telemetry_sharing: boolean;
  powersync_instance_id: string;
  internal_metrics_endpoint: string;
  additional_metric_endpoints: string[];
}

export class Metrics {
  private static instance: Metrics;

  private prometheusExporter: PrometheusExporter;
  private meterProvider: MeterProvider;

  // Metrics
  // 1. Data processing / month

  // 1a. Postgres -> PowerSync
  // Record on replication pod
  public data_replicated_bytes: Counter<Attributes>;
  // 1b. PowerSync -> clients
  // Record on API pod
  public data_synced_bytes: Counter<Attributes>;
  // Unused for pricing
  // Record on replication pod
  public rows_replicated_total: Counter<Attributes>;
  // Unused for pricing
  // Record on replication pod
  public transactions_replicated_total: Counter<Attributes>;
  // Unused for pricing
  // Record on replication pod
  public chunks_replicated_total: Counter<Attributes>;

  // 2. Sync operations / month

  // Record on API pod
  public operations_synced_total: Counter<Attributes>;

  // 3. Data hosted on PowerSync sync service

  // Replication and Sync Rules
  // 3a. Replication storage -> raw data as received from Postgres.
  public replication_storage_size_bytes: ObservableGauge<Attributes>;
  // 3b. Operations storage -> transformed history, as will be synced to clients
  public operation_storage_size_bytes: ObservableGauge<Attributes>;
  // 3c. Parameter storage -> used for parameter queries
  public parameter_storage_size_bytes: ObservableGauge<Attributes>;
  // 3d. Initial syncs
  public initial_syncs: Counter<Attributes>;
  // 3e. Unique number of tables discovered in the sync rules
  public sync_rule_distinct_table_count: ObservableGauge<Attributes>;
  // 3f. Number of sync rule bucket definitions
  public sync_rule_bucket_definition_count: ObservableGauge<Attributes>;

  // 4. Peak concurrent connections

  // Record on API pod
  public concurrent_connections: UpDownCounter<Attributes>;

  // 5. Replication and Sync Rules

  // 5a. Initial syncs

  private constructor(meterProvider: MeterProvider, prometheusExporter: PrometheusExporter) {
    this.meterProvider = meterProvider;
    this.prometheusExporter = prometheusExporter;
    const meter = meterProvider.getMeter('powersync');

    this.data_replicated_bytes = meter.createCounter('powersync_data_replicated_bytes_total', {
      description: 'Uncompressed size of replicated data',
      unit: 'bytes',
      valueType: ValueType.INT
    });

    this.data_synced_bytes = meter.createCounter('powersync_data_synced_bytes_total', {
      description: 'Uncompressed size of synced data',
      unit: 'bytes',
      valueType: ValueType.INT
    });

    this.rows_replicated_total = meter.createCounter('powersync_rows_replicated_total', {
      description: 'Total number of replicated rows',
      valueType: ValueType.INT
    });

    this.transactions_replicated_total = meter.createCounter('powersync_transactions_replicated_total', {
      description: 'Total number of replicated transactions',
      valueType: ValueType.INT
    });

    this.chunks_replicated_total = meter.createCounter('powersync_chunks_replicated_total', {
      description: 'Total number of replication chunks',
      valueType: ValueType.INT
    });

    this.operations_synced_total = meter.createCounter('powersync_operations_synced_total', {
      description: 'Number of operations synced',
      valueType: ValueType.INT
    });

    this.replication_storage_size_bytes = meter.createObservableGauge('powersync_replication_storage_size_bytes', {
      description: 'Size of current data stored in PowerSync',
      unit: 'bytes',
      valueType: ValueType.INT
    });

    this.operation_storage_size_bytes = meter.createObservableGauge('powersync_operation_storage_size_bytes', {
      description: 'Size of operations stored in PowerSync',
      unit: 'bytes',
      valueType: ValueType.INT
    });

    this.parameter_storage_size_bytes = meter.createObservableGauge('powersync_parameter_storage_size_bytes', {
      description: 'Size of parameter data stored in PowerSync',
      unit: 'bytes',
      valueType: ValueType.INT
    });

    this.initial_syncs = meter.createCounter('powersync_initial_syncs_total', {
      description: 'The number of times an initial sync of the DB => Powersync has been performed',
      valueType: ValueType.INT
    });

    this.sync_rule_distinct_table_count = meter.createObservableGauge('powersync_sync_rule_distinct_table_count', {
      description: 'The number of unique tables recorded in the sync rules',
      valueType: ValueType.INT
    });

    this.sync_rule_bucket_definition_count = meter.createObservableGauge(
      'powersync_sync_rule_bucket_definition_count',
      {
        description: 'The number buckets defined in the sync rules',
        valueType: ValueType.INT
      }
    );

    this.concurrent_connections = meter.createUpDownCounter('powersync_concurrent_connections', {
      description: 'Number of concurrent sync connections',
      valueType: ValueType.INT
    });
  }

  // Generally only useful for tests. Note: gauges are ignored here.
  resetCounters() {
    this.data_replicated_bytes.add(0);
    this.data_synced_bytes.add(0);
    this.rows_replicated_total.add(0);
    this.transactions_replicated_total.add(0);
    this.chunks_replicated_total.add(0);
    this.operations_synced_total.add(0);
    this.concurrent_connections.add(0);
  }

  public static getInstance(): Metrics {
    if (!Metrics.instance) {
      throw new Error('Metrics have not been initialised');
    }

    return Metrics.instance;
  }

  public static async initialise(options: MetricsOptions): Promise<void> {
    if (Metrics.instance) {
      return;
    }
    logger.info('Configuring telemetry.');

    logger.info(
      `
Attention:
PowerSync collects completely anonymous telemetry regarding usage.
This information is used to shape our roadmap to better serve our customers.
You can learn more, including how to opt-out if you'd not like to participate in this anonymous program, by visiting the following URL:
https://docs.powersync.com/self-hosting/telemetry
Anonymous telemetry is currently: ${options.disable_telemetry_sharing ? 'disabled' : 'enabled'}
    `.trim()
    );

    const resource = new Resource({
      ['service']: 'PowerSync',
      ['instance_id']: options.powersync_instance_id
    });

    const configuredExporters: MetricReader[] = [];

    const port: number = util.env.METRICS_PORT ?? 0;
    const prometheusExporter = new PrometheusExporter({ port: port, preventServerStart: true });
    configuredExporters.push(prometheusExporter);

    if (!options.disable_telemetry_sharing) {
      logger.info('Sharing anonymous telemetry');
      const periodicExporter = new PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter({
          url: options.internal_metrics_endpoint
        }),
        exportIntervalMillis: 1000 * 60 * 5 // 5 minutes
      });

      configuredExporters.push(periodicExporter);
    } else {
      await Metrics.sendOneTimeRecord(resource, options.internal_metrics_endpoint);
    }

    // Create extra exporters for any additionally configured metric endpoints
    for (const endpoint of options.additional_metric_endpoints) {
      logger.info(`Exporting metrics to endpoint: ${endpoint}`);
      const additionalEndpointExporter = new PeriodicExportingMetricReader({
        exporter: new OTLPMetricExporter({
          url: endpoint
        }),
        exportIntervalMillis: 1000 * 60 * 5 // 5 minutes
      });

      configuredExporters.push(additionalEndpointExporter);
    }

    const meterProvider = new MeterProvider({
      resource: resource,
      readers: configuredExporters
    });

    if (port > 0) {
      await prometheusExporter.startServer();
    }

    Metrics.instance = new Metrics(meterProvider, prometheusExporter);

    logger.info('Telemetry configuration complete.');
  }

  public async shutdown(): Promise<void> {
    await this.meterProvider.shutdown();
  }

  public configureApiMetrics() {
    // Initialize the metric, so that it reports a value before connections
    // have been opened.
    this.concurrent_connections.add(0);
  }

  public configureReplicationMetrics(system: CorePowerSyncSystem) {
    // Rate limit collection of these stats, since it may be an expensive query
    const MINIMUM_INTERVAL = 60_000;

    let cachedRequest: Promise<storage.StorageMetrics | null> | undefined = undefined;
    let cacheTimestamp = 0;

    function getMetrics() {
      if (cachedRequest == null || Date.now() - cacheTimestamp > MINIMUM_INTERVAL) {
        cachedRequest = system.storage.getStorageMetrics().catch((e) => {
          logger.error(`Failed to get storage metrics`, e);
          return null;
        });
        cacheTimestamp = Date.now();
      }
      return cachedRequest;
    }

    this.operation_storage_size_bytes.addCallback(async (result) => {
      const metrics = await getMetrics();
      if (metrics) {
        result.observe(metrics.operations_size_bytes);
      }
    });

    this.parameter_storage_size_bytes.addCallback(async (result) => {
      const metrics = await getMetrics();
      if (metrics) {
        result.observe(metrics.parameters_size_bytes);
      }
    });

    this.replication_storage_size_bytes.addCallback(async (result) => {
      const metrics = await getMetrics();
      if (metrics) {
        result.observe(metrics.replication_size_bytes);
      }
    });

    const class_scoped_data_replicated_bytes = this.data_replicated_bytes;
    // Record replicated bytes using global jpgwire metrics.
    jpgwire.setMetricsRecorder({
      addBytesRead(bytes) {
        class_scoped_data_replicated_bytes.add(bytes);
      }
    });
  }

  public async getMetricValueForTests(name: string): Promise<number | undefined> {
    const metrics = await this.prometheusExporter.collect();
    const scoped = metrics.resourceMetrics.scopeMetrics[0].metrics;
    const metric = scoped.find((metric) => metric.descriptor.name == name);
    if (metric == null) {
      throw new Error(
        `Cannot find metric ${name}. Options: ${scoped.map((metric) => metric.descriptor.name).join(',')}`
      );
    }
    const point = metric.dataPoints[metric.dataPoints.length - 1];
    return point?.value as number;
  }

  private static async sendOneTimeRecord(resource: IResource, endpoint: string): Promise<void> {
    const onceOfExporter = new PeriodicExportingMetricReader({
      exporter: new OTLPMetricExporter({
        url: endpoint
      }),
      exportIntervalMillis: 1000 * 60 * 10 // 10 minutes - but not really relevant since we will force a send and a shutdown
    });

    const meterProvider = new MeterProvider({
      resource: resource,
      readers: [onceOfExporter]
    });

    const meter = meterProvider.getMeter('powersync');

    const counter = meter.createCounter('powersync_metrics_opted_out', {
      description: 'One time message registering that metrics were opted out of'
    });

    counter.add(1);

    await meterProvider.forceFlush();
    await meterProvider.shutdown();
  }
}
