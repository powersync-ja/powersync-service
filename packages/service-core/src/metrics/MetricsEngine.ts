import { logger, ServiceAssertionError } from '@powersync/lib-services-framework';
import { Counter, UpDownCounter, ObservableGauge, MetricMetadata, MetricsFactory } from './metrics-interfaces.js';

export interface MetricsEngineOptions {
  factory: MetricsFactory;
  disable_telemetry_sharing: boolean;
}

export class MetricsEngine {
  private counters: Map<string, Counter>;
  private upDownCounters: Map<string, UpDownCounter>;
  private observableGauges: Map<string, ObservableGauge>;

  constructor(private options: MetricsEngineOptions) {
    this.counters = new Map();
    this.upDownCounters = new Map();
    this.observableGauges = new Map();
  }

  private get factory(): MetricsFactory {
    return this.options.factory;
  }

  createCounter(metadata: MetricMetadata): Counter {
    if (this.counters.has(metadata.name)) {
      logger.warn(`Counter with name ${metadata.name} already created and registered, skipping.`);
      return this.counters.get(metadata.name)!;
    }

    const counter = this.factory.createCounter(metadata);
    this.counters.set(metadata.name, counter);
    return counter;
  }
  createUpDownCounter(metadata: MetricMetadata): UpDownCounter {
    if (this.upDownCounters.has(metadata.name)) {
      logger.warn(`UpDownCounter with name ${metadata.name} already created and registered, skipping.`);
      return this.upDownCounters.get(metadata.name)!;
    }

    const upDownCounter = this.factory.createUpDownCounter(metadata);
    this.upDownCounters.set(metadata.name, upDownCounter);
    return upDownCounter;
  }

  createObservableGauge(metadata: MetricMetadata): ObservableGauge {
    if (this.observableGauges.has(metadata.name)) {
      logger.warn(`ObservableGauge with name ${metadata.name} already created and registered, skipping.`);
      return this.observableGauges.get(metadata.name)!;
    }

    const observableGauge = this.factory.createObservableGauge(metadata);
    this.observableGauges.set(metadata.name, observableGauge);
    return observableGauge;
  }

  getCounter(name: string): Counter {
    const counter = this.counters.get(name);
    if (!counter) {
      throw new ServiceAssertionError(`Counter '${name}' has not been created and registered yet.`);
    }
    return counter;
  }

  getUpDownCounter(name: string): UpDownCounter {
    const upDownCounter = this.upDownCounters.get(name);
    if (!upDownCounter) {
      throw new ServiceAssertionError(`UpDownCounter '${name}' has not been created and registered yet.`);
    }
    return upDownCounter;
  }

  getObservableGauge(name: string): ObservableGauge {
    const observableGauge = this.observableGauges.get(name);
    if (!observableGauge) {
      throw new ServiceAssertionError(`ObservableGauge '${name}' has not been created and registered yet.`);
    }
    return observableGauge;
  }

  public async start(): Promise<void> {
    logger.info(
      `
Attention:
PowerSync collects completely anonymous telemetry regarding usage.
This information is used to shape our roadmap to better serve our customers.
You can learn more, including how to opt-out if you'd not like to participate in this anonymous program, by visiting the following URL:
https://docs.powersync.com/self-hosting/lifecycle-maintenance/telemetry
    `.trim()
    );
    logger.info(`Anonymous telemetry is currently: ${this.options.disable_telemetry_sharing ? 'disabled' : 'enabled'}`);

    logger.info('Successfully started Metrics Engine.');
  }

  public async shutdown(): Promise<void> {
    logger.info('Successfully shut down Metrics Engine.');
  }
}
