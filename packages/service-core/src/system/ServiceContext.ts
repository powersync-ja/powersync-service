import { LifeCycledSystem, ServiceIdentifier, container } from '@powersync/lib-services-framework';
import { Metrics } from '../metrics/Metrics.js';
import { ReplicationEngine } from '../replication/core/ReplicationEngine.js';
import { RouterEngine } from '../routes/RouterEngine.js';
import { BucketStorageFactory } from '../storage/BucketStorage.js';
import { StorageProvider } from '../storage/StorageProvider.js';
import { MongoStorageProvider } from '../storage/mongo/MongoStorageProvider.js';
import { ResolvedPowerSyncConfig, RunnerConfig } from '../util/config/types.js';
import { CompoundConfigCollector } from '../util/util-index.js';

/**
 * Context which allows for registering and getting implementations
 * of various service engines.
 * This controls registering, initializing and the lifecycle of various services.
 */
export class ServiceContext extends LifeCycledSystem {
  private _storage: BucketStorageFactory | null;
  private _configuration: ResolvedPowerSyncConfig | null;
  private _metrics: Metrics | null;

  protected storageProviders: Map<string, StorageProvider>;
  routerEngine: RouterEngine;
  configCollector: CompoundConfigCollector;
  // TODO metrics

  get configuration(): ResolvedPowerSyncConfig {
    if (!this._configuration) {
      throw new Error(`Attempt to use configuration before it has been collected`);
    }
    return this._configuration;
  }

  get storage(): BucketStorageFactory {
    if (!this._storage) {
      throw new Error(`Attempt to use storage before [initialize] has been called`);
    }
    return this._storage;
  }

  get metrics(): Metrics {
    if (!this._metrics) {
      throw new Error(`Attempt to use metrics before [initialize] has been called`);
    }
    return this._metrics;
  }

  get replicationEngine(): ReplicationEngine {
    // TODO clean this up
    return container.getImplementation(ReplicationEngine);
  }

  constructor() {
    super();

    // These will only be set once `initialize` has been called
    this._storage = null;
    this._configuration = null;
    this._metrics = null;

    this.storageProviders = new Map();
    // Mongo storage is available as an option by default
    this.registerStorageProvider(new MongoStorageProvider());

    this.configCollector = new CompoundConfigCollector();

    this.routerEngine = new RouterEngine();
    this.withLifecycle(this.routerEngine, {
      stop: (routerEngine) => routerEngine.shutdown()
    });
  }

  /**
   * Allows for registering core and generic implementations of services/helpers.
   * This uses the framework container under the hood.
   */
  register<T>(identifier: ServiceIdentifier<T>, implementation: T) {
    container.register(identifier, implementation);
  }

  /**
   * Gets the implementation of an identifiable service.
   */
  get<T>(identifier: ServiceIdentifier<T>) {
    return container.getImplementation(identifier);
  }

  /**
   * Register a provider which generates a {@link BucketStorageFactory}
   * given the matching config specified in the loaded {@link ResolvedPowerSyncConfig}
   */
  registerStorageProvider(provider: StorageProvider) {
    this.storageProviders.set(provider.type, provider);
  }

  async initialize(entryConfig: RunnerConfig) {
    // Collect the config
    this._configuration = await this.configCollector.collectConfig(entryConfig);

    const { storage: storageConfig } = this.configuration;
    const { type } = storageConfig;
    const provider = this.storageProviders.get(type);

    if (!provider) {
      throw new Error(`No storage provider registered for type: ${type}`);
    }

    const { storage, disposer } = await provider.generate({
      ...storageConfig,
      slot_name_prefix: this.configuration.slot_name_prefix
    });

    this._storage = storage;
    this.withLifecycle(storage, {
      stop: () => disposer()
    });

    // Metrics go here for now
    this._metrics = await this.initializeMetrics();
    this.withLifecycle(null, {
      stop: () => Metrics.getInstance().shutdown()
    });

    // TODO neaten this
    container.register(
      ReplicationEngine,
      new ReplicationEngine({
        storage
      })
    );
  }

  protected async initializeMetrics() {
    const instanceId = await this.storage.getPowerSyncInstanceId();
    await Metrics.initialise({
      powersync_instance_id: instanceId,
      disable_telemetry_sharing: this.configuration.telemetry.disable_telemetry_sharing,
      internal_metrics_endpoint: this.configuration.telemetry.internal_service_endpoint
    });
    return Metrics.getInstance();
  }
}
