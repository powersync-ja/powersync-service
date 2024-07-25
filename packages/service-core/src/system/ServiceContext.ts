import { LifeCycledSystem, ServiceIdentifier, container } from '@powersync/lib-services-framework';
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

  protected storageProviders: Map<string, StorageProvider>;

  // TODO metrics

  constructor() {
    super();

    this._storage = null;
    this._configuration = null;

    this.storageProviders = new Map();
    container.register(RouterEngine, new RouterEngine());
    container.register(CompoundConfigCollector, new CompoundConfigCollector());

    // Mongo storage is available as an option by default
    this.registerStorageProvider(new MongoStorageProvider());
  }

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

  get replicationEngine(): ReplicationEngine {
    return container.getImplementation(ReplicationEngine);
  }

  get routerEngine(): RouterEngine {
    return container.getImplementation(RouterEngine);
  }

  get configCollector(): CompoundConfigCollector {
    return container.getImplementation(CompoundConfigCollector);
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

    container.register(
      ReplicationEngine,
      new ReplicationEngine({
        storage
      })
    );
  }
}
