import { LifeCycledSystem } from '@powersync/lib-services-framework';
import { ReplicationEngine } from '../replication/core/ReplicationEngine.js';
import { RouterEngine } from '../routes/RouterEngine.js';
import { BucketStorageFactory } from '../storage/BucketStorage.js';
import { StorageProvider } from '../storage/StorageProvider.js';
import { MongoStorageProvider } from '../storage/mongo/MongoStorageProvider.js';
import { ResolvedPowerSyncConfig } from '../util/config/types.js';

export class ServiceContext extends LifeCycledSystem {
  private _storage: BucketStorageFactory | null;
  private _replicationEngine: ReplicationEngine | null;

  protected storageProviders: Map<string, StorageProvider>;

  routerEngine: RouterEngine;

  constructor(public configuration: ResolvedPowerSyncConfig) {
    super();
    this._storage = null;
    this._replicationEngine = null;
    this.storageProviders = new Map();
    this.routerEngine = new RouterEngine();

    this.registerStorageProvider(new MongoStorageProvider());
  }

  get storage(): BucketStorageFactory {
    if (!this._storage) {
      throw new Error(`Attempt to use storage before [initialize] has been called`);
    }
    return this._storage;
  }

  get replicationEngine(): ReplicationEngine {
    if (!this._replicationEngine) {
      throw new Error(`Attempt to use replicationEngine before [initialize] has been called`);
    }
    return this._replicationEngine;
  }

  /**
   * Register a provider which generates a {@link BucketStorageFactory}
   * given the matching config specified in the loaded {@link ResolvedPowerSyncConfig}
   */
  registerStorageProvider(provider: StorageProvider) {
    this.storageProviders.set(provider.type, provider);
  }

  async initialize() {
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

    this._replicationEngine = new ReplicationEngine({
      storage
    });
  }
}
