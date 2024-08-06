import { LifeCycledSystem, ServiceIdentifier, container } from '@powersync/lib-services-framework';
import { Metrics } from '../metrics/Metrics.js';
import { ReplicationEngine } from '../replication/core/ReplicationEngine.js';
import { RouterEngine } from '../routes/RouterEngine.js';
import { BucketStorageFactory } from '../storage/BucketStorage.js';
import { StorageFactory } from '../storage/StorageFactory.js';
import { MongoStorageProvider } from '../storage/mongo/MongoStorageProvider.js';
import { ResolvedPowerSyncConfig } from '../util/config/types.js';

export interface ServiceContext {
  configuration: ResolvedPowerSyncConfig;
  lifeCycleEngine: LifeCycledSystem;
  metrics: Metrics;
  replicationEngine: ReplicationEngine;
  routerEngine: RouterEngine;
  storage: BucketStorageFactory;
}

export enum ServiceIdentifiers {
  // TODO a better identifier
  STORAGE = 'storage'
}

/**
 * Context which allows for registering and getting implementations
 * of various service engines.
 * This controls registering, initializing and the lifecycle of various services.
 */
export class ServiceContextContainer implements ServiceContext {
  lifeCycleEngine: LifeCycledSystem;
  storageFactory: StorageFactory;

  constructor(public configuration: ResolvedPowerSyncConfig) {
    this.lifeCycleEngine = new LifeCycledSystem();
    this.storageFactory = new StorageFactory();

    // Mongo storage is available as an option by default
    this.storageFactory.registerProvider(new MongoStorageProvider());
  }

  get replicationEngine(): ReplicationEngine {
    return this.get(ReplicationEngine);
  }

  get routerEngine(): RouterEngine {
    return this.get(RouterEngine);
  }

  get storage(): BucketStorageFactory {
    return this.get(ServiceIdentifiers.STORAGE);
  }

  get metrics(): Metrics {
    return this.get(Metrics);
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
}
