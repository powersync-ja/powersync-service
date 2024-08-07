import { LifeCycledSystem, ServiceIdentifier, container } from '@powersync/lib-services-framework';

import * as metrics from '../metrics/Metrics.js';
import * as replication from '../replication/replication-index.js';
import * as routes from '../routes/routes-index.js';
import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';

export interface ServiceContext {
  configuration: utils.ResolvedPowerSyncConfig;
  lifeCycleEngine: LifeCycledSystem;
  metrics: metrics.Metrics;
  replicationEngine: replication.ReplicationEngine;
  routerEngine: routes.RouterEngine;
  storage: storage.StorageFactoryProvider;
}

/**
 * Context which allows for registering and getting implementations
 * of various service engines.
 * This controls registering, initializing and the lifecycle of various services.
 */
export class ServiceContextContainer implements ServiceContext {
  lifeCycleEngine: LifeCycledSystem;
  storage: storage.StorageFactoryProvider;

  constructor(public configuration: utils.ResolvedPowerSyncConfig) {
    this.lifeCycleEngine = new LifeCycledSystem();
    this.storage = new storage.StorageFactoryProvider({
      configuration,
      lifecycle_engine: this.lifeCycleEngine
    });

    // Mongo storage is available as an option by default
    this.storage.registerProvider(new storage.MongoStorageProvider());
  }

  get replicationEngine(): replication.ReplicationEngine {
    return this.get(replication.ReplicationEngine);
  }

  get routerEngine(): routes.RouterEngine {
    return this.get(routes.RouterEngine);
  }

  get metrics(): metrics.Metrics {
    return this.get(metrics.Metrics);
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
