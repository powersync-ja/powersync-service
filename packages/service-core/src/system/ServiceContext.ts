import { LifeCycledSystem, ServiceIdentifier, container } from '@powersync/lib-services-framework';

import * as metrics from '../metrics/Metrics.js';
import * as replication from '../replication/replication-index.js';
import * as routes from '../routes/routes-index.js';
import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';

export interface ServiceContext {
  configuration: utils.ResolvedPowerSyncConfig;
  lifeCycleEngine: LifeCycledSystem;
  metrics: metrics.Metrics | null;
  replicationEngine: replication.ReplicationEngine | null;
  routerEngine: routes.RouterEngine | null;
  storageEngine: storage.StorageEngine;
}

/**
 * Context which allows for registering and getting implementations
 * of various service engines.
 * This controls registering, initializing and the lifecycle of various services.
 */
export class ServiceContextContainer implements ServiceContext {
  lifeCycleEngine: LifeCycledSystem;
  storageEngine: storage.StorageEngine;

  constructor(public configuration: utils.ResolvedPowerSyncConfig) {
    this.lifeCycleEngine = new LifeCycledSystem();

    this.storageEngine = new storage.StorageEngine({
      configuration
    });
    this.lifeCycleEngine.withLifecycle(this.storageEngine, {
      start: (storageEngine) => storageEngine.start(),
      stop: (storageEngine) => storageEngine.shutDown()
    });

    // Mongo storage is available as an option by default TODO: Consider moving this to a Mongo Storage Module
    this.storageEngine.registerProvider(new storage.MongoStorageProvider());
  }

  get replicationEngine(): replication.ReplicationEngine | null {
    return container.getOptional(replication.ReplicationEngine);
  }

  get routerEngine(): routes.RouterEngine | null {
    return container.getOptional(routes.RouterEngine);
  }

  get metrics(): metrics.Metrics | null {
    return container.getOptional(metrics.Metrics);
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
