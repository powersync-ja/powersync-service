import { LifeCycledSystem, MigrationManager, ServiceIdentifier, container } from '@powersync/lib-services-framework';

import { framework } from '../index.js';
import * as metrics from '../metrics/Metrics.js';
import { PowerSyncMigrationManager } from '../migrations/PowerSyncMigrationManager.js';
import * as replication from '../replication/replication-index.js';
import * as routes from '../routes/routes-index.js';
import * as storage from '../storage/storage-index.js';
import * as utils from '../util/util-index.js';
import { SyncContext } from '../sync/SyncContext.js';

export interface ServiceContext {
  configuration: utils.ResolvedPowerSyncConfig;
  lifeCycleEngine: LifeCycledSystem;
  metrics: metrics.Metrics | null;
  replicationEngine: replication.ReplicationEngine | null;
  routerEngine: routes.RouterEngine | null;
  storageEngine: storage.StorageEngine;
  migrations: PowerSyncMigrationManager;
  syncContext: SyncContext;
}

/**
 * Context which allows for registering and getting implementations
 * of various service engines.
 * This controls registering, initializing and the lifecycle of various services.
 */
export class ServiceContextContainer implements ServiceContext {
  lifeCycleEngine: LifeCycledSystem;
  storageEngine: storage.StorageEngine;
  syncContext: SyncContext;

  constructor(public configuration: utils.ResolvedPowerSyncConfig) {
    this.lifeCycleEngine = new LifeCycledSystem();

    this.storageEngine = new storage.StorageEngine({
      configuration
    });

    this.syncContext = new SyncContext({
      maxDataFetchConcurrency: configuration.api_parameters.max_data_fetch_concurrency,
      maxBuckets: configuration.api_parameters.max_buckets_per_connection,
      maxParameterQueryResults: configuration.api_parameters.max_parameter_query_results
    });

    const migrationManager = new MigrationManager();
    container.register(framework.ContainerImplementation.MIGRATION_MANAGER, migrationManager);

    this.lifeCycleEngine.withLifecycle(migrationManager, {
      // Migrations should be executed before the system starts
      start: () => migrationManager[Symbol.asyncDispose]()
    });

    this.lifeCycleEngine.withLifecycle(this.storageEngine, {
      start: (storageEngine) => storageEngine.start(),
      stop: (storageEngine) => storageEngine.shutDown()
    });
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

  get migrations(): PowerSyncMigrationManager {
    return container.getImplementation(framework.ContainerImplementation.MIGRATION_MANAGER);
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
