import { container, LifeCycledSystem, MigrationManager, ServiceIdentifier } from '@powersync/lib-services-framework';
import type { AdditionalSyncConfigParser } from '@powersync/service-sync-rules';

import { EventsEngine } from '../events/EventsEngine.js';
import { framework } from '../index.js';
import * as metrics from '../metrics/MetricsEngine.js';
import { PowerSyncMigrationManager } from '../migrations/PowerSyncMigrationManager.js';
import * as replication from '../replication/replication-index.js';
import * as routes from '../routes/routes-index.js';
import * as storage from '../storage/storage-index.js';
import { SyncContext } from '../sync/SyncContext.js';
import * as utils from '../util/util-index.js';

/**
 * Registration-capable sync-config parser exposed to modules through the service context.
 */
export interface ServiceContextSyncConfigParser extends storage.SyncConfigParser {
  /**
   * Registers a deterministic parser extension during module initialization, before storage starts.
   */
  registerParser(extension: AdditionalSyncConfigParser): void;
}

export interface ServiceContext {
  configuration: utils.ResolvedPowerSyncConfig;
  lifeCycleEngine: LifeCycledSystem;
  metricsEngine: metrics.MetricsEngine;
  replicationEngine: replication.ReplicationEngine | null;
  routerEngine: routes.RouterEngine;
  storageEngine: storage.StorageEngine;
  migrations: PowerSyncMigrationManager;
  syncContext: SyncContext;
  writeCheckpointBatcher: utils.WriteCheckpointBatcher;
  serviceMode: ServiceContextMode;
  eventsEngine: EventsEngine;
  /**
   * Service-wide parser shared by routes, replication, and storage. Source modules register parser extensions on this
   * instance during module initialization.
   */
  readonly syncConfigParser: ServiceContextSyncConfigParser;
}

export enum ServiceContextMode {
  API = utils.ServiceRunner.API,
  SYNC = utils.ServiceRunner.SYNC,
  UNIFIED = utils.ServiceRunner.UNIFIED,
  COMPACT = 'compact',
  MIGRATION = 'migration',
  TEARDOWN = 'teardown',
  TEST_CONNECTION = 'test-connection'
}

export interface ServiceContextOptions {
  serviceMode: ServiceContextMode;
  configuration: utils.ResolvedPowerSyncConfig;
}

/**
 * Context which allows for registering and getting implementations
 * of various service engines.
 * This controls registering, initializing and the lifecycle of various services.
 */
export class ServiceContextContainer implements ServiceContext {
  configuration: utils.ResolvedPowerSyncConfig;
  lifeCycleEngine: LifeCycledSystem;
  storageEngine: storage.StorageEngine;
  eventsEngine: EventsEngine;
  syncContext: SyncContext;
  routerEngine: routes.RouterEngine;
  writeCheckpointBatcher: utils.WriteCheckpointBatcher;
  serviceMode: ServiceContextMode;
  readonly #syncConfigParser: ServiceContextSyncConfigParser;

  constructor(options: ServiceContextOptions) {
    this.serviceMode = options.serviceMode;
    const { configuration } = options;
    this.configuration = configuration;

    this.#syncConfigParser = new ServiceContextSyncConfigParserImpl(() => this.storageEngine.started);

    this.lifeCycleEngine = new LifeCycledSystem();

    this.storageEngine = new storage.StorageEngine({
      configuration,
      syncConfigParser: this.#syncConfigParser
    });
    this.storageEngine.registerListener({
      storageFatalError: (error) => {
        // Propagate the error to the lifecycle engine
        this.lifeCycleEngine.stopWithError(error);
      }
    });

    this.eventsEngine = new EventsEngine();
    this.lifeCycleEngine.withLifecycle(this.eventsEngine, {
      stop: (emitterEngine) => emitterEngine.shutDown()
    });

    this.lifeCycleEngine.withLifecycle(this.storageEngine, {
      start: (storageEngine) => storageEngine.start(),
      stop: (storageEngine) => storageEngine.shutDown()
    });

    this.routerEngine = new routes.RouterEngine();
    this.lifeCycleEngine.withLifecycle(this.routerEngine, {
      stop: (routerEngine) => routerEngine.shutDown()
    });

    this.writeCheckpointBatcher = new utils.WriteCheckpointBatcher(
      () => this.routerEngine.getAPI(),
      () => this.storageEngine.activeBucketStorage
    );

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

    this.lifeCycleEngine.withLifecycle(this.eventsEngine, {
      stop: (emitterEngine) => emitterEngine.shutDown()
    });
  }

  get replicationEngine(): replication.ReplicationEngine | null {
    return container.getOptional(replication.ReplicationEngine);
  }

  /**
   * Returns the service-wide parser shared by routes, replication, and storage.
   */
  get syncConfigParser(): ServiceContextSyncConfigParser {
    return this.#syncConfigParser;
  }

  get metricsEngine(): metrics.MetricsEngine {
    return container.getImplementation(metrics.MetricsEngine);
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

/**
 * Registration-capable parser exposed by the service context during module initialization.
 */
class ServiceContextSyncConfigParserImpl extends storage.SqlSyncConfigParser implements ServiceContextSyncConfigParser {
  constructor(private readonly isStorageStarted: () => boolean) {
    super();
  }

  /**
   * Registers an extension while preserving one parser definition for the service lifetime.
   */
  override registerParser(extension: AdditionalSyncConfigParser): void {
    if (this.isStorageStarted()) {
      throw new Error('A sync config parser extension cannot be registered after the storage engine has started.');
    }
    super.registerParser(extension);
  }
}
