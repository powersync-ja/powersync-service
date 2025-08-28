import { baseUri, NormalizedBasePostgresConnectionConfig } from '@powersync/lib-service-postgres';
import {
  api,
  ConfigurationFileSyncRulesProvider,
  ConnectionTestResult,
  modules,
  replication,
  system
} from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import { ReplicationMetric } from '@powersync/service-types';
import { PostgresRouteAPIAdapter } from '../api/PostgresRouteAPIAdapter.js';
import { ConnectionManagerFactory } from '../replication/ConnectionManagerFactory.js';
import { PgManager } from '../replication/PgManager.js';
import { PostgresErrorRateLimiter } from '../replication/PostgresErrorRateLimiter.js';
import { checkSourceConfiguration, cleanUpReplicationSlot } from '../replication/replication-utils.js';
import { PUBLICATION_NAME } from '../replication/WalStream.js';
import { WalStreamReplicator } from '../replication/WalStreamReplicator.js';
import * as types from '../types/types.js';
import { PostgresConnectionConfig } from '../types/types.js';
import { getApplicationName } from '../utils/application-name.js';
import { CustomTypeRegistry } from '../types/registry.js';

export class PostgresModule extends replication.ReplicationModule<types.PostgresConnectionConfig> {
  private customTypes: CustomTypeRegistry = new CustomTypeRegistry();

  constructor() {
    super({
      name: 'Postgres',
      type: types.POSTGRES_CONNECTION_TYPE,
      configSchema: types.PostgresConnectionConfig
    });
  }

  async onInitialized(context: system.ServiceContextContainer): Promise<void> {
    // Record replicated bytes using global jpgwire metrics. Only registered if this module is replicating
    if (context.replicationEngine) {
      jpgwire.setMetricsRecorder({
        addBytesRead(bytes) {
          context.metricsEngine.getCounter(ReplicationMetric.DATA_REPLICATED_BYTES).add(bytes);
        }
      });
      this.logger.info('Successfully set up connection metrics recorder for PostgresModule.');
    }
  }

  protected createRouteAPIAdapter(): api.RouteAPI {
    return PostgresRouteAPIAdapter.withConfig(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new ConnectionManagerFactory(normalisedConfig, this.customTypes);

    return new WalStreamReplicator({
      id: this.getDefaultId(normalisedConfig.database),
      syncRuleProvider: syncRuleProvider,
      storageEngine: context.storageEngine,
      metricsEngine: context.metricsEngine,
      connectionFactory: connectionFactory,
      rateLimiter: new PostgresErrorRateLimiter()
    });
  }

  /**
   * Combines base config with normalized connection settings
   */
  private resolveConfig(config: types.PostgresConnectionConfig): types.ResolvedConnectionConfig {
    return {
      ...config,
      ...types.normalizeConnectionConfig(config),
      typeRegistry: this.customTypes
    };
  }

  async teardown(options: modules.TearDownOptions): Promise<void> {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const connectionManager = new PgManager(normalisedConfig, {
      idleTimeout: 30_000,
      maxSize: 1,
      applicationName: getApplicationName(),
      registry: this.customTypes
    });

    try {
      if (options.syncRules) {
        // TODO: In the future, once we have more replication types, we will need to check if these syncRules are for Postgres
        for (let syncRules of options.syncRules) {
          try {
            await cleanUpReplicationSlot(syncRules.slot_name, connectionManager.pool);
          } catch (e) {
            // Not really much we can do here for failures, most likely the database is no longer accessible
            this.logger.warn(`Failed to fully clean up Postgres replication slot: ${syncRules.slot_name}`, e);
          }
        }
      }
    } finally {
      await connectionManager.end();
    }
  }

  async testConnection(config: PostgresConnectionConfig): Promise<ConnectionTestResult> {
    this.decodeConfig(config);
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    return await this.testConnection(normalizedConfig);
  }

  static async testConnection(normalizedConfig: NormalizedBasePostgresConnectionConfig): Promise<ConnectionTestResult> {
    // FIXME: This is not a complete implementation yet.
    const connectionManager = new PgManager(normalizedConfig, {
      idleTimeout: 30_000,
      maxSize: 1,
      applicationName: getApplicationName(),
      registry: new CustomTypeRegistry()
    });
    const connection = await connectionManager.snapshotConnection();
    try {
      await checkSourceConfiguration(connection, PUBLICATION_NAME);
    } finally {
      await connectionManager.end();
    }
    return {
      connectionDescription: baseUri(normalizedConfig)
    };
  }
}
