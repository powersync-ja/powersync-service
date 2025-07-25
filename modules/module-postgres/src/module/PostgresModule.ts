import {
  api,
  auth,
  ConfigurationFileSyncRulesProvider,
  ConnectionTestResult,
  modules,
  replication,
  system
} from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import { PostgresRouteAPIAdapter } from '../api/PostgresRouteAPIAdapter.js';
import { SupabaseKeyCollector } from '../auth/SupabaseKeyCollector.js';
import { ConnectionManagerFactory } from '../replication/ConnectionManagerFactory.js';
import { PgManager } from '../replication/PgManager.js';
import { PostgresErrorRateLimiter } from '../replication/PostgresErrorRateLimiter.js';
import { checkSourceConfiguration, cleanUpReplicationSlot } from '../replication/replication-utils.js';
import { PUBLICATION_NAME } from '../replication/WalStream.js';
import { WalStreamReplicator } from '../replication/WalStreamReplicator.js';
import * as types from '../types/types.js';
import { PostgresConnectionConfig } from '../types/types.js';
import { baseUri, NormalizedBasePostgresConnectionConfig } from '@powersync/lib-service-postgres';
import { ReplicationMetric } from '@powersync/service-types';
import { getApplicationName } from '../utils/application-name.js';

export class PostgresModule extends replication.ReplicationModule<types.PostgresConnectionConfig> {
  constructor() {
    super({
      name: 'Postgres',
      type: types.POSTGRES_CONNECTION_TYPE,
      configSchema: types.PostgresConnectionConfig
    });
  }

  async onInitialized(context: system.ServiceContextContainer): Promise<void> {
    const client_auth = context.configuration.base_config.client_auth;

    if (client_auth?.supabase && client_auth?.supabase_jwt_secret == null) {
      // Only use the deprecated SupabaseKeyCollector when there is no
      // secret hardcoded. Hardcoded secrets are handled elsewhere, using
      // StaticSupabaseKeyCollector.

      // Support for SupabaseKeyCollector is deprecated and support will be
      // completely removed by Supabase soon. We can keep support a while
      // longer for self-hosted setups, before also removing that on our side.
      this.registerSupabaseAuth(context);
    }

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
    const connectionFactory = new ConnectionManagerFactory(normalisedConfig);

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
      ...types.normalizeConnectionConfig(config)
    };
  }

  async teardown(options: modules.TearDownOptions): Promise<void> {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const connectionManager = new PgManager(normalisedConfig, {
      idleTimeout: 30_000,
      maxSize: 1,
      applicationName: getApplicationName()
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

  // TODO: This should rather be done by registering the key collector in some kind of auth engine
  private registerSupabaseAuth(context: system.ServiceContextContainer) {
    const { configuration } = context;
    // Register the Supabase key collector(s)
    configuration.connections
      ?.map((baseConfig) => {
        if (baseConfig.type != types.POSTGRES_CONNECTION_TYPE) {
          return;
        }
        try {
          return this.resolveConfig(types.PostgresConnectionConfig.decode(baseConfig as any));
        } catch (ex) {
          this.logger.warn('Failed to decode configuration.', ex);
        }
      })
      .filter((c) => !!c)
      .forEach((config) => {
        const keyCollector = new SupabaseKeyCollector(config!);
        context.lifeCycleEngine.withLifecycle(keyCollector, {
          // Close the internal pool
          stop: (collector) => collector.shutdown()
        });
        configuration.client_keystore.collector.add(new auth.CachedKeyCollector(keyCollector));
      });
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
      applicationName: getApplicationName()
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
