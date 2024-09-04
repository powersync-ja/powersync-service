import { api, auth, ConfigurationFileSyncRulesProvider, replication, system } from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import { PostgresRouteAPIAdapter } from '../api/PostgresRouteAPIAdapter.js';
import { SupabaseKeyCollector } from '../auth/SupabaseKeyCollector.js';
import { ConnectionManagerFactory } from '../replication/ConnectionManagerFactory.js';
import { PostgresErrorRateLimiter } from '../replication/PostgresErrorRateLimiter.js';
import { WalStreamReplicator } from '../replication/WalStreamReplicator.js';
import * as types from '../types/types.js';

export class PostgresModule extends replication.ReplicationModule<types.PostgresConnectionConfig> {
  constructor() {
    super({
      name: 'Postgres',
      type: types.POSTGRES_CONNECTION_TYPE,
      configSchema: types.PostgresConnectionConfig
    });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    await super.initialize(context);

    // Record replicated bytes using global jpgwire metrics.
    if (context.configuration.base_config.client_auth?.supabase) {
      this.registerSupabaseAuth(context);
    }

    jpgwire.setMetricsRecorder({
      addBytesRead(bytes) {
        context.metrics.data_replicated_bytes.add(bytes);
      }
    });
  }

  protected createRouteAPIAdapter(decodedConfig: types.PostgresConnectionConfig): api.RouteAPI {
    return new PostgresRouteAPIAdapter(this.resolveConfig(decodedConfig));
  }

  protected createReplicator(
    decodedConfig: types.PostgresConnectionConfig,
    context: system.ServiceContext
  ): replication.AbstractReplicator {
    const normalisedConfig = this.resolveConfig(decodedConfig);
    const connectionFactory = new ConnectionManagerFactory(normalisedConfig);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);

    return new WalStreamReplicator({
      id: this.getDefaultId(normalisedConfig.database),
      syncRuleProvider: syncRuleProvider,
      storageEngine: context.storage,
      connectionFactory: connectionFactory,
      rateLimiter: new PostgresErrorRateLimiter(),
      eventManager: context.replicationEngine.eventManager
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

  async teardown(): Promise<void> {}

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
}
