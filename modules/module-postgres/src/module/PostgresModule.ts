import {
  api,
  auth,
  ConfigurationFileSyncRulesProvider,
  replication,
  Replicator,
  system
} from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import { logger } from '@powersync/lib-services-framework';
import * as types from '../types/types.js';
import { PostgresRouteAPIAdapter } from '../api/PostgresRouteAPIAdapter.js';
import { SupabaseKeyCollector } from '../auth/SupabaseKeyCollector.js';
import { WalStreamManager } from '../replication/WalStreamManager.js';
import { ConnectionManagerFactory } from '../replication/ConnectionManagerFactory.js';

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
  ): Replicator {
    const normalisedConfig = this.resolveConfig(decodedConfig);
    const connectionFactory = new ConnectionManagerFactory(normalisedConfig);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);

    return new WalStreamManager({
      id: this.getDefaultId(normalisedConfig.database),
      syncRuleProvider: syncRuleProvider,
      storageFactory: context.storage,
      connectionFactory: connectionFactory
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

  // TODO: Confirm if there are any resources that would not already be closed by the Replication and Router engines
  async teardown(): Promise<void> {}

  protected registerSupabaseAuth(context: system.ServiceContextContainer) {
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
          logger.warn('Failed to decode configuration in Postgres module initialization.', ex);
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
