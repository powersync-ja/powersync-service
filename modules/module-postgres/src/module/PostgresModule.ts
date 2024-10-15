import { api, auth, ConfigurationFileSyncRulesProvider, modules, replication, system } from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import { PostgresRouteAPIAdapter } from '../api/PostgresRouteAPIAdapter.js';
import { SupabaseKeyCollector } from '../auth/SupabaseKeyCollector.js';
import { ConnectionManagerFactory } from '../replication/ConnectionManagerFactory.js';
import { PgManager } from '../replication/PgManager.js';
import { PostgresErrorRateLimiter } from '../replication/PostgresErrorRateLimiter.js';
import { cleanUpReplicationSlot } from '../replication/replication-utils.js';
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

    if (context.configuration.base_config.client_auth?.supabase) {
      this.registerSupabaseAuth(context);
    }

    // Record replicated bytes using global jpgwire metrics.
    if (context.metrics) {
      jpgwire.setMetricsRecorder({
        addBytesRead(bytes) {
          context.metrics!.data_replicated_bytes.add(bytes);
        }
      });
    }
  }

  protected createRouteAPIAdapter(): api.RouteAPI {
    return new PostgresRouteAPIAdapter(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new ConnectionManagerFactory(normalisedConfig);

    return new WalStreamReplicator({
      id: this.getDefaultId(normalisedConfig.database),
      syncRuleProvider: syncRuleProvider,
      storageEngine: context.storageEngine,
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
      maxSize: 1
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
}
