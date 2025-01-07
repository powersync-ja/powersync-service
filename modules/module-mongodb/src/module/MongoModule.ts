import * as lib_mongo from '@powersync/lib-service-mongodb';
import { api, ConfigurationFileSyncRulesProvider, replication, system, TearDownOptions } from '@powersync/service-core';
import { MongoRouteAPIAdapter } from '../api/MongoRouteAPIAdapter.js';
import { ChangeStreamReplicator } from '../replication/ChangeStreamReplicator.js';
import { ConnectionManagerFactory } from '../replication/ConnectionManagerFactory.js';
import { MongoErrorRateLimiter } from '../replication/MongoErrorRateLimiter.js';
import { MongoManager } from '../replication/MongoManager.js';
import { checkSourceConfiguration } from '../replication/replication-utils.js';
import * as types from '../types/types.js';

export class MongoModule extends replication.ReplicationModule<types.MongoConnectionConfig> {
  constructor() {
    super({
      name: 'MongoDB',
      type: lib_mongo.MONGO_CONNECTION_TYPE,
      configSchema: types.MongoConnectionConfig
    });
  }

  protected createRouteAPIAdapter(): api.RouteAPI {
    return new MongoRouteAPIAdapter(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new ConnectionManagerFactory(normalisedConfig);

    return new ChangeStreamReplicator({
      id: this.getDefaultId(normalisedConfig.database ?? ''),
      syncRuleProvider: syncRuleProvider,
      storageEngine: context.storageEngine,
      connectionFactory: connectionFactory,
      rateLimiter: new MongoErrorRateLimiter()
    });
  }

  /**
   * Combines base config with normalized connection settings
   */
  private resolveConfig(config: types.MongoConnectionConfig): types.ResolvedConnectionConfig {
    return {
      ...config,
      ...types.normalizeConnectionConfig(config)
    };
  }

  async teardown(options: TearDownOptions): Promise<void> {
    // TODO: Implement?
  }

  async testConnection(config: types.MongoConnectionConfig): Promise<void> {
    this.decodeConfig(config);
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const connectionManager = new MongoManager(normalisedConfig, {
      // Use short timeouts for testing connections.
      // Must be < 30s, to ensure we get a proper timeout error.
      socketTimeoutMS: 5_000,
      serverSelectionTimeoutMS: 5_000
    });
    try {
      return await checkSourceConfiguration(connectionManager);
    } finally {
      await connectionManager.end();
    }
  }
}
