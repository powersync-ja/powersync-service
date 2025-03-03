import * as lib_mongo from '@powersync/lib-service-mongodb';
import {
  api,
  ConfigurationFileSyncRulesProvider,
  ConnectionTestResult,
  replication,
  system,
  TearDownOptions
} from '@powersync/service-core';
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
      metricsEngine: context.metricsEngine,
      connectionFactory: connectionFactory,
      rateLimiter: new MongoErrorRateLimiter()
    });
  }

  async onInitialized(context: system.ServiceContextContainer): Promise<void> {}

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
    // No-op
  }

  async testConnection(config: types.MongoConnectionConfig) {
    this.decodeConfig(config);
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    return await MongoModule.testConnection(normalizedConfig);
  }

  static async testConnection(normalizedConfig: types.NormalizedMongoConnectionConfig): Promise<ConnectionTestResult> {
    const connectionManager = new MongoManager(normalizedConfig, {
      // Use short timeouts for testing connections.
      // Must be < 30s, to ensure we get a proper timeout error.
      socketTimeoutMS: 1_000,
      serverSelectionTimeoutMS: 1_000
    });
    try {
      await checkSourceConfiguration(connectionManager);
    } catch (e) {
      throw lib_mongo.mapConnectionError(e);
    } finally {
      await connectionManager.end();
    }
    return {
      connectionDescription: normalizedConfig.uri
    };
  }
}
