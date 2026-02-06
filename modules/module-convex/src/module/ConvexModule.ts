import {
  api,
  ConfigurationFileSyncRulesProvider,
  ConnectionTestResult,
  replication,
  system,
  TearDownOptions
} from '@powersync/service-core';
import { ConvexRouteAPIAdapter } from '../api/ConvexRouteAPIAdapter.js';
import { ConvexConnectionManager } from '../replication/ConvexConnectionManager.js';
import { ConvexConnectionManagerFactory } from '../replication/ConvexConnectionManagerFactory.js';
import { ConvexErrorRateLimiter } from '../replication/ConvexErrorRateLimiter.js';
import { ConvexReplicator } from '../replication/ConvexReplicator.js';
import * as types from '../types/types.js';

export class ConvexModule extends replication.ReplicationModule<types.ConvexConnectionConfig> {
  constructor() {
    super({
      name: 'Convex',
      type: types.CONVEX_CONNECTION_TYPE,
      configSchema: types.ConvexConnectionConfig
    });
  }

  async onInitialized(context: system.ServiceContextContainer): Promise<void> {}

  protected createRouteAPIAdapter(): api.RouteAPI {
    return new ConvexRouteAPIAdapter(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new ConvexConnectionManagerFactory(normalizedConfig);

    return new ConvexReplicator({
      id: this.getDefaultId(normalizedConfig.deploymentUrl),
      syncRuleProvider,
      storageEngine: context.storageEngine,
      metricsEngine: context.metricsEngine,
      connectionFactory,
      rateLimiter: new ConvexErrorRateLimiter()
    });
  }

  private resolveConfig(config: types.ConvexConnectionConfig): types.ResolvedConvexConnectionConfig {
    return {
      ...config,
      ...types.normalizeConnectionConfig(config)
    };
  }

  async teardown(options: TearDownOptions): Promise<void> {
    // No source-side teardown required.
  }

  async testConnection(config: types.ConvexConnectionConfig) {
    this.decodeConfig(config);
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    return await ConvexModule.testConnection(normalizedConfig);
  }

  static async testConnection(
    normalizedConfig: types.ResolvedConvexConnectionConfig
  ): Promise<ConnectionTestResult> {
    const connectionManager = new ConvexConnectionManager(normalizedConfig);
    try {
      await connectionManager.client.getJsonSchemas();
    } finally {
      await connectionManager.end();
    }

    return {
      connectionDescription: normalizedConfig.deploymentUrl
    };
  }
}
