import {
  api,
  ConfigurationFileSyncRulesProvider,
  ConnectionTestResult,
  replication,
  system,
  TearDownOptions
} from '@powersync/service-core';
import { MSSQLConnectionManagerFactory } from '../replication/MSSQLConnectionManagerFactory.js';
import * as types from '../types/types.js';
import { CDCReplicator } from '../replication/CDCReplicator.js';
import { MSSQLConnectionManager } from '../replication/MSSQLConnectionManager.js';
import { checkSourceConfiguration } from '../utils/mssql.js';
import { MSSQLErrorRateLimiter } from '../replication/MSSQLErrorRateLimiter.js';
import { MSSQLRouteAPIAdapter } from '../api/MSSQLRouteAPIAdapter.js';

export class MSSQLModule extends replication.ReplicationModule<types.MSSQLConnectionConfig> {
  constructor() {
    super({
      name: 'MSSQL',
      type: types.MSSQL_CONNECTION_TYPE,
      configSchema: types.MSSQLConnectionConfig
    });
  }

  async onInitialized(context: system.ServiceContextContainer): Promise<void> {}

  protected createRouteAPIAdapter(): api.RouteAPI {
    return new MSSQLRouteAPIAdapter(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new MSSQLConnectionManagerFactory(normalisedConfig);

    return new CDCReplicator({
      id: this.getDefaultId(normalisedConfig.database),
      syncRuleProvider: syncRuleProvider,
      storageEngine: context.storageEngine,
      metricsEngine: context.metricsEngine,
      connectionFactory: connectionFactory,
      rateLimiter: new MSSQLErrorRateLimiter(),
      pollingOptions: normalisedConfig.cdcPollingOptions
    });
  }

  /**
   * Combines base config with normalized connection settings
   */
  private resolveConfig(config: types.MSSQLConnectionConfig): types.ResolvedMSSQLConnectionConfig {
    return {
      ...config,
      ...types.normalizeConnectionConfig(config)
    };
  }

  async teardown(options: TearDownOptions): Promise<void> {
    // No specific teardown required for MSSQL
  }

  async testConnection(config: types.MSSQLConnectionConfig) {
    this.decodeConfig(config);
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    return await MSSQLModule.testConnection(normalizedConfig);
  }

  static async testConnection(normalizedConfig: types.ResolvedMSSQLConnectionConfig): Promise<ConnectionTestResult> {
    const connectionManager = new MSSQLConnectionManager(normalizedConfig, { max: 1 });
    try {
      const errors = await checkSourceConfiguration(connectionManager);
      if (errors.length > 0) {
        throw new Error(errors.join('\n'));
      }
    } finally {
      await connectionManager.end();
    }
    return {
      connectionDescription: normalizedConfig.hostname
    };
  }
}
