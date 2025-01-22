import {
  api,
  ConfigurationFileSyncRulesProvider,
  ConnectionTestResult,
  replication,
  system,
  TearDownOptions
} from '@powersync/service-core';

import { MySQLRouteAPIAdapter } from '../api/MySQLRouteAPIAdapter.js';
import { BinLogReplicator } from '../replication/BinLogReplicator.js';
import { MySQLErrorRateLimiter } from '../replication/MySQLErrorRateLimiter.js';
import * as types from '../types/types.js';
import { MySQLConnectionManagerFactory } from '../replication/MySQLConnectionManagerFactory.js';
import { MySQLConnectionConfig } from '../types/types.js';
import { checkSourceConfiguration } from '../common/check-source-configuration.js';
import { MySQLConnectionManager } from '../replication/MySQLConnectionManager.js';

export class MySQLModule extends replication.ReplicationModule<types.MySQLConnectionConfig> {
  constructor() {
    super({
      name: 'MySQL',
      type: types.MYSQL_CONNECTION_TYPE,
      configSchema: types.MySQLConnectionConfig
    });
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    await super.initialize(context);
  }

  protected createRouteAPIAdapter(): api.RouteAPI {
    return new MySQLRouteAPIAdapter(this.resolveConfig(this.decodedConfig!));
  }

  protected createReplicator(context: system.ServiceContext): replication.AbstractReplicator {
    const normalisedConfig = this.resolveConfig(this.decodedConfig!);
    const syncRuleProvider = new ConfigurationFileSyncRulesProvider(context.configuration.sync_rules);
    const connectionFactory = new MySQLConnectionManagerFactory(normalisedConfig);

    return new BinLogReplicator({
      id: this.getDefaultId(normalisedConfig.database),
      syncRuleProvider: syncRuleProvider,
      storageEngine: context.storageEngine,
      connectionFactory: connectionFactory,
      rateLimiter: new MySQLErrorRateLimiter()
    });
  }

  /**
   * Combines base config with normalized connection settings
   */
  private resolveConfig(config: types.MySQLConnectionConfig): types.ResolvedConnectionConfig {
    return {
      ...config,
      ...types.normalizeConnectionConfig(config)
    };
  }

  async teardown(options: TearDownOptions): Promise<void> {
    // No specific teardown required for MySQL
  }

  async testConnection(config: MySQLConnectionConfig) {
    this.decodeConfig(config);
    const normalizedConfig = this.resolveConfig(this.decodedConfig!);
    return await MySQLModule.testConnection(normalizedConfig);
  }

  static async testConnection(normalizedConfig: types.ResolvedConnectionConfig): Promise<ConnectionTestResult> {
    const connectionManager = new MySQLConnectionManager(normalizedConfig, {});
    const connection = await connectionManager.getConnection();
    try {
      const errors = await checkSourceConfiguration(connection);
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
