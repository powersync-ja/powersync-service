import { api, replication, system, utils } from '@powersync/service-core';

import { MySQLRouteAPIAdapter } from '../api/MySQLRouteAPIAdapter.js';
import { BinLogReplicator } from '../replication/BinLogReplicator.js';
import { MySQLErrorRateLimiter } from '../replication/MySQLErrorRateLimiter.js';
import * as types from '../types/types.js';

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

    // TODO move this to the binlog consumer
    // jpgwire.setMetricsRecorder({
    //   addBytesRead(bytes) {
    //     context.metrics.data_replicated_bytes.add(bytes);
    //   }
    // });
  }

  protected createRouteAPIAdapter(config: types.MySQLConnectionConfig): api.RouteAPI {
    return new MySQLRouteAPIAdapter(this.resolveConfig(config));
  }

  protected createReplicator(
    config: types.MySQLConnectionConfig,
    context: system.ServiceContext
  ): replication.AbstractReplicator {
    const resolvedConfig = this.resolveConfig(config);
    return new BinLogReplicator({
      connectionConfig: resolvedConfig,
      id: this.getDefaultId(resolvedConfig.database),
      rateLimiter: new MySQLErrorRateLimiter(),
      syncRuleProvider: {
        // TODO should maybe improve this
        get: () => utils.loadSyncRules(context.configuration)
      },
      storageFactory: context.storage
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

  async teardown(): Promise<void> {
    // TODO this needs the service context to operate.
    // Should this keep a refference?
    // const mongoDB = storage.createPowerSyncMongo(context.configuration.storage);
    // try {
    //   // TODO this should not be necessary since the service context
    //   // has already been initialized.
    //   // However we need a direct mongo connection for this.
    //   // Maybe we can add termination methods to the storage.
    //   // TODO improve this when other storage methods or connections are implemented
    //   logger.info(`Waiting for auth`);
    //   await db.mongo.waitForAuth(mongoDB.db);
    //   logger.info(`Terminating replication slots`);
    //   const connections = (context.configuration.connections ?? [])
    //     .filter((c) => c.type == 'postgresql')
    //     .map((c) => types.PostgresConnectionConfig.decode(c as any));
    //   for (const connection of connections) {
    //     await terminateReplicators(context.storage, this.resolveConfig(connection));
    //   }
    //   const database = mongoDB.db;
    //   logger.info(`Dropping database ${database.namespace}`);
    //   await database.dropDatabase();
    //   logger.info(`Done`);
    //   await mongoDB.client.close();
    //   // If there was an error connecting to postgress, the process may stay open indefinitely.
    //   // This forces an exit.
    //   // We do not consider those errors a teardown failure.
    //   process.exit(0);
    // } catch (e) {
    //   logger.error(`Teardown failure`, e);
    //   await mongoDB.client.close();
    //   process.exit(1);
    // }
  }
}
