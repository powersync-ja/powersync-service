import { api, replication, system } from '@powersync/service-core';
import * as t from 'ts-codec';

import { MySQLAPIAdapter } from '../api/MySQLAPIAdapter.js';
import { MSSQLReplicationAdapter } from '../replication/MSSQLReplicationAdapter.js';
import * as types from '../types/types.js';

export class MySQLModule extends replication.ReplicationModule {
  constructor() {
    super({
      name: 'MySQL',
      type: types.MYSQL_CONNECTION_TYPE
    });
  }

  protected configSchema(): t.AnyCodec {
    // Intersection types have some limitations in codec typing
    return types.MySQLConnectionConfig;
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    await super.initialize(context);

    // jpgwire.setMetricsRecorder({
    //   addBytesRead(bytes) {
    //     context.metrics.data_replicated_bytes.add(bytes);
    //   }
    // });
  }

  protected createSyncAPIAdapter(config: types.MySQLConnectionConfig): api.RouteAPI {
    return new MySQLAPIAdapter(this.resolveConfig(config));
  }

  protected createReplicationAdapter(config: types.MySQLConnectionConfig): MSSQLReplicationAdapter {
    return new MSSQLReplicationAdapter(this.resolveConfig(config));
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
