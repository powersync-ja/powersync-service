import { api, auth, replication, system } from '@powersync/service-core';
import * as jpgwire from '@powersync/service-jpgwire';
import * as t from 'ts-codec';

import { logger } from '@powersync/lib-services-framework';
import * as types from '../types/types.js';

import { PostgresSyncAPIAdapter } from '../api/PostgresSyncAPIAdapter.js';
import { SupabaseKeyCollector } from '../auth/SupabaseKeyCollector.js';
import { PostgresReplicationAdapter } from '../replication/PostgresReplicationAdapter.js';

export class PostgresModule extends replication.ReplicationModule {
  constructor() {
    super({
      name: 'Postgres',
      type: types.POSTGRES_CONNECTION_TYPE
    });
  }

  protected configSchema(): t.AnyCodec {
    // Intersection types have some limitations in codec typing
    return types.PostgresConnectionConfig;
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

  protected createSyncAPIAdapter(config: types.PostgresConnectionConfig): api.RouteAPI {
    return new PostgresSyncAPIAdapter(this.resolveConfig(config));
  }

  protected createReplicationAdapter(config: types.PostgresConnectionConfig): PostgresReplicationAdapter {
    return new PostgresReplicationAdapter(this.resolveConfig(config));
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
