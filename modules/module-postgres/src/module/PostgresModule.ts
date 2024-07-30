import * as t from 'ts-codec';

import { api, auth, db, replication, storage, system } from '@powersync/service-core';

import { logger } from '@powersync/lib-services-framework';
import * as types from '../types/types.js';

import { PostgresSyncAPIAdapter } from '../api/PostgresSyncAPIAdapter.js';
import { SupabaseKeyCollector } from '../auth/SupabaseKeyCollector.js';
import { PostgresReplicationAdapter } from '../replication/PostgresReplicationAdapter.js';
import { normalizeConnectionConfig, PostgresConnectionConfig, ResolvedConnectionConfig } from '../types/types.js';
import { terminateReplicators } from '../utils/teardown.js';

export class PostgresModule extends replication.ReplicationModule {
  constructor() {
    super({
      name: 'Postgres',
      type: types.POSTGRES_CONNECTION_TYPE
    });
  }

  protected configSchema(): t.AnyCodec {
    // Intersection types have some limitations in codec typing
    return PostgresConnectionConfig;
  }

  async register(context: system.ServiceContext): Promise<void> {}

  async initialize(context: system.ServiceContext): Promise<void> {
    // Register the Supabase key collector(s)
    (context.configuration.data_sources ?? [])
      .map((baseConfig) => {
        if (baseConfig.type != types.POSTGRES_CONNECTION_TYPE) {
          return;
        }
        try {
          return this.resolveConfig(PostgresConnectionConfig.decode(baseConfig as any));
        } catch (ex) {
          logger.warn('Failed to decode configuration in Postgres module initialization.');
        }
      })
      .filter((c) => !!c)
      .forEach((config) => {
        const keyCollector = new SupabaseKeyCollector(config);
        context.withLifecycle(keyCollector, {
          // Close the internal pool
          stop: (collector) => collector.shutdown()
        });
        context.configuration.client_keystore.add(new auth.CachedKeyCollector(keyCollector));
      });
  }

  protected createSyncAPIAdapter(config: PostgresConnectionConfig): api.RouteAPI {
    throw new PostgresSyncAPIAdapter(this.resolveConfig(config));
  }

  protected createReplicationAdapter(config: PostgresConnectionConfig): PostgresReplicationAdapter {
    return new PostgresReplicationAdapter(this.resolveConfig(config));
  }

  /**
   * Combines base config with normalized connection settings
   */
  private resolveConfig(config: PostgresConnectionConfig): ResolvedConnectionConfig {
    return {
      ...config,
      ...normalizeConnectionConfig(config)
    };
  }

  async teardown(context: system.ServiceContext): Promise<void> {
    const mongoDB = storage.createPowerSyncMongo(context.configuration.storage);
    try {
      // TODO this should not be necessary since the service context
      // has already been initialized.
      // However we need a direct mongo connection for this.
      // Maybe we can add termination methods to the storage.
      // TODO improve this when other storage methods or connections are implemented
      logger.info(`Waiting for auth`);
      await db.mongo.waitForAuth(mongoDB.db);

      logger.info(`Terminating replication slots`);
      const connections = (context.configuration.data_sources ?? [])
        .filter((c) => c.type == 'postgresql')
        .map((c) => PostgresConnectionConfig.decode(c as any));

      for (const connection of connections) {
        await terminateReplicators(context.storage, this.resolveConfig(connection));
      }

      const database = mongoDB.db;
      logger.info(`Dropping database ${database.namespace}`);
      await database.dropDatabase();
      logger.info(`Done`);
      await mongoDB.client.close();

      // If there was an error connecting to postgress, the process may stay open indefinitely.
      // This forces an exit.
      // We do not consider those errors a teardown failure.
      process.exit(0);
    } catch (e) {
      logger.error(`Teardown failure`, e);
      await mongoDB.client.close();
      process.exit(1);
    }
  }
}
