import * as t from 'ts-codec';

import { api, replication } from '@powersync/service-core';

import { ServiceContext } from '@powersync/service-core/src/system/ServiceContext.js';
import { PostgresSyncAPIAdapter } from '../api/PostgresSyncAPIAdapter.js';
import { PostgresReplicationAdapter } from '../replication/PostgresReplicationAdapter.js';
import { normalizeConnectionConfig, PostgresConnectionConfig, ResolvedConnectionConfig } from '../types/types.js';

export class PostgresModule extends replication.ReplicationModule {
  constructor() {
    super({
      name: 'Postgres',
      type: 'postgres'
    });
  }

  async register(context: ServiceContext): Promise<void> {}

  protected configSchema(): t.AnyCodec {
    // Intersection types have some limitations in codec typing
    return PostgresConnectionConfig;
  }

  protected createSyncAPIAdapter(config: PostgresConnectionConfig): api.RouteAPI {
    throw new PostgresSyncAPIAdapter(this.resolveConfig(config));
  }

  protected createReplicationAdapter(config: PostgresConnectionConfig): PostgresReplicationAdapter {
    return new PostgresReplicationAdapter(this.resolveConfig(config));
  }

  public teardown(): Promise<void> {
    throw new Error('Method not implemented.');
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
}
