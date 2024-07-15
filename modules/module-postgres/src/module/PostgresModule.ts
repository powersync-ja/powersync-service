import * as t from 'ts-codec';

import { api, replication } from '@powersync/service-core';

import { PostgresConnectionConfig } from '../types/types.js';
import { PostgresReplicationAdapter } from '../replication/PostgresReplicationAdapter.js';
import { PostgresSyncAPIAdapter } from '../replication/PostgresSyncAPIAdapter.js';

export class PostgresModule extends replication.ReplicationModule {
  protected configSchema(): t.AnyCodec {
    // Intersection types have some limitations in codec typing
    return PostgresConnectionConfig;
  }

  protected createSyncAPIAdapter(config: PostgresConnectionConfig): api.SyncAPI {
    throw new PostgresSyncAPIAdapter(config);
  }

  protected createReplicationAdapter(config: PostgresConnectionConfig): PostgresReplicationAdapter {
    return new PostgresReplicationAdapter(config);
  }

  public shutdown(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  public teardown(): Promise<void> {
    throw new Error('Method not implemented.');
  }
}
