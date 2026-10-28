import { system, utils } from '@powersync/service-core';
import { PostgresModule } from '@powersync/service-module-postgres';
import {
  ReplicationChildServiceSetup,
  ReplicationChildSourceImplementation
} from '../../../replication/ReplicationChildSourceImplementation.js';

export interface PostgresReplicationChildImplementationOptions {
  readonly uri: string;
  readonly schema: string;
}

export class PostgresReplicationChildImplementation implements ReplicationChildSourceImplementation {
  constructor(private readonly options: PostgresReplicationChildImplementationOptions) {}

  getServiceSetup(): ReplicationChildServiceSetup {
    const configuration = {
      storage: { type: 'benchmark' },
      api_parameters: {
        max_concurrent_connections: 1,
        max_data_fetch_concurrency: 1,
        max_buckets_per_connection: 1_000,
        max_parameter_query_results: 1_000,
        checkpoint_request_retention_minutes: 60,
        bucket_count_cache_ttl_minutes: 60
      },
      telemetry: { disable_telemetry_sharing: true, internal_service_endpoint: '' },
      sync_rules: { present: false, exit_on_error: true },
      api_tokens: [],
      jwt_audiences: [],
      token_max_expiration: '1h',
      metadata: {},
      port: 0,
      slot_name_prefix: 'benchmark_',
      healthcheck: { probes: { use_filesystem: false, use_http: false, use_legacy: false } },
      parameters: {},
      base_config: {},
      client_keystore: {},
      connections: [
        {
          type: 'postgresql',
          uri: this.options.uri,
          sslmode: 'disable',
          heartbeat_interval_seconds: 5
        }
      ]
    } as unknown as utils.ResolvedPowerSyncConfig;
    return { configuration, defaultSchema: this.options.schema };
  }

  async initialize(context: system.ServiceContextContainer): Promise<void> {
    await new PostgresModule().initialize(context);
  }
}
