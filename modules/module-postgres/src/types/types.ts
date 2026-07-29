import * as lib_postgres from '@powersync/lib-service-postgres';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';

// Maintain backwards compatibility by exporting these
export const validatePort = lib_postgres.validatePort;
export const baseUri = lib_postgres.baseUri;
export interface NormalizedPostgresConnectionConfig extends lib_postgres.NormalizedBasePostgresConnectionConfig {
  snapshot_socket_timeout_ms?: number | undefined;
}
export const POSTGRES_CONNECTION_TYPE = lib_postgres.POSTGRES_CONNECTION_TYPE;

export const PostgresConnectionConfig = service_types.configFile.DataSourceConfig.and(
  lib_postgres.BasePostgresConnectionConfig
).and(
  t.object({
    /**
     * Idle timeout in seconds for snapshot connection sockets.
     *
     * Defaults to 30 seconds. When the storage cannot keep up with the snapshot,
     * a storage flush can stall the snapshot loop for longer than this, killing
     * the source connection mid-snapshot. Raising the timeout gives the source
     * connection more slack under storage backpressure.
     */
    snapshot_socket_timeout: t.number.optional()
  })
);

/**
 * Config input specified when starting services
 */
export type PostgresConnectionConfig = t.Decoded<typeof PostgresConnectionConfig>;

/**
 * Resolved version of {@link PostgresConnectionConfig}
 */
export type ResolvedConnectionConfig = PostgresConnectionConfig & NormalizedPostgresConnectionConfig;

export function isPostgresConfig(
  config: service_types.configFile.DataSourceConfig
): config is PostgresConnectionConfig {
  return config.type == lib_postgres.POSTGRES_CONNECTION_TYPE;
}

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: PostgresConnectionConfig) {
  return {
    ...lib_postgres.normalizeConnectionConfig(options),
    snapshot_socket_timeout_ms: lib_postgres.parseConnectTimeout(options.snapshot_socket_timeout, undefined)
  } satisfies NormalizedPostgresConnectionConfig;
}
