import * as lib_postgres from '@powersync/lib-service-postgres';
import { ErrorCode, ServiceError } from '@powersync/lib-services-framework';
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
    snapshot_socket_timeout: t.number.optional(),
    /**
     * Interval in seconds between source connection heartbeats. Null or omitted uses the default.
     */
    heartbeat_interval_seconds: service_types.orNull(t.number).optional()
  })
);

/**
 * Config input specified when starting services
 */
export type PostgresConnectionConfig = t.Decoded<typeof PostgresConnectionConfig>;

/**
 * Resolved version of {@link PostgresConnectionConfig}
 */
export type ResolvedConnectionConfig = PostgresConnectionConfig &
  NormalizedPostgresConnectionConfig & {
    heartbeat_interval_seconds: number;
  };

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
    snapshot_socket_timeout_ms: lib_postgres.parseConnectTimeout(options.snapshot_socket_timeout, undefined),
    heartbeat_interval_seconds: normalizeHeartbeatInterval(options.heartbeat_interval_seconds)
  } satisfies NormalizedPostgresConnectionConfig & { heartbeat_interval_seconds: number };
}

const DEFAULT_PING_INTERVAL_SECONDS = 60;
const MIN_PING_INTERVAL_SECONDS = 5;
const MAX_PING_INTERVAL_SECONDS = 60;

function normalizeHeartbeatInterval(value: number | null | undefined): number {
  if (value == null) {
    return DEFAULT_PING_INTERVAL_SECONDS;
  }
  if (!Number.isFinite(value) || value < MIN_PING_INTERVAL_SECONDS || value > MAX_PING_INTERVAL_SECONDS) {
    throw new ServiceError(
      ErrorCode.PSYNC_S1109,
      `Postgres connection: heartbeat_interval_seconds must be between ${MIN_PING_INTERVAL_SECONDS} and ${MAX_PING_INTERVAL_SECONDS} seconds`
    );
  }
  return value;
}
