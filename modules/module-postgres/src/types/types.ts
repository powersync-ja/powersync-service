import * as lib_postgres from '@powersync/lib-service-postgres';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';

// Maintain backwards compatibility by exporting these
export const validatePort = lib_postgres.validatePort;
export const baseUri = lib_postgres.baseUri;
export type NormalizedPostgresConnectionConfig = lib_postgres.NormalizedBasePostgresConnectionConfig;
export const POSTGRES_CONNECTION_TYPE = lib_postgres.POSTGRES_CONNECTION_TYPE;

export const PostgresConnectionConfig = service_types.configFile.DataSourceConfig.and(
  lib_postgres.BasePostgresConnectionConfig
).and(
  t.object({
    // Add any replication connection specific config here in future
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
    ...lib_postgres.normalizeConnectionConfig(options)
  } satisfies NormalizedPostgresConnectionConfig;
}
