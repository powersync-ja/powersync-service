import * as t from 'ts-codec';

/**
 * Users might specify ports as strings if using YAML custom tag environment substitutions
 */
export const portCodec = t.codec<number, number | string>(
  'Port',
  (value) => value,
  (value) => (typeof value == 'number' ? value : parseInt(value))
);

/**
 * This gets used whenever generating a JSON schema
 */
export const portParser = {
  tag: portCodec._tag,
  parse: () => ({
    anyOf: [{ type: 'number' }, { type: 'string' }]
  })
};

/**
 * Health check probe types
 */
export enum ProbeType {
  FILESYSTEM = 'filesystem',
  HTTP = 'http',
  /**
   * @deprecated
   * This applies legacy defaults. This should  not be used directly.
   * This enables HTTP probes when the service is started in the API or UNIFIED mode.
   * The FILESYTEM probe is always registered by default.
   */
  LEGACY_DEFAULT = 'legacy_default'
}

export const DataSourceConfig = t.object({
  // Unique string identifier for the data source
  type: t.string,
  /** Unique identifier for the connection - optional when a single connection is present. */
  id: t.string.optional(),
  /** Additional meta tag for connection */
  tag: t.string.optional(),
  /**
   * Allows for debug query execution
   */
  debug_api: t.boolean.optional()
});

export type DataSourceConfig = t.Decoded<typeof DataSourceConfig>;

/**
 * Resolved version of {@link DataSourceConfig} where the optional
 * `id` and `tag` field is now required.
 */
export const ResolvedDataSourceConfig = DataSourceConfig.and(
  t.object({
    id: t.string,
    tag: t.string
  })
);

export type ResolvedDataSourceConfig = t.Decoded<typeof ResolvedDataSourceConfig>;

/**
 * This essentially allows any extra fields on this type
 */
export const genericDataSourceConfig = DataSourceConfig.and(t.record(t.any));
export type GenericDataSourceConfig = t.Decoded<typeof genericDataSourceConfig>;

export const jwkRSA = t.object({
  kty: t.literal('RSA'),
  kid: t.string,
  n: t.string,
  e: t.string,
  alg: t.literal('RS256').or(t.literal('RS384')).or(t.literal('RS512')).optional(),
  use: t.string.optional()
});

export const jwkHmac = t.object({
  kty: t.literal('oct'),
  /**
   * undefined kid indicates it can match any JWT, with or without a kid.
   * Use a kid wherever possible.
   */
  kid: t.string.optional(),
  k: t.string,
  alg: t.literal('HS256').or(t.literal('HS384')).or(t.literal('HS512')),
  use: t.string.optional()
});

export const jwkOKP = t.object({
  kty: t.literal('OKP'),
  kid: t.string.optional(),
  /** Other curves have security issues so only these two are supported. */
  crv: t.literal('Ed25519').or(t.literal('Ed448')),
  x: t.string,
  alg: t.literal('EdDSA'),
  use: t.string.optional()
});

export const jwkEC = t.object({
  kty: t.literal('EC'),
  kid: t.string.optional(),
  crv: t.literal('P-256').or(t.literal('P-384')).or(t.literal('P-512')),
  x: t.string,
  y: t.string,
  alg: t.literal('ES256').or(t.literal('ES384')).or(t.literal('ES512')),
  use: t.string.optional()
});

const jwk = t.union(t.union(t.union(jwkRSA, jwkHmac), jwkOKP), jwkEC);

export const strictJwks = t.object({
  keys: t.array(jwk)
});

export type StrictJwk = t.Decoded<typeof jwk>;

export const BaseStorageConfig = t.object({
  type: t.string,
  // Maximum number of connections to the storage database, per process.
  // Defaults to 8.
  max_pool_size: t.number.optional()
});

/**
 * Base configuration for Bucket storage connections.
 */
export type BaseStorageConfig = t.Encoded<typeof BaseStorageConfig>;

/**
 * This essentially allows any extra fields on this type
 */
export const GenericStorageConfig = BaseStorageConfig.and(t.record(t.any));
export type GenericStorageConfig = t.Encoded<typeof GenericStorageConfig>;

export const powerSyncConfig = t.object({
  replication: t
    .object({
      // This uses the generic config which may have additional fields
      connections: t.array(genericDataSourceConfig).optional()
    })
    .optional(),

  dev: t
    .object({
      demo_auth: t.boolean.optional(),
      /** @deprecated */
      demo_password: t.string.optional(),
      /** @deprecated */
      crud_api: t.boolean.optional(),
      /** @deprecated */
      demo_client: t.boolean.optional()
    })
    .optional(),

  client_auth: t
    .object({
      jwks_uri: t.string.or(t.array(t.string)).optional(),
      block_local_jwks: t.boolean.optional(),
      jwks_reject_ip_ranges: t.array(t.string).optional(),
      jwks: strictJwks.optional(),
      supabase: t.boolean.optional(),
      supabase_jwt_secret: t.string.optional(),
      audience: t.array(t.string).optional()
    })
    .optional(),

  api: t
    .object({
      tokens: t.array(t.string).optional(),
      parameters: t
        .object({
          // Maximum number of connections (http streams or websockets) per API process.
          // Default of 200.
          max_concurrent_connections: t.number.optional(),
          // This should not be siginificantly more than storage.max_pool_size, otherwise it would block on the
          // pool. Increasing this can significantly increase memory usage in some cases.
          // Default of 10.
          max_data_fetch_concurrency: t.number.optional(),
          // Maximum number of buckets for each connection.
          // More buckets increase latency and memory usage. While the actual number is controlled by sync rules,
          // having a hard limit ensures that the service errors instead of crashing when a sync rule is misconfigured.
          // Default of 1000.
          max_buckets_per_connection: t.number.optional(),

          // Related to max_buckets_per_connection, but this limit applies directly on the parameter
          // query results, _before_ we convert it into an unique set.
          // Default of 1000.
          max_parameter_query_results: t.number.optional()
        })
        .optional()
    })
    .optional(),

  storage: GenericStorageConfig,

  port: portCodec.optional(),
  sync_rules: t
    .object({
      path: t.string.optional(),
      content: t.string.optional(),
      exit_on_error: t.boolean.optional()
    })
    .optional(),

  metadata: t.record(t.string).optional(),

  migrations: t
    .object({
      disable_auto_migration: t.boolean.optional()
    })
    .optional(),

  telemetry: t
    .object({
      // When set, metrics will be available on this port for scraping by Prometheus.
      prometheus_port: portCodec.optional(),
      disable_telemetry_sharing: t.boolean,
      internal_service_endpoint: t.string.optional()
    })
    .optional(),

  service: t
    .object({
      health_checks: t
        .object({
          probe_modes: t.array(t.Enum(ProbeType)).optional()
        })
        .optional()
    })
    .optional(),

  parameters: t.record(t.number.or(t.string).or(t.boolean).or(t.Null)).optional()
});

export type PowerSyncConfig = t.Decoded<typeof powerSyncConfig>;
export type SerializedPowerSyncConfig = t.Encoded<typeof powerSyncConfig>;
