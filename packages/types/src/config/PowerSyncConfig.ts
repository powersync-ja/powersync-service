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

export const dataSourceConfig = t.object({
  // Unique string identifier for the data source
  type: t.string,
  /** Unique identifier for the connection - optional when a single connection is present. */
  id: t.string.optional()
});

export type DataSourceConfig = t.Decoded<typeof dataSourceConfig>;

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

const jwk = t.union(jwkRSA, jwkHmac);

export const strictJwks = t.object({
  keys: t.array(jwk)
});

export type StrictJwk = t.Decoded<typeof jwk>;

export const storageConfig = t.object({
  type: t.literal('mongodb'),
  uri: t.string,
  database: t.string.optional(),
  username: t.string.optional(),
  password: t.string.optional()
});

export type StorageConfig = t.Decoded<typeof storageConfig>;

export const powerSyncConfig = t.object({
  replication: t
    .object({
      data_sources: t.array(dataSourceConfig).optional()
    })
    .optional(),

  dev: t
    .object({
      demo_auth: t.boolean.optional(),
      demo_password: t.string.optional(),
      crud_api: t.boolean.optional(),
      demo_client: t.boolean.optional()
    })
    .optional(),

  client_auth: t
    .object({
      jwks_uri: t.string.or(t.array(t.string)).optional(),
      block_local_jwks: t.boolean.optional(),
      jwks: strictJwks.optional(),
      supabase: t.boolean.optional(),
      audience: t.array(t.string).optional()
    })
    .optional(),

  api: t
    .object({
      tokens: t.array(t.string).optional()
    })
    .optional(),

  storage: storageConfig,

  port: portCodec.optional(),
  sync_rules: t
    .object({
      path: t.string.optional(),
      content: t.string.optional()
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
      disable_telemetry_sharing: t.boolean,
      internal_service_endpoint: t.string.optional()
    })
    .optional()
});

export type PowerSyncConfig = t.Decoded<typeof powerSyncConfig>;
export type SerializedPowerSyncConfig = t.Encoded<typeof powerSyncConfig>;
