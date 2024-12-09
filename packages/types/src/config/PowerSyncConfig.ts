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

const jwk = t.union(t.union(jwkRSA, jwkHmac), jwkOKP);

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
      // This uses the generic config which may have additional fields
      connections: t.array(genericDataSourceConfig).optional()
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
      supabase_jwt_secret: t.string.optional(),
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
    .optional(),

  parameters: t.record(t.number.or(t.string).or(t.boolean).or(t.Null)).optional()
});

export type PowerSyncConfig = t.Decoded<typeof powerSyncConfig>;
export type SerializedPowerSyncConfig = t.Encoded<typeof powerSyncConfig>;
