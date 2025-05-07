import dedent from 'dedent';
import * as t from 'ts-codec';

/**
 * The meta tags here are used in the generated JSON schema.
 * The JSON schema can be used to help self hosted users edit the YAML config file.
 */

/**
 * Users might specify ports as strings if using YAML custom tag environment substitutions
 */
export const portCodec = t
  .codec<number, number | string>(
    'Port',
    (value) => value,
    (value) => (typeof value == 'number' ? value : parseInt(value))
  )
  .meta({
    description:
      'A network port value that can be specified as either a number or a string that will be parsed to a number.'
  });

/**
 * This gets used whenever generating a JSON schema
 */
export const portParser = {
  tag: portCodec._tag,
  parse: () => ({
    anyOf: [{ type: 'number' }, { type: 'string' }]
  })
};

export const DataSourceConfig = t
  .object({
    // Unique string identifier for the data source
    type: t.string.meta({
      description: 'Unique string identifier for the data source type (e.g., "postgresql", "mysql", etc.).'
    }),
    /** Unique identifier for the connection - optional when a single connection is present. */
    id: t.string
      .meta({
        description: 'Unique identifier for the connection. Optional when only a single connection is present.'
      })
      .optional(),
    /** Additional meta tag for connection */
    tag: t.string
      .meta({
        description: 'Additional meta tag for the connection, used for categorization or grouping.'
      })
      .optional(),
    /**
     * Allows for debug query execution
     */
    debug_api: t.boolean
      .meta({
        description: 'When enabled, allows query execution.'
      })
      .optional()
  })
  .meta({
    description: 'Base configuration for a replication data source connection.'
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

export const jwkRSA = t
  .object({
    kty: t.literal('RSA').meta({
      description: 'Key type identifier, must be "RSA" for RSA keys.'
    }),
    kid: t.string.meta({
      description: 'Key ID, a unique identifier for the key.'
    }),
    n: t.string.meta({
      description: 'RSA modulus, Base64 URL encoded.'
    }),
    e: t.string.meta({
      description: 'RSA exponent, Base64 URL encoded.'
    }),
    alg: t
      .literal('RS256')
      .or(t.literal('RS384'))
      .or(t.literal('RS512'))
      .meta({
        description: 'The algorithm intended for use with this key (RS256, RS384, or RS512).'
      })
      .optional(),
    use: t.string
      .meta({
        description: 'The intended use of the key (e.g., "sig" for signature).'
      })
      .optional()
  })
  .meta({
    description: 'JSON Web Key (JWK) representation of an RSA key.'
  });

export const jwkHmac = t
  .object({
    kty: t.literal('oct').meta({
      description: 'Key type identifier, must be "oct" for HMAC keys.'
    }),
    kid: t.string
      .meta({
        description:
          'Key ID. Undefined kid indicates it can match any JWT, with or without a kid. Use a kid wherever possible.'
      })
      .optional(),
    k: t.string.meta({
      description: 'The HMAC key value, Base64 URL encoded.'
    }),
    alg: t.literal('HS256').or(t.literal('HS384')).or(t.literal('HS512')).meta({
      description: 'The algorithm intended for use with this key (HS256, HS384, or HS512).'
    }),
    use: t.string
      .meta({
        description: 'The intended use of the key (e.g., "sig" for signature).'
      })
      .optional()
  })
  .meta({
    description: 'JSON Web Key (JWK) representation of an HMAC key.'
  });

export const jwkOKP = t
  .object({
    kty: t.literal('OKP').meta({
      description: 'Key type identifier, must be "OKP" for Octet Key Pair keys.'
    }),
    kid: t.string
      .meta({
        description: 'Key ID, a unique identifier for the key.'
      })
      .optional(),
    /** Other curves have security issues so only these two are supported. */
    crv: t.literal('Ed25519').or(t.literal('Ed448')).meta({
      description: 'The cryptographic curve used with this key. Only Ed25519 and Ed448 are supported.'
    }),
    x: t.string.meta({
      description: 'The public key, Base64 URL encoded.'
    }),
    alg: t.literal('EdDSA').meta({
      description: 'The algorithm intended for use with this key (EdDSA).'
    }),
    use: t.string
      .meta({
        description: 'The intended use of the key (e.g., "sig" for signature).'
      })
      .optional()
  })
  .meta({
    description:
      'JSON Web Key (JWK) representation of an Octet Key Pair (OKP) key used with Edwards-curve Digital Signature Algorithm.'
  });

export const jwkEC = t
  .object({
    kty: t.literal('EC').meta({
      description: 'Key type identifier, must be "EC" for Elliptic Curve keys.'
    }),
    kid: t.string
      .meta({
        description: 'Key ID, a unique identifier for the key.'
      })
      .optional(),
    crv: t.literal('P-256').or(t.literal('P-384')).or(t.literal('P-512')).meta({
      description: 'The cryptographic curve used with this key (P-256, P-384, or P-512).'
    }),
    x: t.string.meta({
      description: 'The x coordinate for the Elliptic Curve point, Base64 URL encoded.'
    }),
    y: t.string.meta({
      description: 'The y coordinate for the Elliptic Curve point, Base64 URL encoded.'
    }),
    alg: t.literal('ES256').or(t.literal('ES384')).or(t.literal('ES512')).meta({
      description: 'The algorithm intended for use with this key (ES256, ES384, or ES512).'
    }),
    use: t.string
      .meta({
        description: 'The intended use of the key (e.g., "sig" for signature).'
      })
      .optional()
  })
  .meta({
    description: 'JSON Web Key (JWK) representation of an Elliptic Curve key.'
  });

const jwk = t.union(t.union(t.union(jwkRSA, jwkHmac), jwkOKP), jwkEC).meta({
  description: 'A JSON Web Key (JWK) representing a cryptographic key. Can be RSA, HMAC, OKP, or EC key types.'
});

export const strictJwks = t
  .object({
    keys: t.array(jwk).meta({
      description: 'An array of JSON Web Keys (JWKs).'
    })
  })
  .meta({
    description: 'A JSON Web Key Set (JWKS) containing a collection of JWKs.'
  });

export type StrictJwk = t.Decoded<typeof jwk>;

export const BaseStorageConfig = t
  .object({
    type: t.string.meta({
      description: 'The type of storage backend to use (e.g., "postgresql", "mongodb").'
    }),
    max_pool_size: t.number
      .meta({
        description: 'Maximum number of connections to the storage database, per process. Defaults to 8.'
      })
      .optional()
  })
  .meta({
    description: 'Base configuration for storage connections.'
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

export const powerSyncConfig = t
  .object({
    replication: t
      .object({
        // This uses the generic config which may have additional fields
        connections: t
          .array(genericDataSourceConfig)
          .meta({
            description: 'Array of data source connections used for replication.'
          })
          .optional()
      })
      .meta({
        description: 'Configuration for data replication services.'
      })
      .optional(),

    dev: t
      .object({
        demo_auth: t.boolean
          .meta({
            description: 'Enables demo authentication for development purposes.'
          })
          .optional(),
        /** @deprecated */
        demo_password: t.string
          .meta({
            description: 'Deprecated. Demo password for development authentication.'
          })
          .optional(),
        /** @deprecated */
        crud_api: t.boolean
          .meta({
            description: 'Deprecated. Enables CRUD API for development.'
          })
          .optional(),
        /** @deprecated */
        demo_client: t.boolean
          .meta({
            description: 'Deprecated. Enables demo client for development.'
          })
          .optional()
      })
      .meta({
        description: 'Development-specific configuration options.'
      })
      .optional(),

    client_auth: t
      .object({
        jwks_uri: t.string
          .or(t.array(t.string))
          .meta({
            description: 'URI or array of URIs pointing to JWKS endpoints for client authentication.'
          })
          .optional(),
        block_local_jwks: t.boolean
          .meta({
            description: 'When true, blocks JWKS URIs that resolve to local network addresses.'
          })
          .optional(),
        jwks_reject_ip_ranges: t
          .array(t.string)
          .meta({
            description: 'IP ranges to reject when validating JWKS URIs.'
          })
          .optional(),
        jwks: strictJwks
          .meta({
            description: 'Inline JWKS configuration for client authentication.'
          })
          .optional(),
        supabase: t.boolean
          .meta({
            description: 'Enables Supabase authentication integration.'
          })
          .optional(),
        supabase_jwt_secret: t.string
          .meta({
            description: 'JWT secret for Supabase authentication.'
          })
          .optional(),
        audience: t
          .array(t.string)
          .meta({
            description: 'Valid audiences for JWT validation.'
          })
          .optional()
      })
      .meta({
        description: 'Configuration for client authentication mechanisms.'
      })
      .optional(),

    api: t
      .object({
        tokens: t
          .array(t.string)
          .meta({
            description: 'API access tokens for administrative operations.'
          })
          .optional(),
        parameters: t
          .object({
            max_concurrent_connections: t.number
              .meta({
                description: dedent`
              Maximum number of connections (http streams or websockets) per API process.
              Default of 200.
            `
              })
              .optional(),

            max_data_fetch_concurrency: t.number
              .meta({
                description: dedent`
              This should not be siginificantly more than storage.max_pool_size, otherwise it would block on the
              pool. Increasing this can significantly increase memory usage in some cases.
              Default of 10.
            `
              })
              .optional(),

            max_buckets_per_connection: t.number
              .meta({
                description: dedent`
              Maximum number of buckets for each connection.
              More buckets increase latency and memory usage. While the actual number is controlled by sync rules,
              having a hard limit ensures that the service errors instead of crashing when a sync rule is misconfigured.
              Default of 1000.
            `
              })
              .optional(),

            max_parameter_query_results: t.number
              .meta({
                description: dedent`
              Related to max_buckets_per_connection, but this limit applies directly on the parameter
              query results, _before_ we convert it into an unique set.
              Default of 1000.
            `
              })
              .optional()
          })
          .meta({
            description: 'Performance and safety parameters for the API service.'
          })
          .optional()
      })
      .meta({
        description: 'API service configuration and parameters.'
      })
      .optional(),

    storage: GenericStorageConfig.meta({
      description: 'Configuration for the storage backend.'
    }),

    port: portCodec
      .meta({
        description:
          'The port on which the service will listen for connections. Can be specified as a number or string.'
      })
      .optional(),

    sync_rules: t
      .object({
        path: t.string
          .meta({
            description: 'Path to the sync rules file or directory.'
          })
          .optional(),
        content: t.string
          .meta({
            description: 'Inline sync rules content as a string.'
          })
          .optional(),
        exit_on_error: t.boolean
          .meta({
            description: 'Whether to exit the process if there is an error parsing sync rules.'
          })
          .optional()
      })
      .meta({
        description: 'Configuration for synchronization rules that define data access patterns.'
      })
      .optional(),

    metadata: t
      .record(t.string)
      .meta({
        description: 'Custom metadata key-value pairs for the service.'
      })
      .optional(),

    migrations: t
      .object({
        disable_auto_migration: t.boolean
          .meta({
            description: 'When true, disables automatic storage database schema migrations.'
          })
          .optional()
      })
      .meta({
        description: 'Configuration for database schema migrations.'
      })
      .optional(),

    telemetry: t
      .object({
        // When set, metrics will be available on this port for scraping by Prometheus.
        prometheus_port: portCodec
          .meta({
            description:
              'Port on which Prometheus metrics will be exposed. When set, metrics will be available on this port for scraping.'
          })
          .optional(),
        disable_telemetry_sharing: t.boolean.meta({
          description: 'When true, disables sharing of anonymized telemetry data.'
        }),
        internal_service_endpoint: t.string
          .meta({
            description: 'Internal endpoint for telemetry services.'
          })
          .optional()
      })
      .meta({
        description: 'Configuration for service telemetry and monitoring.'
      })
      .optional(),

    parameters: t
      .record(t.number.or(t.string).or(t.boolean).or(t.Null))
      .meta({
        description: 'Global parameters that can be referenced in sync rules and other configurations.'
      })
      .optional()
  })
  .meta({
    description: 'Root configuration object for PowerSync service.'
  });

export type PowerSyncConfig = t.Decoded<typeof powerSyncConfig>;
export type SerializedPowerSyncConfig = t.Encoded<typeof powerSyncConfig>;

export const PowerSyncConfigJSONSchema = t.generateJSONSchema(powerSyncConfig, {
  allowAdditional: true,
  parsers: [portParser]
});
