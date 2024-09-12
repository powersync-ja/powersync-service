import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';
import * as urijs from 'uri-js';

export const MONGO_CONNECTION_TYPE = 'mongodb' as const;

export interface NormalizedMongoConnectionConfig {
  id: string;
  tag: string;

  uri: string;
  database: string;

  username?: string;
  password?: string;
}

export const MongoConnectionConfig = service_types.configFile.dataSourceConfig.and(
  t.object({
    type: t.literal(MONGO_CONNECTION_TYPE),
    /** Unique identifier for the connection - optional when a single connection is present. */
    id: t.string.optional(),
    /** Tag used as reference in sync rules. Defaults to "default". Does not have to be unique. */
    tag: t.string.optional(),
    uri: t.string.optional(),
    hostname: t.string.optional(),
    port: service_types.configFile.portCodec.optional(),
    username: t.string.optional(),
    password: t.string.optional(),
    database: t.string.optional(),

    /** Defaults to verify-full */
    sslmode: t.literal('verify-full').or(t.literal('verify-ca')).or(t.literal('disable')).optional(),
    /** Required for verify-ca, optional for verify-full */
    cacert: t.string.optional(),

    client_certificate: t.string.optional(),
    client_private_key: t.string.optional()
  })
);

/**
 * Config input specified when starting services
 */
export type MongoConnectionConfig = t.Decoded<typeof MongoConnectionConfig>;

/**
 * Resolved version of {@link MongoConnectionConfig}
 */
export type ResolvedConnectionConfig = MongoConnectionConfig & NormalizedMongoConnectionConfig;

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: MongoConnectionConfig): NormalizedMongoConnectionConfig {
  let uri: urijs.URIComponents;
  if (options.uri) {
    uri = urijs.parse(options.uri);
    if (uri.scheme != 'mongodb') {
      `Invalid URI - protocol must be postgresql, got ${uri.scheme}`;
    }
  } else {
    uri = urijs.parse('mongodb:///');
  }

  const database = options.database ?? uri.path?.substring(1) ?? '';

  const [uri_username, uri_password] = (uri.userinfo ?? '').split(':');

  const username = options.username ?? uri_username;
  const password = options.password ?? uri_password;

  if (database == '') {
    throw new Error(`database required`);
  }

  return {
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',

    // TODO: remove username & password from uri
    uri: options.uri ?? '',
    database,

    username,
    password
  };
}

/**
 * Check whether the port is in a "safe" range.
 *
 * We do not support connecting to "privileged" ports.
 */
export function validatePort(port: string | number): number {
  if (typeof port == 'string') {
    port = parseInt(port);
  }
  if (port >= 1024 && port <= 49151) {
    return port;
  } else {
    throw new Error(`Port ${port} not supported`);
  }
}

/**
 * Construct a mongodb URI, without username, password or ssl options.
 *
 * Only contains hostname, port, database.
 */
export function baseUri(options: NormalizedMongoConnectionConfig) {
  return options.uri;
}
