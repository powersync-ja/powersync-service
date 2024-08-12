import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';
import * as urijs from 'uri-js';

export const MSSQL_CONNECTION_TYPE = 'mssql' as const;

export interface NormalizedMSSQLConnectionConfig {
  id: string;
  tag: string;

  hostname: string;
  port: number;
  database: string;

  username: string;
  password: string;

  //   TODO other fields
}

export const MSSQLConnectionConfig = service_types.configFile.dataSourceConfig.and(
  t.object({
    type: t.literal(MSSQL_CONNECTION_TYPE),
    /** Unique identifier for the connection - optional when a single connection is present. */
    id: t.string.optional(),
    /** Tag used as reference in sync rules. Defaults to "default". Does not have to be unique. */
    tag: t.string.optional(),
    uri: t.string.optional(),
    hostname: t.string.optional(),
    port: service_types.configFile.portCodec.optional(),
    username: t.string.optional(),
    password: t.string.optional(),
    database: t.string.optional()
  })
);

/**
 * Config input specified when starting services
 */
export type MSSQLConnectionConfig = t.Decoded<typeof MSSQLConnectionConfig>;

/**
 * Resolved version of {@link PostgresConnectionConfig}
 */
export type ResolvedConnectionConfig = MSSQLConnectionConfig & NormalizedMSSQLConnectionConfig;

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: MSSQLConnectionConfig): NormalizedMSSQLConnectionConfig {
  //   TOOD
  let uri: urijs.URIComponents;
  if (options.uri) {
    uri = urijs.parse(options.uri);
    if (uri.scheme != 'postgresql' && uri.scheme != 'postgres') {
      `Invalid URI - protocol must be postgresql, got ${uri.scheme}`;
    } else if (uri.scheme != 'postgresql') {
      uri.scheme = 'postgresql';
    }
  } else {
    uri = urijs.parse('postgresql:///');
  }

  const hostname = options.hostname ?? uri.host ?? '';
  const port = Number(options.port ?? uri.port ?? 5432);

  const database = options.database ?? uri.path?.substring(1) ?? '';

  const [uri_username, uri_password] = (uri.userinfo ?? '').split(':');

  const username = options.username ?? uri_username ?? '';
  const password = options.password ?? uri_password ?? '';

  const sslmode = options.sslmode ?? 'verify-full'; // Configuration not supported via URI
  const cacert = options.cacert;

  if (sslmode == 'verify-ca' && cacert == null) {
    throw new Error('Explicit cacert is required for sslmode=verify-ca');
  }

  if (hostname == '') {
    throw new Error(`hostname required`);
  }

  if (username == '') {
    throw new Error(`username required`);
  }

  if (password == '') {
    throw new Error(`password required`);
  }

  if (database == '') {
    throw new Error(`database required`);
  }

  return {
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',

    hostname,
    port,
    database,

    username,
    password
  };
}
