import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';
import * as urijs from 'uri-js';

export const MYSQL_CONNECTION_TYPE = 'mysql' as const;

export interface NormalizedMySQLConnectionConfig {
  id: string;
  tag: string;

  hostname: string;
  port: number;
  database: string;

  username: string;
  password: string;
}

export const MySQLConnectionConfig = service_types.configFile.dataSourceConfig.and(
  t.object({
    type: t.literal(MYSQL_CONNECTION_TYPE),
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
export type MySQLConnectionConfig = t.Decoded<typeof MySQLConnectionConfig>;

/**
 * Resolved version of {@link MySQLConnectionConfig}
 */
export type ResolvedConnectionConfig = MySQLConnectionConfig & NormalizedMySQLConnectionConfig;

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: MySQLConnectionConfig): NormalizedMySQLConnectionConfig {
  let uri: urijs.URIComponents;
  if (options.uri) {
    uri = urijs.parse(options.uri);
    if (uri.scheme != 'mysql') {
      throw new Error(`Invalid URI - protocol must be mysql, got ${uri.scheme}`);
    }
  } else {
    uri = urijs.parse('postgresql:///');
  }

  const hostname = options.hostname ?? uri.host ?? '';
  const port = Number(options.port ?? uri.port ?? 3306);

  const database = options.database ?? uri.path?.substring(1) ?? '';

  const [uri_username, uri_password] = (uri.userinfo ?? '').split(':');

  const username = options.username ?? uri_username ?? '';
  const password = options.password ?? uri_password ?? '';

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
