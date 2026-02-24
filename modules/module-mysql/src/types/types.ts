import { ErrorCode, makeHostnameLookupFunction, ServiceError } from '@powersync/lib-services-framework';
import * as service_types from '@powersync/service-types';
import { LookupFunction } from 'node:net';
import * as t from 'ts-codec';
import * as urijs from 'uri-js';

export const MYSQL_CONNECTION_TYPE = 'mysql' as const;

/**
 * Connection parameters that can be parsed from the MySQL URI query string.
 *
 * All values are in milliseconds (for timeouts) or counts (for limits).
 * MySQL uses camelCase naming convention.
 */
export interface MySQLConnectionParams {
  connectTimeout?: number;
  connectionLimit?: number;
  queueLimit?: number;
}

export interface NormalizedMySQLConnectionConfig {
  id: string;
  tag: string;

  hostname: string;
  port: number;
  database: string;

  username: string;
  password: string;
  server_id: number;

  cacert?: string;
  client_certificate?: string;
  client_private_key?: string;

  lookup?: LookupFunction;

  binlog_queue_memory_limit: number;

  connectionParams: MySQLConnectionParams;
}

export const MySQLConnectionConfig = service_types.configFile.DataSourceConfig.and(
  t.object({
    type: t.literal(MYSQL_CONNECTION_TYPE),
    uri: t.string.optional(),
    hostname: t.string.optional(),
    port: service_types.configFile.portCodec.optional(),
    username: t.string.optional(),
    password: t.string.optional(),
    database: t.string.optional(),
    server_id: t.number.optional(),

    cacert: t.string.optional(),
    client_certificate: t.string.optional(),
    client_private_key: t.string.optional(),

    reject_ip_ranges: t.array(t.string).optional(),
    // The combined size of binlog events that can be queued in memory before throttling is applied.
    binlog_queue_memory_limit: t.number.optional()
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
      throw new ServiceError(
        ErrorCode.PSYNC_S1109,
        `Invalid URI - protocol must be mysql, got ${JSON.stringify(uri.scheme)}`
      );
    }
  } else {
    uri = urijs.parse('mysql:///');
  }

  const hostname = options.hostname ?? uri.host ?? '';
  const port = Number(options.port ?? uri.port ?? 3306);

  const database = options.database ?? uri.path?.substring(1) ?? '';

  const [uri_username, uri_password] = (uri.userinfo ?? '').split(':');

  const username = options.username ?? uri_username ?? '';
  const password = options.password ?? uri_password ?? '';

  if (hostname == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1106, `MySQL connection: hostname required`);
  }

  if (username == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1107, `MySQL connection: username required`);
  }

  if (password == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1108, `MySQL connection: password required`);
  }

  if (database == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1105, `MySQL connection: database required`);
  }

  const lookup = makeHostnameLookupFunction(hostname, { reject_ip_ranges: options.reject_ip_ranges ?? [] });

  // Parse connection parameters from URL query string
  const uriQuery = uri.query ? new URLSearchParams(uri.query) : undefined;
  const connectionParams = parseMySQLConnectionParams(uriQuery);

  return {
    id: options.id ?? 'default',
    tag: options.tag ?? 'default',

    hostname,
    port,
    database,

    username,
    password,

    server_id: options.server_id ?? 1,

    // Binlog processing queue memory limit before throttling is applied.
    binlog_queue_memory_limit: options.binlog_queue_memory_limit ?? 50,

    lookup,

    connectionParams
  };
}

/**
 * Parse a single numeric connection parameter from a URI query string value.
 *
 * Returns undefined if the value is missing, not a valid number, NaN, negative, zero, or Infinity.
 */
export function parseMySQLConnectionParam(value: string | null | undefined): number | undefined {
  if (value == null) {
    return undefined;
  }
  const parsed = Number(value);
  if (isFinite(parsed) && parsed > 0) {
    return parsed;
  }
  return undefined;
}

/**
 * Parse connection parameters from a MySQL URI's query string.
 *
 * MySQL uses camelCase naming convention.
 * Invalid values (NaN, negative, non-numeric) are silently ignored.
 */
export function parseMySQLConnectionParams(searchParams: URLSearchParams | undefined): MySQLConnectionParams {
  const params: MySQLConnectionParams = {};

  if (searchParams == null) {
    return params;
  }

  const connectTimeout = parseMySQLConnectionParam(searchParams.get('connectTimeout'));
  if (connectTimeout != null) {
    params.connectTimeout = connectTimeout;
  }

  const connectionLimit = parseMySQLConnectionParam(searchParams.get('connectionLimit'));
  if (connectionLimit != null) {
    params.connectionLimit = connectionLimit;
  }

  const queueLimit = parseMySQLConnectionParam(searchParams.get('queueLimit'));
  if (queueLimit != null) {
    params.queueLimit = queueLimit;
  }

  return params;
}
