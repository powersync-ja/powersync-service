import * as urijs from 'uri-js';
import { PostgresConnection } from './PowerSyncConfig.js';

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnection(options: PostgresConnection): NormalizedPostgresConnection {
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
  const port = validatePort(options.port ?? uri.port ?? 5432);

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
    password,
    sslmode,
    cacert,

    client_certificate: options.client_certificate ?? undefined,
    client_private_key: options.client_private_key ?? undefined
  };
}

export interface NormalizedPostgresConnection {
  id: string;
  tag: string;

  hostname: string;
  port: number;
  database: string;

  username: string;
  password: string;

  sslmode: 'verify-full' | 'verify-ca' | 'disable';
  cacert: string | undefined;

  client_certificate: string | undefined;
  client_private_key: string | undefined;
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
  if (port < 1024) {
    throw new Error(`Port ${port} not supported`);
  }
  return port;
}

/**
 * Construct a postgres URI, without username, password or ssl options.
 *
 * Only contains hostname, port, database.
 */
export function baseUri(options: NormalizedPostgresConnection) {
  return `postgresql://${options.hostname}:${options.port}/${options.database}`;
}
