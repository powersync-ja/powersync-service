import { ErrorCode, makeHostnameLookupFunction, ServiceError } from '@powersync/lib-services-framework';
import type * as jpgwire from '@powersync/service-jpgwire';
import * as service_types from '@powersync/service-types';
import * as t from 'ts-codec';
import * as urijs from 'uri-js';

export interface NormalizedBasePostgresConnectionConfig extends jpgwire.NormalizedConnectionConfig {
  max_pool_size: number;
}

export const POSTGRES_CONNECTION_TYPE = 'postgresql' as const;

export const BasePostgresConnectionConfig = t.object({
  /** Unique identifier for the connection - optional when a single connection is present. */
  id: t.string.optional(),
  /** Additional meta tag for connection */
  tag: t.string.optional(),
  type: t.literal(POSTGRES_CONNECTION_TYPE),
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
  client_private_key: t.string.optional(),

  /** Specify to use a servername for TLS that is different from hostname. */
  tls_servername: t.string.optional(),

  /**
   * Block connections in any of these IP ranges.
   *
   * Use 'local' to block anything not in public unicast ranges.
   */
  reject_ip_ranges: t.array(t.string).optional(),

  /**
   * Prefix for the slot name. Defaults to "powersync_"
   */
  slot_name_prefix: t.string.optional(),

  max_pool_size: t.number.optional(),

  /**
   * Connection timeout in seconds (following Postgres convention).
   * Overrides the default socket connect timeout.
   * Takes precedence over the connect_timeout URL query parameter.
   */
  connect_timeout: t.number.optional()
});

export type BasePostgresConnectionConfig = t.Encoded<typeof BasePostgresConnectionConfig>;
export type BasePostgresConnectionConfigDecoded = t.Decoded<typeof BasePostgresConnectionConfig>;

/**
 * Validate and normalize connection options.
 *
 * Returns destructured options.
 */
export function normalizeConnectionConfig(options: BasePostgresConnectionConfigDecoded) {
  let uri: urijs.URIComponents;
  if (options.uri) {
    uri = urijs.parse(options.uri);
    if (uri.scheme != 'postgresql' && uri.scheme != 'postgres') {
      throw new ServiceError(
        ErrorCode.PSYNC_S1109,
        `Invalid URI - protocol must be postgresql, got ${JSON.stringify(uri.scheme)}`
      );
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
    throw new ServiceError(
      ErrorCode.PSYNC_S1104,
      'Postgres connection: Explicit cacert is required for `sslmode: verify-ca`'
    );
  }

  if (hostname == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1106, `Postgres connection: hostname required`);
  }

  if (username == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1107, `Postgres connection: username required`);
  }

  if (password == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1108, `Postgres connection: password required`);
  }

  if (database == '') {
    throw new ServiceError(ErrorCode.PSYNC_S1105, `Postgres connection: database required`);
  }

  // Parse connect_timeout from URL query string (in seconds, per Postgres convention)
  const uriQuery = uri.query ? new URLSearchParams(uri.query) : undefined;
  const connect_timeout_ms = parseConnectTimeout(options.connect_timeout, uriQuery?.get('connect_timeout'));

  const lookupOptions = { reject_ip_ranges: options.reject_ip_ranges ?? [] };
  const lookup = makeHostnameLookupFunction(hostname, lookupOptions);

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

    tls_servername: options.tls_servername ?? undefined,
    lookup,

    client_certificate: options.client_certificate ?? undefined,
    client_private_key: options.client_private_key ?? undefined,

    connect_timeout_ms,

    max_pool_size: options.max_pool_size ?? 8
  } satisfies NormalizedBasePostgresConnectionConfig;
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
  if (port < 1024 || port > 65535) {
    throw new ServiceError(ErrorCode.PSYNC_S1110, `Port ${port} not supported`);
  }
  return port;
}

/**
 * Parse connect_timeout from explicit config or URL query parameter.
 *
 * - Explicit config value takes precedence over URL query param.
 * - Value is in seconds (Postgres convention), converted to milliseconds.
 * - Invalid values (NaN, negative, non-numeric) are silently ignored.
 * - Returns undefined if no valid value is found.
 */
export function parseConnectTimeout(
  explicitValue: number | undefined,
  uriQueryValue: string | null | undefined
): number | undefined {
  // Explicit config takes precedence
  if (explicitValue != null) {
    if (typeof explicitValue === 'number' && isFinite(explicitValue) && explicitValue > 0) {
      return explicitValue * 1000;
    }
    // Invalid explicit value: silently ignore
    return undefined;
  }

  if (uriQueryValue != null) {
    const parsed = Number(uriQueryValue);
    if (isFinite(parsed) && parsed > 0) {
      return parsed * 1000;
    }
    // Invalid URI value: silently ignore
    return undefined;
  }

  return undefined;
}

/**
 * Construct a postgres URI, without username, password or ssl options.
 *
 * Only contains hostname, port, database.
 */
export function baseUri(options: NormalizedBasePostgresConnectionConfig) {
  return `postgresql://${options.hostname}:${options.port}/${options.database}`;
}
