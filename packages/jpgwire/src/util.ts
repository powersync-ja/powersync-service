import * as datefns from 'date-fns';
import * as net from 'node:net';
import * as tls from 'node:tls';
import { DEFAULT_CERTS } from './certs.js';
import * as pgwire from './pgwire.js';
import { PgType } from './pgwire_types.js';
import { ConnectOptions } from './socket_adapter.js';
import { DatabaseInputValue, DateTimeValue } from '@powersync/service-sync-rules';

// TODO this is duplicated, but maybe that is ok
export interface NormalizedConnectionConfig {
  id: string;
  tag: string;

  hostname: string;
  port: number;
  database: string;

  username: string;
  password: string;

  sslmode: 'verify-full' | 'verify-ca' | 'disable';
  cacert: string | undefined;

  /**
   * Specify to use a servername for TLS that is different from hostname.
   *
   * Only relevant if sslmode = 'verify-full'.
   */
  tls_servername: string | undefined;

  /**
   * Hostname lookup function.
   */
  lookup?: net.LookupFunction;

  client_certificate: string | undefined;
  client_private_key: string | undefined;
}

type Mutable<T> = {
  -readonly [P in keyof T]: T[P];
};

export interface PgWireConnectionOptions extends NormalizedConnectionConfig {
  resolved_ip?: string;
}

export function clientTlsOptions(options: PgWireConnectionOptions): tls.ConnectionOptions {
  if (options.client_certificate && options.client_private_key) {
    return {
      cert: options.client_certificate,
      key: options.client_private_key
    };
  } else {
    return {};
  }
}

export function makeTlsOptions(options: PgWireConnectionOptions): false | tls.ConnectionOptions {
  if (options.sslmode == 'disable') {
    return false;
  } else if (options.sslmode == 'verify-full') {
    // Validate against default CA, or supplied CA.
    // Also validate hostname.
    return {
      ca: options.cacert ?? DEFAULT_CERTS,
      // hostname for TLS validation.
      // This may be different from the host we're connecting to if we pre-resolved
      // the IP.
      host: options.hostname,
      servername: options.tls_servername ?? options.hostname,
      ...clientTlsOptions(options)
    };
  } else if (options.sslmode == 'verify-ca') {
    if (options.cacert == null) {
      throw new Error(`cacert required for verify-ca`);
    }
    return {
      ca: options.cacert!,
      host: options.hostname,
      servername: options.hostname,
      // Disable hostname check
      checkServerIdentity: () => undefined,
      ...clientTlsOptions(options)
    };
  } else {
    throw new Error(`Unsupported sslmode: ${options.sslmode}`);
  }
}

export async function connectPgWire(
  config: PgWireConnectionOptions,
  options?: { type?: 'standard' | 'replication'; applicationName: string }
) {
  let connectionOptions: Mutable<pgwire.PgConnectKnownOptions> = {
    application_name: options?.applicationName ?? 'powersync',

    // tlsOptions below contains the original hostname
    hostname: config.resolved_ip ?? config.hostname,
    port: config.port,
    database: config.database,

    user: config.username,
    password: config.password
  };

  if (options?.type != 'standard') {
    connectionOptions.replication = 'database';
  }

  let tlsOptions: tls.ConnectionOptions | false = false;

  if (config.sslmode != 'disable') {
    connectionOptions.sslmode = 'require';

    tlsOptions = makeTlsOptions(config);
  } else {
    connectionOptions.sslmode = 'disable';
  }
  const connection = pgwire.pgconnection(connectionOptions as pgwire.PgConnectOptions);

  // HACK: Not standard pgwire options
  // Just the easiest way to pass on our config to SocketAdapter
  const connectOptions = (connection as any)._connectOptions as ConnectOptions;
  connectOptions.tlsOptions = tlsOptions;
  connectOptions.lookup = config.lookup;

  // HACK: Replace row decoding with our own implementation
  (connection as any)._recvDataRow = _recvDataRow;

  await (connection as any).start();
  return connection;
}

function _recvDataRow(this: any, _message: any, row: (Uint8Array | DatabaseInputValue)[], batch: any) {
  for (let i = 0; i < this._rowColumns.length; i++) {
    const valbuf = row[i];
    if (valbuf == null) {
      continue;
    }
    const { binary, typeOid } = this._rowColumns[i];
    // TODO avoid this._clientTextDecoder.decode for bytea
    row[i] = binary
      ? // do not valbuf.slice() because nodejs Buffer .slice does not copy
        // TODO but we not going to receive Buffer here ?
        Uint8Array.prototype.slice.call(valbuf)
      : PgType.decode(this._rowTextDecoder.decode(valbuf), typeOid);
  }
  batch.rows.push(row);
}

export interface PgPoolOptions {
  /**
   * Maximum number of open connections.
   *
   * Once this limit is reached, queries are queued.
   *
   * Defaults to 5.
   */
  maxSize?: number;

  /**
   * Idle timeout in ms before a connection is closed.
   */
  idleTimeout?: number | undefined;

  applicationName?: string;
}

/**
 * Open a postgres pool.
 *
 * The pool cannot be used for transactions, and cannot be used for logical replication.
 */
export function connectPgWirePool(config: PgWireConnectionOptions, options?: PgPoolOptions) {
  const idleTimeout = options?.idleTimeout;
  const maxSize = options?.maxSize ?? 5;

  let connectionOptions: Mutable<pgwire.PgConnectKnownOptions> = {
    application_name: options?.applicationName ?? 'powersync',

    // tlsOptions below contains the original hostname
    hostname: config.resolved_ip ?? config.hostname,
    port: config.port,
    database: config.database,

    user: config.username,
    password: config.password,

    _poolSize: maxSize,
    _poolIdleTimeout: idleTimeout
  };

  let tlsOptions: tls.ConnectionOptions | false = false;
  if (config.sslmode != 'disable') {
    connectionOptions.sslmode = 'require';

    tlsOptions = makeTlsOptions(config);
  } else {
    connectionOptions.sslmode = 'disable';
  }

  const pool = pgwire.pgpool(connectionOptions as pgwire.PgConnectOptions);
  const originalGetConnection = (pool as any)._getConnection;
  (pool as any)._getConnection = function (this: any) {
    const con = originalGetConnection.call(this);

    const connectOptions = (con as any)._connectOptions as ConnectOptions;
    connectOptions.tlsOptions = tlsOptions;
    connectOptions.lookup = config.lookup;

    // HACK: Replace row decoding with our own implementation
    (con as any)._recvDataRow = _recvDataRow;
    return con;
  };
  return pool;
}

export function lsnMakeComparable(text: string) {
  const [h, l] = text.split('/');
  return h.padStart(8, '0') + '/' + l.padStart(8, '0');
}

const timeRegex = /^([\d\-]+) ([\d:]+)(\.\d+)?([+-][\d:]+)?$/;

/**
 * Convert a postgres timestamptz to a SQLite-compatible/normalized timestamp.
 *
 * We output in the format:
 * YYYY-MM-DD HH:MM:SSZ
 * YYYY-MM-DD HH:MM:SS.SSSZ
 *
 * Precision is preserved from the source string, with the exception of ".000".
 *
 * We have specific exceptions for -infinity and infinity.
 */
export function timestamptzToSqlite(source?: string): DateTimeValue | null {
  if (source == null) {
    return null;
  }
  // Make compatible with SQLite
  const match = timeRegex.exec(source);
  if (match == null) {
    if (source == 'infinity') {
      return new DateTimeValue('9999-12-31T23:59:59Z');
    } else if (source == '-infinity') {
      return new DateTimeValue('0000-01-01T00:00:00Z');
    } else {
      return null;
    }
  }
  const [_, date, time, precision, timezone] = match as any;
  // datefns.parseISO() supports some dates that new Date() doesn't, e.g. '0022-09-08 21:19:28+00'
  const parsed = datefns.parseISO(`${date}T${time}${timezone}`);
  // Some dates are outside the range of JS and/or SQLite, e.g. 10022-09-08 21:19:28+00.
  // Just fallback to null for now.
  // We may consider preserving the source value in these cases.
  if (isNaN(parsed.getTime())) {
    return null;
  }

  const baseValue = parsed.toISOString().replace('.000', '').replace('Z', '');

  // In the new format, we always use ISO 8601. Since Postgres drops zeroes from the fractional seconds, we also pad
  // that back to the highest theoretical precision (microseconds). This ensures that sorting returned values as text
  // returns them in order of the time value they represent.
  //
  // In the old format, we keep the sub-second precision only if it's not `.000`.
  const missingPrecision = precision?.padEnd(7, '0') ?? '.000000';
  return new DateTimeValue(`${baseValue}${missingPrecision}Z`, `${baseValue.replace('T', ' ')}${precision ?? ''}Z`);
}

/**
 * For timestamp without timezone, we keep it mostly as-is.
 *
 * We make specific exceptions for 'infinity' and '-infinity'.
 *
 * https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-VALUES
 */
export function timestampToSqlite(source?: string): DateTimeValue | null {
  if (source == null) {
    return null;
  }

  const match = timeRegex.exec(source);
  if (match == null) {
    if (source == 'infinity') {
      return new DateTimeValue('9999-12-31T23:59:59');
    } else if (source == '-infinity') {
      return new DateTimeValue('0000-01-01T00:00:00');
    } else {
      return null;
    }
  }

  const [_, date, time, precision, __] = match as any;
  const missingPrecision = precision?.padEnd(7, '0') ?? '.000000';

  return new DateTimeValue(`${date}T${time}${missingPrecision}`, source);
}
/**
 * For date, we keep it mostly as-is.
 *
 * We make specific exceptions for 'infinity' and '-infinity'.
 *
 * https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-VALUES
 */
export function dateToSqlite(source?: string) {
  if (source == null) {
    return null;
  }
  if (source == 'infinity') {
    return '9999-12-31';
  } else if (source == '-infinity') {
    return '0000-01-01';
  } else {
    return source;
  }
}

/**
 * PgResult rows are arrays of values.
 *
 * This converts it to objects.
 */
export function pgwireRows<T = Record<string, any>>(rs: pgwire.PgResult): T[] {
  const columns = rs.columns;
  return rs.rows.map((row) => {
    let r: T = {} as any;
    for (let i = 0; i < columns.length; i++) {
      const c = columns[i];
      (r as any)[c.name] = row[i];
    }
    return r;
  });
}
