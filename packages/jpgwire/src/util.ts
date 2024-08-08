import * as tls from 'tls';
import { DEFAULT_CERTS } from './certs.js';
import * as pgwire from './pgwire.js';
import { PgType } from './pgwire_types.js';
import * as datefns from 'date-fns';

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
export function tlsOptions(options: PgWireConnectionOptions): false | tls.ConnectionOptions {
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
      servername: options.hostname,
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

export async function connectPgWire(config: PgWireConnectionOptions, options?: { type?: 'standard' | 'replication' }) {
  let connectionOptions: Mutable<ExtendedPgwireOptions> = {
    application_name: 'PowerSync',

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

  if (config.sslmode != 'disable') {
    connectionOptions.sslmode = 'require';

    // HACK: Not standard pgwire options
    // Just the easiest way to pass on our config to pgwire_node.js
    connectionOptions.sslrootcert = tlsOptions(config);
  } else {
    connectionOptions.sslmode = 'disable';
  }
  const connection = await pgwire.pgconnect(connectionOptions as pgwire.PgConnectOptions);
  // HACK: Replace row decoding with our own implementation
  (connection as any)._recvDataRow = _recvDataRow;
  return connection;
}

function _recvDataRow(this: any, _message: any, row: Uint8Array[], batch: any) {
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
}

/**
 * Open a postgres pool.
 *
 * The pool cannot be used for transactions, and cannot be used for logical replication.
 */
export function connectPgWirePool(config: PgWireConnectionOptions, options?: PgPoolOptions) {
  const idleTimeout = options?.idleTimeout;
  const maxSize = options?.maxSize ?? 5;

  let connectionOptions: Mutable<ExtendedPgwireOptions> = {
    application_name: 'PowerSync',

    // tlsOptions below contains the original hostname
    hostname: config.resolved_ip ?? config.hostname,
    port: config.port,
    database: config.database,

    user: config.username,
    password: config.password,

    _poolSize: maxSize,
    _poolIdleTimeout: idleTimeout
  };

  if (config.sslmode != 'disable') {
    connectionOptions.sslmode = 'require';

    // HACK: Not standard pgwire options
    // Just the easiest way to pass on our config to pgwire_node.js
    connectionOptions.sslrootcert = tlsOptions(config);
  } else {
    connectionOptions.sslmode = 'disable';
  }

  const pool = pgwire.pgpool(connectionOptions as pgwire.PgConnectOptions);
  const originalGetConnection = (pool as any)._getConnection;
  (pool as any)._getConnection = function (this: any) {
    const con = originalGetConnection.call(this);
    // HACK: Replace row decoding with our own implementation
    (con as any)._recvDataRow = _recvDataRow;
    return con;
  };
  return pool;
}

/**
 * Hack: sslrootcert is passed through as-is to pgwire_node.
 *
 * We use that to pass in custom TLS options, without having to modify pgwire itself.
 */
export interface ExtendedPgwireOptions extends pgwire.PgConnectKnownOptions {
  sslrootcert?: false | tls.ConnectionOptions;
}

export function lsnMakeComparable(text: string) {
  const [h, l] = text.split('/');
  return h.padStart(8, '0') + '/' + l.padStart(8, '0');
}

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
export function timestamptzToSqlite(source?: string) {
  if (source == null) {
    return null;
  }
  // Make compatible with SQLite
  const match = /^([\d\-]+) ([\d:]+)(\.\d+)?([+-][\d:]+)$/.exec(source);
  if (match == null) {
    if (source == 'infinity') {
      return '9999-12-31 23:59:59Z';
    } else if (source == '-infinity') {
      return '0000-01-01 00:00:00Z';
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
  const text = parsed.toISOString().replace('T', ' ').replace('.000', '').replace('Z', '');

  return `${text}${precision ?? ''}Z`;
}

/**
 * For timestamp without timezone, we keep it mostly as-is.
 *
 * We make specific exceptions for 'infinity' and '-infinity'.
 *
 * https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-SPECIAL-VALUES
 */
export function timestampToSqlite(source?: string) {
  if (source == null) {
    return null;
  }
  if (source == 'infinity') {
    return '9999-12-31 23:59:59';
  } else if (source == '-infinity') {
    return '0000-01-01 00:00:00';
  } else {
    return source;
  }
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
export function pgwireRows(rs: pgwire.PgResult): Record<string, any>[] {
  const columns = rs.columns;
  return rs.rows.map((row) => {
    let r: Record<string, any> = {};
    for (let i = 0; i < columns.length; i++) {
      const c = columns[i];
      r[c.name] = row[i];
    }
    return r;
  });
}
