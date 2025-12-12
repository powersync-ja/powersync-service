import * as datefns from 'date-fns';
import * as net from 'node:net';
import * as tls from 'node:tls';
import { DEFAULT_CERTS } from './certs.js';
import * as pgwire from './pgwire.js';
import { PgType, postgresTimeOptions } from './pgwire_types.js';
import { ConnectOptions } from './socket_adapter.js';
import { DateTimeValue } from '@powersync/service-sync-rules';

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
    host: config.resolved_ip ?? config.hostname,
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
  } else {
    connectionOptions.sslmode = 'disable';
  }

  const connection = pgwire.pgconnection(connectionOptions as pgwire.PgConnectOptions);
  patchConnection(connection, config);

  // To test the connection, send an empty query. This appears to be the only way to establish a connection, see e.g.
  // this: https://github.com/exe-dealer/pgwire/blob/24465b25768ef0d9048acee1fddc748cf1690a14/mod.js#L26.
  try {
    await connection.query();
  } catch (e) {
    await connection.end();

    if (e instanceof Error && e.message == 'postgres query failed') {
      // We didn't actually send a query, so we report the inner cause instead:
      // https://github.com/exe-dealer/pgwire/blob/24465b25768ef0d9048acee1fddc748cf1690a14/mod.js#L334
      throw e.cause;
    }
    throw e;
  }

  return connection;
}

function patchConnection(connection: pgwire.PgConnection, config: PgWireConnectionOptions) {
  // Just the easiest way to pass on our config to SocketAdapter
  const connectOptions = (connection as any)._socketOptions as ConnectOptions;
  connectOptions.tlsOptions = makeTlsOptions(config);
  connectOptions.lookup = config.lookup;

  // Hack for safety: We always want to be responsible for decoding values ourselves, and NEVER
  // use the PgType.decode implementation from pgwire. We can use our own implementation by using
  // row.raw, this ensures that forgetting to do that is an error instead of potentially corrupted
  // data.
  // Original definition: https://github.com/exe-dealer/pgwire/blob/24465b25768ef0d9048acee1fddc748cf1690a14/mod.js#L679-L701
  (connection as any)._recvRowDescription = async function (this: any, m: any, columns: any) {
    class RowImplementation implements pgwire.PgRow {
      declare readonly columns: pgwire.ColumnDescription[];

      constructor(readonly raw: (string | Uint8Array)[]) {}

      get length() {
        return this.raw.length;
      }

      decodeWithoutCustomTypes(index: number) {
        return PgType.decode(this.raw[index], this.columns[index].typeOid);
      }
    }

    Object.defineProperties(RowImplementation.prototype, {
      columns: { enumerable: true, value: columns }
    });

    for (const [i] of columns.entries()) {
      Object.defineProperty(RowImplementation.prototype, i, {
        enumerable: false,
        get() {
          throw new Error(
            `Illegal call to PgRow[i]. Use decodeWithoutCustomTypes(i) instead, or use a custom type registry.`
          );
        }
      });
    }

    this._rowCtor = RowImplementation;
    await this._fwdBemsg(m);
  };
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
    host: config.resolved_ip ?? config.hostname,
    port: config.port,
    database: config.database,

    user: config.username,
    password: config.password,

    _poolSize: maxSize,
    _poolIdleTimeout: idleTimeout
  };

  if (config.sslmode != 'disable') {
    connectionOptions.sslmode = 'require';
  } else {
    connectionOptions.sslmode = 'disable';
  }

  const pool = pgwire.pgpool(connectionOptions as pgwire.PgConnectOptions);
  const originalGetConnection = (pool as any)._getConnection;
  (pool as any)._getConnection = function (this: any) {
    const con = originalGetConnection.call(this);
    patchConnection(con, config);
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
      return new DateTimeValue('9999-12-31T23:59:59Z', undefined, postgresTimeOptions);
    } else if (source == '-infinity') {
      return new DateTimeValue('0000-01-01T00:00:00Z', undefined, postgresTimeOptions);
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

  // In the old format, we keep the sub-second precision only if it's not `.000`.
  return new DateTimeValue(
    `${baseValue}${precision ?? ''}Z`,
    `${baseValue.replace('T', ' ')}${precision ?? ''}Z`,
    postgresTimeOptions
  );
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
      return new DateTimeValue('9999-12-31T23:59:59', undefined, postgresTimeOptions);
    } else if (source == '-infinity') {
      return new DateTimeValue('0000-01-01T00:00:00', undefined, postgresTimeOptions);
    } else {
      return null;
    }
  }

  const [_, date, time, precision, __] = match as any;
  const missingPrecision = precision?.padEnd(7, '0') ?? '.000000';

  return new DateTimeValue(`${date}T${time}${missingPrecision}`, source, postgresTimeOptions);
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

      (r as any)[c.name] = row.decodeWithoutCustomTypes(i);
    }
    return r;
  });
}
