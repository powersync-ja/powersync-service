import { BaseObserver, logger } from '@powersync/lib-services-framework';
import { ZongJi, ZongjiOptions } from '@powersync/mysql-zongji';
import { createConnection, VlaskyConnection } from '@vlasky/mysql';
import mysql, { FieldPacket, RowDataPacket } from 'mysql2';
import mysqlPromise from 'mysql2/promise';
import { NormalizedMySQLConnectionConfig } from '../types/types.js';
import * as mysql_utils from '../utils/mysql-utils.js';

export interface MySQLConnectionManagerListener {
  onEnded(): void;
}

export interface BinlogListenerConnections {
  zongji: ZongJi;
  /**
   *  The connection Zongji uses for table metadata queries and the KILL query issued during stop.
   *  Created by us so that we keep a handle on it: Zongji does not destroy connections it did not
   *  create, so the owner of the BinLogListener is responsible for destroying it.
   */
  controlConnection: VlaskyConnection;
}

export class MySQLConnectionManager extends BaseObserver<MySQLConnectionManagerListener> {
  /**
   *  Pool that can create streamable connections
   */
  private readonly pool: mysql.Pool;
  /**
   *  Pool that can create promise-based connections
   */
  private readonly promisePool: mysqlPromise.Pool;

  private binlogListeners: ZongJi[] = [];
  private controlConnections: VlaskyConnection[] = [];

  private isClosed = false;

  constructor(
    public options: NormalizedMySQLConnectionConfig,
    public poolOptions: mysqlPromise.PoolOptions
  ) {
    super();
    // The pool is lazy - no connections are opened until a query is performed.
    this.pool = mysql_utils.createPool(options, poolOptions);
    this.promisePool = this.pool.promise();
  }

  public get connectionTag() {
    return this.options.tag;
  }

  public get connectionId() {
    return this.options.id;
  }

  public get databaseName() {
    return this.options.database;
  }

  /**
   * Create a new replication listener, along with the control connection it uses.
   */
  createBinlogListener(): BinlogListenerConnections {
    // We create the control connection ourselves and pass it to Zongji, so that we keep a handle
    // on it for liveness probes and cleanup. Zongji creates its binlog connection from a copy of
    // this connection's config, so the options here apply to both connections.
    const controlConnection = createConnection({
      host: this.options.hostname,
      port: this.options.port,
      user: this.options.username,
      password: this.options.password,
      // TCP keepalive is disabled by default in @vlasky/mysql. Without it, the idle control
      // connection can be silently dropped by stateful firewalls, freezing replication on the
      // next table metadata query until the TCP retransmission timeout (~950s).
      enableKeepAlive: true,
      keepAliveInitialDelay: mysql_utils.TCP_KEEPALIVE_INITIAL_DELAY,
      // We want to avoid parsing date/time values to Date, because that drops sub-millisecond precision.
      dateStrings: true,
      timeZone: 'Z'
    });
    // The published ZongjiOptions type does not cover passing in an existing connection yet.
    const listener = new ZongJi(controlConnection as unknown as ZongjiOptions);

    this.binlogListeners.push(listener);
    this.controlConnections.push(controlConnection);

    return { zongji: listener, controlConnection };
  }

  /**
   *  Run a query using a connection from the pool
   *  A promise with the result is returned
   *  @param query
   *  @param params
   */
  async query(query: string, params?: any[]): Promise<[RowDataPacket[], FieldPacket[]]> {
    let connection: mysqlPromise.PoolConnection | undefined;
    try {
      connection = await this.promisePool.getConnection();
      await connection.query(`SET time_zone = '+00:00'`);
      return connection.query<RowDataPacket[]>(query, params);
    } finally {
      connection?.release();
    }
  }

  /**
   *  Get a streamable connection from this manager's pool
   *  The connection should be released when it is no longer needed
   */
  async getStreamingConnection(): Promise<mysql.PoolConnection> {
    return new Promise((resolve, reject) => {
      this.pool.getConnection((err, connection) => {
        if (err) {
          reject(err);
        } else {
          resolve(connection);
        }
      });
    });
  }

  /**
   *  Get a promise connection from this manager's pool
   *  The connection should be released when it is no longer needed
   */
  async getConnection(): Promise<mysqlPromise.PoolConnection> {
    return this.promisePool.getConnection();
  }

  async end(): Promise<void> {
    if (this.isClosed) {
      return;
    }

    for (const listener of this.binlogListeners) {
      listener.stop();
    }

    // Zongji does not destroy connections it did not create.
    for (const connection of this.controlConnections) {
      connection.destroy();
    }

    try {
      await this.promisePool.end();
    } catch (error) {
      // We don't particularly care if any errors are thrown when shutting down the pool
      logger.warn('Error shutting down MySQL connection pool', error);
    } finally {
      this.isClosed = true;
      this.iterateListeners((listener) => {
        listener.onEnded?.();
      });
    }
  }
}
