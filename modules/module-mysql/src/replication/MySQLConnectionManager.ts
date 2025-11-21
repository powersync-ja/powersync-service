import { NormalizedMySQLConnectionConfig } from '../types/types.js';
import mysqlPromise from 'mysql2/promise';
import mysql, { FieldPacket, RowDataPacket } from 'mysql2';
import * as mysql_utils from '../utils/mysql-utils.js';
import { BaseObserver, logger } from '@powersync/lib-services-framework';
import { ZongJi } from '@powersync/mysql-zongji';

export interface MySQLConnectionManagerListener {
  onEnded(): void;
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
   * Create a new replication listener
   */
  createBinlogListener(): ZongJi {
    const listener = new ZongJi({
      host: this.options.hostname,
      port: this.options.port,
      user: this.options.username,
      password: this.options.password,
      // We want to avoid parsing date/time values to Date, because that drops sub-millisecond precision.
      dateStrings: true
    });

    this.binlogListeners.push(listener);

    return listener;
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
