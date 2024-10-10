import { ResolvedConnectionConfig } from '../types/types.js';
import mysqlPromise from 'mysql2/promise';
import mysql, { RowDataPacket } from 'mysql2';
import * as mysql_utils from '../utils/mysql_utils.js';
import ZongJi from '@powersync/mysql-zongji';

export class MySQLConnectionManager {
  /**
   *  Pool that can create streamable connections
   */
  private readonly pool: mysql.Pool;
  /**
   *  Pool that can create promise-based connections
   */
  private readonly promisePool: mysqlPromise.Pool;

  private binlogListeners: ZongJi[] = [];

  constructor(
    public options: ResolvedConnectionConfig,
    public poolOptions: mysqlPromise.PoolOptions
  ) {
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
      user: this.options.username,
      password: this.options.password
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
  async query(query: string, params?: any[]) {
    return this.promisePool.query<RowDataPacket[]>(query, params);
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
    for (const listener of this.binlogListeners) {
      listener.stop();
    }

    await new Promise<void>((resolve, reject) => {
      this.pool.end((err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  destroy() {
    for (const listener of this.binlogListeners) {
      listener.stop();
    }
  }
}
