import { ResolvedConnectionConfig } from '../types/types.js';
import mysql from 'mysql2/promise';
import * as mysql_utils from '../utils/mysql_utils.js';
import ZongJi from '@vlasky/zongji';

export class MySQLConnectionManager {
  /**
   * Do not use this for any transactions.
   */
  public readonly pool: mysql.Pool;

  private binlogListeners: ZongJi[] = [];

  constructor(
    public options: ResolvedConnectionConfig,
    public poolOptions: mysql.PoolOptions
  ) {
    // The pool is lazy - no connections are opened until a query is performed.
    this.pool = mysql_utils.createPool(options, poolOptions);
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

  public get serverId() {
    return this.options.;
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
   *  Get a connection from the pool. It should be released back to the pool when done.
   */
  async getConnection(): Promise<mysql.PoolConnection> {
    return this.pool.getConnection();
  }

  async end(): Promise<void> {
    for (const listener of this.binlogListeners) {
      listener.stop();
    }

    await this.pool.end();
  }

  destroy() {
    for (const listener of this.binlogListeners) {
      listener.stop();
    }

    this.pool.destroy();
  }
}
