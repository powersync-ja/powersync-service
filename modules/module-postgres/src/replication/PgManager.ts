import * as pgwire from '@powersync/service-jpgwire';
import { NormalizedPostgresConnectionConfig } from '../types/types.js';

/**
 * Shorter timeout for snapshot connections than for replication connections.
 */
const SNAPSHOT_SOCKET_TIMEOUT = 30_000;

export class PgManager {
  /**
   * Do not use this for any transactions.
   */
  public readonly pool: pgwire.PgClient;

  private connectionPromises: Promise<pgwire.PgConnection>[] = [];

  constructor(
    public options: NormalizedPostgresConnectionConfig,
    public poolOptions: pgwire.PgPoolOptions
  ) {
    // The pool is lazy - no connections are opened until a query is performed.
    this.pool = pgwire.connectPgWirePool(this.options, poolOptions);
  }

  public get connectionTag() {
    return this.options.tag;
  }

  /**
   * Create a new replication connection.
   */
  async replicationConnection(): Promise<pgwire.PgConnection> {
    const p = pgwire.connectPgWire(this.options, { type: 'replication' });
    this.connectionPromises.push(p);
    return await p;
  }

  /**
   * Create a new standard connection, used for initial snapshot.
   *
   * This connection must not be shared between multiple async contexts.
   */
  async snapshotConnection(): Promise<pgwire.PgConnection> {
    const p = pgwire.connectPgWire(this.options, { type: 'standard' });
    this.connectionPromises.push(p);
    const connection = await p;

    // Use an shorter timeout for snapshot connections.
    // This is to detect broken connections early, instead of waiting
    // for the full 6 minutes.
    // This we are constantly using the connection, we don't need any
    // custom keepalives.
    (connection as any)._socket.setTimeout(SNAPSHOT_SOCKET_TIMEOUT);

    // Disable statement timeout for snapshot queries.
    // On Supabase, the default is 2 minutes.
    await connection.query(`set session statement_timeout = 0`);

    return connection;
  }

  async end(): Promise<void> {
    for (let result of await Promise.allSettled([
      this.pool.end(),
      ...this.connectionPromises.map(async (promise) => {
        const connection = await promise;
        return await connection.end();
      })
    ])) {
      // Throw the first error, if any
      if (result.status == 'rejected') {
        throw result.reason;
      }
    }
  }

  async destroy() {
    this.pool.destroy();
    for (let result of await Promise.allSettled([
      ...this.connectionPromises.map(async (promise) => {
        const connection = await promise;
        return connection.destroy();
      })
    ])) {
      // Throw the first error, if any
      if (result.status == 'rejected') {
        throw result.reason;
      }
    }
  }
}
