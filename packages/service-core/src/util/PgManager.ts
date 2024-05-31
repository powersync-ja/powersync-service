import * as pgwire from '@powersync/service-jpgwire';
import { NormalizedPostgresConnection } from '@powersync/service-types';

export class PgManager {
  /**
   * Do not use this for any transactions.
   */
  public readonly pool: pgwire.PgClient;

  private connectionPromises: Promise<pgwire.PgConnection>[] = [];

  constructor(public options: NormalizedPostgresConnection, public poolOptions: pgwire.PgPoolOptions) {
    // The pool is lazy - no connections are opened until a query is performed.
    this.pool = pgwire.connectPgWirePool(this.options, poolOptions);
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
    return await p;
  }

  async end() {
    for (let result of await Promise.allSettled([
      this.pool.end(),
      ...this.connectionPromises.map((promise) => {
        return promise.then((connection) => connection.end());
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
      ...this.connectionPromises.map((promise) => {
        return promise.then((connection) => connection.destroy());
      })
    ])) {
      // Throw the first error, if any
      if (result.status == 'rejected') {
        throw result.reason;
      }
    }
  }
}
