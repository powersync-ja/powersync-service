import * as pgwire from '@powersync/service-jpgwire';

import { replication } from '@powersync/service-core';
import { NormalizedPostgresConnectionConfig } from '../types/types.js';

export type PostgresConnection = pgwire.PgClient;

export class PostgresConnectionManager implements replication.ConnectionManager<PostgresConnection> {
  /**
   * Do not use this for any transactions.
   */
  public readonly pool: pgwire.PgClient;

  private connectionPromises: Promise<pgwire.PgConnection>[] = [];

  constructor(public options: NormalizedPostgresConnectionConfig) {
    // The pool is lazy - no connections are opened until a query is performed.
    this.pool = pgwire.connectPgWirePool(this.options, {});
  }

  /**
   * Create a new replication connection.
   */
  async createReplicationConnection(): Promise<pgwire.PgConnection> {
    const p = pgwire.connectPgWire(this.options, { type: 'replication' });
    this.connectionPromises.push(p);
    return await p;
  }

  /**
   * Create a new standard connection, used for initial snapshot.
   *
   * This connection must not be shared between multiple async contexts.
   */
  async createConnection(): Promise<pgwire.PgConnection> {
    const p = pgwire.connectPgWire(this.options, { type: 'standard' });
    this.connectionPromises.push(p);
    return await p;
  }

  mapError(error: Error): replication.ConnectionError {
    throw new Error('Method not implemented.');
  }

  // TODO need some way to end connections
  //  The generics don't cater for this
  //  Can't register an automatic termination callback without service context

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
