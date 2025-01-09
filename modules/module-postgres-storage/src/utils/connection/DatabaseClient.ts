import * as pgwire from '@powersync/service-jpgwire';
import { pg_utils } from '@powersync/service-module-postgres';
import * as pg_types from '@powersync/service-module-postgres/types';
import pDefer, { DeferredPromise } from 'p-defer';
import { AbstractPostgresConnection, sql } from './AbstractPostgresConnection.js';
import { ConnectionLease, ConnectionSlot, NotificationListener } from './ConnectionSlot.js';
import { WrappedConnection } from './WrappedConnection.js';

export const TRANSACTION_CONNECTION_COUNT = 5;

export const STORAGE_SCHEMA_NAME = 'powersync';

const SCHEMA_STATEMENT: pgwire.Statement = {
  statement: `SET search_path TO ${STORAGE_SCHEMA_NAME};`
};

export class DatabaseClient extends AbstractPostgresConnection<NotificationListener> {
  closed: boolean;

  protected pool: pgwire.PgClient;
  protected connections: ConnectionSlot[];

  protected initialized: Promise<void>;
  protected queue: DeferredPromise<ConnectionLease>[];

  constructor(protected config: pg_types.NormalizedPostgresConnectionConfig) {
    super();
    this.closed = false;
    this.pool = pgwire.connectPgWirePool(this.config, {});
    this.connections = Array.from({ length: TRANSACTION_CONNECTION_COUNT }, () => {
      const slot = new ConnectionSlot(config);
      slot.registerListener({
        connectionAvailable: () => this.processConnectionQueue(),
        connectionError: (ex) => this.handleConnectionError(ex)
      });
      return slot;
    });
    this.queue = [];
    this.initialized = this.initialize();
  }

  protected get baseConnection() {
    return this.pool;
  }

  registerListener(listener: Partial<NotificationListener>): () => void {
    let disposeNotification: (() => void) | null = null;
    if ('notification' in listener) {
      // Pass this on to the first connection slot
      // It will only actively listen on the connection once a listener has been registered
      disposeNotification = this.connections[0].registerListener({
        notification: listener.notification
      });
      delete listener['notification'];
    }

    const superDispose = super.registerListener(listener);
    return () => {
      disposeNotification?.();
      superDispose();
    };
  }

  /**
   * There is no direct way to set the default schema with pgwire.
   * This hack uses multiple statements in order to always ensure the
   * appropriate connection uses the correct schema.
   */
  async query(...args: pgwire.Statement[]): Promise<pgwire.PgResult> {
    await this.initialized;
    // Retry pool queries. Note that we can't retry queries in a transaction
    // since a failed query will end the transaction.
    return pg_utils.retriedQuery(this.pool, ...[SCHEMA_STATEMENT, ...args]);
  }

  async *stream(...args: pgwire.Statement[]): AsyncIterableIterator<pgwire.PgChunk> {
    await this.initialized;
    yield* super.stream(...[SCHEMA_STATEMENT, ...args]);
  }

  async lockConnection<T>(callback: (db: WrappedConnection) => Promise<T>): Promise<T> {
    const { connection, release } = await this.requestConnection();

    await this.setSchema(connection);

    try {
      return await callback(new WrappedConnection(connection));
    } finally {
      release();
    }
  }

  async transaction<T>(tx: (db: WrappedConnection) => Promise<T>): Promise<T> {
    return this.lockConnection(async (db) => {
      try {
        await db.query(sql`BEGIN`);
        const result = await tx(db);
        await db.query(sql`COMMIT`);
        return result;
      } catch (ex) {
        await db.query(sql`ROLLBACK`);
        throw ex;
      }
    });
  }

  /**
   * Use the `powersync` schema as the default when resolving table names
   */
  protected async setSchema(client: pgwire.PgClient) {
    await client.query(SCHEMA_STATEMENT);
  }

  protected async initialize() {
    // Create the schema if it doesn't exist
    await this.pool.query({ statement: `CREATE SCHEMA IF NOT EXISTS ${STORAGE_SCHEMA_NAME}` });
  }

  protected async requestConnection(): Promise<ConnectionLease> {
    if (this.closed) {
      throw new Error('Database client is closed');
    }

    await this.initialized;

    // Queue the operation
    const deferred = pDefer<ConnectionLease>();
    this.queue.push(deferred);

    // Poke the slots to check if they are alive
    for (const slot of this.connections) {
      // No need to await this. Errors are reported asynchronously
      slot.poke();
    }

    return deferred.promise;
  }

  protected leaseConnectionSlot(): ConnectionLease | null {
    const availableSlots = this.connections.filter((s) => s.isAvailable);
    for (const slot of availableSlots) {
      const lease = slot.lock();
      if (lease) {
        return lease;
      }
      // Possibly some contention detected, keep trying
    }
    return null;
  }

  protected processConnectionQueue() {
    if (this.closed && this.queue.length) {
      for (const q of this.queue) {
        q.reject(new Error('Database has closed while waiting for a connection'));
      }
      this.queue = [];
    }

    if (this.queue.length) {
      const lease = this.leaseConnectionSlot();
      if (lease) {
        const deferred = this.queue.shift()!;
        deferred.resolve(lease);
      }
    }
  }

  /**
   * Reports connection errors which might occur from bad configuration or
   * a server which is no longer available.
   * This fails all pending requests.
   */
  protected handleConnectionError(exception: any) {
    for (const q of this.queue) {
      q.reject(exception);
    }
    this.queue = [];
  }

  async [Symbol.asyncDispose]() {
    await this.initialized;
    this.closed = true;

    for (const c of this.connections) {
      await c[Symbol.asyncDispose]();
    }

    await this.pool.end();

    // Reject all remaining items
    for (const q of this.queue) {
      q.reject(new Error(`Database is disposed`));
    }
    this.queue = [];
  }
}
