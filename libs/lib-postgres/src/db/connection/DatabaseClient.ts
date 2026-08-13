import * as lib_postgres from '@powersync/lib-service-postgres';
import { DO_NOT_LOG } from '@powersync/lib-services-framework';
import * as pgwire from '@powersync/service-jpgwire';
import { AbstractPostgresConnection, sql } from './AbstractPostgresConnection.js';
import { ConnectionLease, ConnectionSlot, NotificationListener } from './ConnectionSlot.js';
import { WrappedConnection } from './WrappedConnection.js';

export type DatabaseClientOptions = {
  config: lib_postgres.NormalizedBasePostgresConnectionConfig;
  /**
   * Optional schema which will be used as the default search path
   */
  schema?: string;
  /**
   * Notification channels to listen to.
   */
  notificationChannels?: string[];

  applicationName: string;
};

export type DatabaseClientListener = NotificationListener & {
  connectionCreated?: (connection: pgwire.PgConnection) => Promise<void>;
};

export const TRANSACTION_CONNECTION_COUNT = 5;

/**
 * This provides access to Postgres via the PGWire library.
 * A connection pool is used for individual query executions while
 * a custom pool of connections is available for transactions or other operations
 * which require being executed on the same connection.
 */
export class DatabaseClient extends AbstractPostgresConnection<DatabaseClientListener> {
  [DO_NOT_LOG] = true;

  closed: boolean;

  pool: pgwire.PgClient;

  protected connections: ConnectionSlot[];

  protected initialized: Promise<void>;
  protected queue: PromiseWithResolvers<ConnectionLease>[];
  /** Latest exhausted connection attempt for each slot in the current attempt round. */
  protected failedConnectionSlots: Map<ConnectionSlot, unknown>;

  constructor(protected options: DatabaseClientOptions) {
    super();
    this.closed = false;
    this.pool = pgwire.connectPgWirePool(options.config, {
      maxSize: options.config.max_pool_size,
      applicationName: options.applicationName
    });
    this.failedConnectionSlots = new Map();
    this.connections = Array.from({ length: TRANSACTION_CONNECTION_COUNT }, (v, index) => {
      // Only listen to notifications on a single (the first) connection
      const notificationChannels = index == 0 ? options.notificationChannels : [];
      const slot = new ConnectionSlot({
        config: options.config,
        notificationChannels,
        applicationName: options.applicationName
      });
      slot.registerListener({
        connectionAvailable: () => this.handleConnectionAvailable(slot),
        connectionError: (ex) => this.handleConnectionError(slot, ex),
        connectionCreated: (connection) => this.iterateAsyncListeners(async (l) => l.connectionCreated?.(connection))
      });
      return slot;
    });
    this.queue = [];
    this.initialized = this.initialize();
  }

  protected get baseConnection() {
    return this.pool;
  }

  protected get schemaStatement() {
    const { schema } = this.options;
    if (!schema) {
      return;
    }
    return {
      statement: `SET search_path TO ${schema};`
    };
  }

  registerListener(listener: Partial<DatabaseClientListener>): () => void {
    let disposeNotification: (() => void) | null = null;
    if ('notificationEvent' in listener) {
      // Pass this on to the first connection slot
      // It will only actively listen on the connection once a listener has been registered
      disposeNotification = this.connections[0].registerListener({ notificationEvent: listener.notificationEvent });
      this.pokeSlots();

      delete listener.notificationEvent;
    }

    const superDispose = super.registerListener(listener);
    return () => {
      disposeNotification?.();
      superDispose();
    };
  }

  query(script: string, options?: pgwire.PgSimpleQueryOptions): Promise<pgwire.PgResult>;
  query(...args: pgwire.Statement[]): Promise<pgwire.PgResult>;
  async query(...args: any[]): Promise<pgwire.PgResult> {
    await this.initialized;
    /**
     * There is no direct way to set the default schema with pgwire.
     * This hack uses multiple statements in order to always ensure the
     * appropriate connection (in the pool) uses the correct schema.
     */
    const { schemaStatement } = this;
    if (typeof args[0] == 'object' && schemaStatement) {
      args.unshift(schemaStatement);
    } else if (typeof args[0] == 'string' && schemaStatement) {
      args[0] = `${schemaStatement.statement}; ${args[0]}`;
    }

    // Retry pool queries. Note that we can't retry queries in a transaction
    // since a failed query will end the transaction.
    return lib_postgres.retriedQuery(this.pool, ...args);
  }

  async *stream(...args: pgwire.Statement[]): AsyncIterableIterator<pgwire.PgChunk> {
    await this.initialized;
    const { schemaStatement } = this;
    if (schemaStatement) {
      args.unshift(schemaStatement);
    }
    yield* super.stream(...args);
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
    const { schemaStatement } = this;
    if (!schemaStatement) {
      return;
    }
    await client.query(schemaStatement);
  }

  protected async initialize() {
    const { schema } = this.options;
    if (schema) {
      // First check if it exists
      const exists = await this.pool.query(sql`
        SELECT
          schema_name
        FROM
          information_schema.schemata
        WHERE
          schema_name = ${{ type: 'varchar', value: schema }};
      `);

      if (exists.rows.length) {
        return;
      }
      // Create the schema if it doesn't exist
      await this.pool.query({ statement: `CREATE SCHEMA IF NOT EXISTS ${this.options.schema}` });
    }
  }

  protected async requestConnection(): Promise<ConnectionLease> {
    if (this.closed) {
      throw new Error('Database client is closed');
    }

    await this.initialized;

    // Queue the operation
    const deferred = Promise.withResolvers<ConnectionLease>();
    this.queue.push(deferred);

    // Try already-connected slots first. poke() only creates missing
    // connections and does not emit another availability event for an existing
    // connection, so skipping this could leave the request queued indefinitely.
    this.processConnectionQueue();
    this.pokeSlots();

    return deferred.promise;
  }

  protected pokeSlots() {
    // Ensure the slots have connections and notify any queued requests.
    for (const slot of this.connections) {
      if (!slot.isConnected && !slot.isPoking) {
        // This slot is starting a fresh attempt, so a failure from an earlier
        // attempt must not count against the current request.
        this.failedConnectionSlots.delete(slot);
      }
      // No need to await this. Errors are reported asynchronously
      slot.poke();
    }
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

  protected handleConnectionAvailable(slot: ConnectionSlot) {
    // This slot recovered, so its earlier failure must no longer contribute to
    // deciding whether the shared lease queue is unreachable.
    this.failedConnectionSlots.delete(slot);
    this.processConnectionQueue();
  }

  /**
   * Lease requests wait in one shared queue and are not assigned to a slot
   * until that slot becomes available. A failure from one slot therefore
   * cannot fail a particular request: another slot may still serve it.
   */
  protected handleConnectionError(slot: ConnectionSlot, exception: any) {
    this.failedConnectionSlots.set(slot, exception);
    if (this.failedConnectionSlots.size < this.connections.length) {
      return;
    }

    // Every slot has exhausted its attempts, so no slot can currently make
    // progress on the shared queue.
    for (const q of this.queue) {
      q.reject(exception);
    }
    this.queue = [];
  }

  async [Symbol.asyncDispose]() {
    try {
      await this.initialized;
    } catch (e) {
      // Error was already reported when initializing - ignore here.
      // If if throw this, we typically get a SuppressedError, which is difficult to debug.
    }
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
