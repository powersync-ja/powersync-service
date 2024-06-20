import * as pgwire from '@powersync/service-jpgwire';

import * as storage from '../storage/storage-index.js';
import * as util from '../util/util-index.js';

import { ErrorRateLimiter } from './ErrorRateLimiter.js';
import { MissingReplicationSlotError, WalStream } from './WalStream.js';
import { ResolvedConnection } from '../util/config/types.js';
import { container } from '@powersync/service-framework';

export interface WalStreamRunnerOptions {
  factory: storage.BucketStorageFactory;
  storage: storage.SyncRulesBucketStorage;
  source_db: ResolvedConnection;
  lock: storage.ReplicationLock;
  rateLimiter?: ErrorRateLimiter;
}

export class WalStreamRunner {
  private abortController = new AbortController();

  private runPromise?: Promise<void>;

  private connections: util.PgManager | null = null;

  private rateLimiter?: ErrorRateLimiter;

  constructor(public options: WalStreamRunnerOptions) {
    this.rateLimiter = options.rateLimiter;
  }

  start() {
    this.runPromise = this.run();
  }

  get slot_name() {
    return this.options.storage.slot_name;
  }

  get stopped() {
    return this.abortController.signal.aborted;
  }

  async run() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {
          replication_slot: this.slot_name
        }
      });
      container.logger.error(`Replication failed on ${this.slot_name}`, e);

      if (e instanceof MissingReplicationSlotError) {
        // This stops replication on this slot, and creates a new slot
        await this.options.storage.factory.slotRemoved(this.slot_name);
      }
    } finally {
      this.abortController.abort();
    }
    await this.options.lock.release();
  }

  async replicateLoop() {
    while (!this.stopped) {
      await this.replicateOnce();

      if (!this.stopped) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  async replicateOnce() {
    // New connections on every iteration (every error with retry),
    // otherwise we risk repeating errors related to the connection,
    // such as caused by cached PG schemas.
    let connections = new util.PgManager(this.options.source_db, {
      // Pool connections are only used intermittently.
      idleTimeout: 30_000,
      maxSize: 2
    });
    this.connections = connections;
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.stopped) {
        return;
      }
      const stream = new WalStream({
        abort_signal: this.abortController.signal,
        factory: this.options.factory,
        storage: this.options.storage,
        connections
      });
      await stream.replicate();
    } catch (e) {
      container.logger.error(`Replication error`, e);
      if (e.cause != null) {
        // Example:
        // PgError.conn_ended: Unable to do postgres query on ended connection
        //     at PgConnection.stream (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:315:13)
        //     at stream.next (<anonymous>)
        //     at PgResult.fromStream (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:1174:22)
        //     at PgConnection.query (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:311:21)
        //     at WalStream.startInitialReplication (file:///.../powersync/powersync-service/lib/replication/WalStream.js:266:22)
        //     ...
        //   cause: TypeError: match is not iterable
        //       at timestamptzToSqlite (file:///.../powersync/packages/jpgwire/dist/util.js:140:50)
        //       at PgType.decode (file:///.../powersync/packages/jpgwire/dist/pgwire_types.js:25:24)
        //       at PgConnection._recvDataRow (file:///.../powersync/packages/jpgwire/dist/util.js:88:22)
        //       at PgConnection._recvMessages (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:656:30)
        //       at PgConnection._ioloopAttempt (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:563:20)
        //       at process.processTicksAndRejections (node:internal/process/task_queues:95:5)
        //       at async PgConnection._ioloop (file:///.../powersync/node_modules/.pnpm/github.com+kagis+pgwire@f1cb95f9a0f42a612bb5a6b67bb2eb793fc5fc87/node_modules/pgwire/mod.js:517:14),
        //   [Symbol(pg.ErrorCode)]: 'conn_ended',
        //   [Symbol(pg.ErrorResponse)]: undefined
        // }
        // Without this additional log, the cause would not be visible in the logs.
        container.logger.error(`cause`, e.cause);
      }
      if (e instanceof MissingReplicationSlotError) {
        throw e;
      } else {
        // Report the error if relevant, before retrying
        container.reporter.captureException(e, {
          metadata: {
            replication_slot: this.slot_name
          }
        });
        // This sets the retry delay
        this.rateLimiter?.reportError(e);
      }
    } finally {
      this.connections = null;
      if (connections != null) {
        await connections.end();
      }
    }
  }

  /**
   * This will also release the lock if start() was called earlier.
   */
  async stop(options?: { force?: boolean }) {
    container.logger.info(`${this.slot_name} Stopping replication`);
    // End gracefully
    this.abortController.abort();

    if (options?.force) {
      // destroy() is more forceful.
      await this.connections?.destroy();
    }
    await this.runPromise;
  }

  /**
   * Terminate this replication stream. This drops the replication slot and deletes the replication data.
   *
   * Stops replication if needed.
   */
  async terminate(options?: { force?: boolean }) {
    container.logger.info(`${this.slot_name} Terminating replication`);
    await this.stop(options);

    const slotName = this.slot_name;
    const db = await pgwire.connectPgWire(this.options.source_db, { type: 'standard' });
    try {
      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });
    } finally {
      await db.end();
    }

    await this.options.storage.terminate();
  }
}
