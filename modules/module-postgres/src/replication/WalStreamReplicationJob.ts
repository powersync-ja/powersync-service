import { container } from '@powersync/lib-services-framework';
import { PgManager } from './PgManager.js';
import { MissingReplicationSlotError, WalStream } from './WalStream.js';

import { replication } from '@powersync/service-core';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { cleanUpReplicationSlot } from './replication-utils.js';

export interface WalStreamReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: ConnectionManagerFactory;
  eventManager: replication.ReplicationEventManager;
}

export class WalStreamReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: ConnectionManagerFactory;
  private readonly connectionManager: PgManager;
  private readonly eventManager: replication.ReplicationEventManager;

  constructor(options: WalStreamReplicationJobOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    this.connectionManager = this.connectionFactory.create({
      // Pool connections are only used intermittently.
      idleTimeout: 30_000,
      maxSize: 2
    });
    this.eventManager = options.eventManager;
  }

  async cleanUp(): Promise<void> {
    const connectionManager = this.connectionFactory.create({
      idleTimeout: 30_000,
      maxSize: 1
    });
    try {
      await cleanUpReplicationSlot(this.slotName, connectionManager.pool);
    } finally {
      await connectionManager.end();
    }
  }

  /**
   * Postgres on RDS writes performs a WAL checkpoint every 5 minutes by default, which creates a new 64MB file.
   *
   * The old WAL files are only deleted once no replication slot still references it.
   *
   * Unfortunately, when there are no changes to the db, the database creates new WAL files without the replication slot
   * advancing**.
   *
   * As a workaround, we write a new message every couple of minutes, to make sure that the replication slot advances.
   *
   * **This may be a bug in pgwire or how we're using it.
   */
  async keepAlive() {
    try {
      await this.connectionManager.pool.query(`SELECT * FROM pg_logical_emit_message(false, 'powersync', 'ping')`);
    } catch (e) {
      this.logger.warn(`KeepAlive failed, unable to post to WAL`, e);
    }
  }

  get slotName() {
    return this.options.storage.slot_name;
  }

  async replicate() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {
          replication_slot: this.slotName
        }
      });
      this.logger.error(`Replication failed on ${this.slotName}`, e);

      if (e instanceof MissingReplicationSlotError) {
        // This stops replication on this slot, and creates a new slot
        await this.options.storage.factory.slotRemoved(this.slotName);
      }
    } finally {
      this.abortController.abort();
    }
  }

  async replicateLoop() {
    while (!this.isStopped) {
      await this.replicateOnce();

      if (!this.isStopped) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  async replicateOnce() {
    // New connections on every iteration (every error with retry),
    // otherwise we risk repeating errors related to the connection,
    // such as caused by cached PG schemas.
    const connectionManager = this.connectionFactory.create({
      // Pool connections are only used intermittently.
      idleTimeout: 30_000,
      maxSize: 2
    });
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new WalStream({
        abort_signal: this.abortController.signal,
        storage: this.options.storage,
        connections: connectionManager,
        event_manager: this.eventManager
      });
      await stream.replicate();
    } catch (e) {
      this.logger.error(`Replication error`, e);
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
        this.logger.error(`cause`, e.cause);
      }
      if (e instanceof MissingReplicationSlotError) {
        throw e;
      } else {
        // Report the error if relevant, before retrying
        container.reporter.captureException(e, {
          metadata: {
            replication_slot: this.slotName
          }
        });
        // This sets the retry delay
        this.rateLimiter?.reportError(e);
      }
    } finally {
      await connectionManager.end();
    }
  }
}
