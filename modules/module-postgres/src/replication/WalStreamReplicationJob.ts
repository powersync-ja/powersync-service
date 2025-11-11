import { container, logger, ReplicationAbortedError } from '@powersync/lib-services-framework';
import { PgManager } from './PgManager.js';
import { MissingReplicationSlotError, sendKeepAlive, WalStream } from './WalStream.js';

import { replication } from '@powersync/service-core';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';
import { getApplicationName } from '../utils/application-name.js';

export interface WalStreamReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class WalStreamReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: ConnectionManagerFactory;
  private connectionManager: PgManager | null = null;
  private lastStream: WalStream | null = null;

  constructor(options: WalStreamReplicationJobOptions) {
    super(options);
    this.logger = logger.child({ prefix: `[${this.slotName}] ` });
    this.connectionFactory = options.connectionFactory;
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
    if (this.connectionManager) {
      try {
        await sendKeepAlive(this.connectionManager.pool);
      } catch (e) {
        this.logger.warn(`KeepAlive failed, unable to post to WAL`, e);
      }
    }
  }

  get slotName() {
    return this.options.storage.slot_name;
  }

  async replicate() {
    try {
      await this.replicateOnce();
    } catch (e) {
      // Fatal exception

      if (!this.isStopped) {
        // Ignore aborted errors

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
        // Report the error if relevant, before retrying
        container.reporter.captureException(e, {
          metadata: {
            replication_slot: this.slotName
          }
        });
        // This sets the retry delay
        this.rateLimiter?.reportError(e);
      }

      if (e instanceof MissingReplicationSlotError) {
        // This stops replication on this slot and restarts with a new slot
        await this.options.storage.factory.restartReplication(this.storage.group_id);
      }

      // No need to rethrow - the error is already logged, and retry behavior is the same on error
    } finally {
      this.abortController.abort();
    }
  }

  async replicateOnce() {
    // New connections on every iteration (every error with retry),
    // otherwise we risk repeating errors related to the connection,
    // such as caused by cached PG schemas.
    const connectionManager = this.connectionFactory.create({
      // Pool connections are only used intermittently.
      idleTimeout: 30_000,
      maxSize: 2,
      applicationName: getApplicationName()
    });
    this.connectionManager = connectionManager;
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new WalStream({
        logger: this.logger,
        abort_signal: this.abortController.signal,
        storage: this.options.storage,
        metrics: this.options.metrics,
        connections: connectionManager
      });
      this.lastStream = stream;
      await stream.replicate();
    } finally {
      this.connectionManager = null;
      await connectionManager.end();
    }
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    return this.lastStream?.getReplicationLagMillis();
  }
}
