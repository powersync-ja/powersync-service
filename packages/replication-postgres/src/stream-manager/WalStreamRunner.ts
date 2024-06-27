import * as pgwire from '@powersync/service-jpgwire';
import { container, logger } from '@powersync/lib-services-framework';
import { replication, utils } from '@powersync/service-core';

import { MissingReplicationSlotError, WalStream } from './WalStream.js';
import { PgManager } from '../utils/PgManager.js';

export class WalStreamRunner extends replication.AbstractStreamRunner<utils.ResolvedConnection> {
  private connections: PgManager | null = null;

  constructor(options: replication.StreamRunnerOptions<utils.ResolvedConnection>) {
    super(options);
  }

  protected async handleReplicationLoopError(ex: any) {
    if (ex instanceof MissingReplicationSlotError) {
      // This stops replication on this slot, and creates a new slot
      await this.options.storage.factory.slotRemoved(this.slot_name);
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
  protected async _ping() {
    if (!this.connections) {
      return;
    }
    try {
      await this.connections.pool.query(`SELECT * FROM pg_logical_emit_message(false, 'powersync', 'ping')`);
    } catch (e) {
      logger.warn(`Failed to ping`, e);
    }
  }
  protected async _forceStop(): Promise<void> {
    await this.connections?.destroy();
  }

  protected async _terminate(options?: { force?: boolean | undefined } | undefined): Promise<void> {
    const slotName = this.slot_name;
    const db = await pgwire.connectPgWire(this.options.config, { type: 'standard' });
    try {
      await db.query({
        statement: 'SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = $1',
        params: [{ type: 'varchar', value: slotName }]
      });
    } finally {
      await db.end();
    }
  }

  async replicateOnce() {
    // New connections on every iteration (every error with retry),
    // otherwise we risk repeating errors related to the connection,
    // such as caused by cached PG schemas.
    let connections = new PgManager(this.options.config, {
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
        factory: this.options.storage_factory,
        storage: this.options.storage,
        connections
      });
      await stream.replicate();
    } catch (e) {
      logger.error(`Replication error`, e);
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
        logger.error(`cause`, e.cause);
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
        this.reportError(e);
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
    logger.info(`${this.slot_name} Stopping replication`);
    // End gracefully
    this.abortController.abort();

    if (options?.force) {
      // destroy() is more forceful.
      await this.connections?.destroy();
    }
    await this.runPromise;
  }

  protected reportError(e: any): void {
    const message = (e.message as string) ?? '';
    if (message.includes('password authentication failed')) {
      // Wait 15 minutes, to avoid triggering Supabase's fail2ban
      this.rateLimiter.reportErrorType(replication.ErrorType.AUTH);
    } else if (message.includes('ENOTFOUND')) {
      // DNS lookup issue - incorrect URI or deleted instance
      this.rateLimiter.reportErrorType(replication.ErrorType.NOT_FOUND);
    } else if (message.includes('ECONNREFUSED')) {
      // Could be fail2ban or similar
      this.rateLimiter.reportErrorType(replication.ErrorType.CONNECTION_REFUSED);
    } else if (
      message.includes('Unable to do postgres query on ended pool') ||
      message.includes('Postgres unexpectedly closed connection')
    ) {
      // Connection timed out - ignore / immediately retry
      // We don't explicitly set the delay to 0, since there could have been another error that
      // we need to respect.
    } else {
      // General error with standard delay
      this.rateLimiter.reportErrorType();
    }
  }
}
