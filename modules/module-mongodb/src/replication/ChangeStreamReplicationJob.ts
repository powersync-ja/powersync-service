import { container } from '@powersync/lib-services-framework';
import { MongoManager } from './MongoManager.js';
import { MissingReplicationSlotError, ChangeStream } from './ChangeStream.js';

import { replication } from '@powersync/service-core';
import { ConnectionManagerFactory } from './ConnectionManagerFactory.js';

export interface ChangeStreamReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: ConnectionManagerFactory;
}

export class ChangeStreamReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: ConnectionManagerFactory;
  private readonly connectionManager: MongoManager;

  constructor(options: ChangeStreamReplicationJobOptions) {
    super(options);
    this.connectionFactory = options.connectionFactory;
    this.connectionManager = this.connectionFactory.create();
  }

  async cleanUp(): Promise<void> {
    // TODO: Implement?
  }

  async keepAlive() {
    // TODO: Implement?
  }

  async replicate() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {}
      });
      this.logger.error(`Replication failed`, e);
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
    const connectionManager = this.connectionFactory.create();
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new ChangeStream({
        abort_signal: this.abortController.signal,
        storage: this.options.storage,
        connections: connectionManager
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
          metadata: {}
        });
        // This sets the retry delay
        this.rateLimiter?.reportError(e);
      }
    } finally {
      await connectionManager.end();
    }
  }
}
