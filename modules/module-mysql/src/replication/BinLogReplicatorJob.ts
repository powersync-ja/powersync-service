import { container } from '@powersync/lib-services-framework';
import * as types from '../types/types.js';

import * as mysql_utils from '../utils/mysql_utils.js';

import { replication } from '@powersync/service-core';
import { MysqlBinLogStream } from './MysqlBinLogStream.js';

export interface BinLogReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  /**
   * Connection config required to create a MySQL Pool
   */
  connectionConfig: types.ResolvedConnectionConfig;
}

export class BinLogReplicatorJob extends replication.AbstractReplicationJob {
  protected connectionConfig: types.ResolvedConnectionConfig;

  constructor(options: BinLogReplicationJobOptions) {
    super(options);
    this.connectionConfig = options.connectionConfig;
  }

  get slot_name() {
    return this.options.storage.slot_name;
  }

  async cleanUp(): Promise<void> {
    // This MySQL module does not create anything which requires cleanup on the MySQL server.
  }

  async keepAlive() {}

  async replicate() {
    try {
      await this.replicateLoop();
    } catch (e) {
      // Fatal exception
      container.reporter.captureException(e, {
        metadata: {
          replication_slot: this.slot_name
        }
      });
      this.logger.error(`Replication failed on ${this.slot_name}`, e);

      //   Slot removal type error logic goes here
      //   if (e) {
      //     // This stops replication on this slot, and creates a new slot
      //     await this.options.storage.factory.slotRemoved(this.slot_name);
      //   }
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
    const pool = mysql_utils.createPool(this.connectionConfig);
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new MysqlBinLogStream({
        abort_signal: this.abortController.signal,
        connection_config: this.connectionConfig,
        pool,
        storage: this.options.storage
      });
      await stream.replicate();
    } catch (e) {
      this.logger.error(`Replication error`, e);
      if (e.cause != null) {
        this.logger.error(`cause`, e.cause);
      }
      // TODO not recoverable error
      if (false) {
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
      await pool.end();
    }
  }
}
