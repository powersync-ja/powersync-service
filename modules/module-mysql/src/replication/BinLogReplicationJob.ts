import { container, logger as defaultLogger } from '@powersync/lib-services-framework';
import { POWERSYNC_VERSION, replication } from '@powersync/service-core';
import { BinlogConfigurationError, BinLogStream } from './BinLogStream.js';
import { MySQLConnectionManagerFactory } from './MySQLConnectionManagerFactory.js';
import { MySQLConnectionManager } from './MySQLConnectionManager.js';

export interface BinLogReplicationJobOptions extends replication.AbstractReplicationJobOptions {
  connectionFactory: MySQLConnectionManagerFactory;
}

export class BinLogReplicationJob extends replication.AbstractReplicationJob {
  private connectionFactory: MySQLConnectionManagerFactory;
  private readonly connectionManager: MySQLConnectionManager;
  private lastStream: BinLogStream | null = null;

  constructor(options: BinLogReplicationJobOptions) {
    super(options);
    this.logger = defaultLogger.child({ prefix: `[powersync_${this.options.storage.group_id}] ` });
    this.connectionFactory = options.connectionFactory;
    this.connectionManager = this.connectionFactory.create({
      // Pool connections are only used intermittently.
      idleTimeout: 30_000,
      connectionLimit: 2,
      connectAttributes: {
        program_name: 'powersync',
        program_version: POWERSYNC_VERSION
      }
    });
  }

  get slot_name() {
    return this.options.storage.slot_name;
  }

  async keepAlive() {
    // Keepalives are handled by the binlog heartbeat mechanism
  }

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
    const connectionManager = this.connectionFactory.create({
      // Pool connections are only used intermittently.
      idleTimeout: 30_000,
      connectionLimit: 2,

      connectAttributes: {
        // https://dev.mysql.com/doc/refman/8.0/en/performance-schema-connection-attribute-tables.html
        // These do not appear to be supported by Zongji yet, so we only specify it here.
        // Query using `select * from performance_schema.session_connect_attrs`.
        program_name: 'powersync',
        program_version: POWERSYNC_VERSION

        // _client_name and _client_version is specified by the driver
      }
    });
    try {
      await this.rateLimiter?.waitUntilAllowed({ signal: this.abortController.signal });
      if (this.isStopped) {
        return;
      }
      const stream = new BinLogStream({
        logger: this.logger,
        abortSignal: this.abortController.signal,
        storage: this.options.storage,
        metrics: this.options.metrics,
        connections: connectionManager
      });
      this.lastStream = stream;
      await stream.replicate();
    } catch (e) {
      if (this.abortController.signal.aborted) {
        return;
      }
      this.logger.error(`Replication error`, e);
      if (e.cause != null) {
        this.logger.error(`cause`, e.cause);
      }

      if (e instanceof BinlogConfigurationError) {
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
      await connectionManager.end();
    }
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    return this.lastStream?.getReplicationLagMillis();
  }
}
