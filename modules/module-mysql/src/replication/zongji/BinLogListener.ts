import * as common from '../../common/common-index.js';
import async from 'async';
import { BinLogEvent, StartOptions, TableMapEntry, ZongJi } from '@powersync/mysql-zongji';
import * as zongji_utils from './zongji-utils.js';
import { logger } from '@powersync/lib-services-framework';
import { MySQLConnectionManager } from '../MySQLConnectionManager.js';

export type Row = Record<string, any>;

export interface BinLogEventHandler {
  onWrite: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onUpdate: (rowsAfter: Row[], rowsBefore: Row[], tableMap: TableMapEntry) => Promise<void>;
  onDelete: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onCommit: (lsn: string) => Promise<void>;
}

export interface BinLogListenerOptions {
  connectionManager: MySQLConnectionManager;
  eventHandler: BinLogEventHandler;
  includedTables: string[];
  serverId: number;
  startPosition: common.BinLogPosition;
}

/**
 *  Wrapper class for the Zongji BinLog listener. Internally handles the creation and management of the listener and posts
 *  events on the provided BinLogEventHandler.
 */
export class BinLogListener {
  private connectionManager: MySQLConnectionManager;
  private eventHandler: BinLogEventHandler;
  private binLogPosition: common.BinLogPosition;
  private currentGTID: common.ReplicatedGTID | null;

  zongji: ZongJi;
  processingQueue: async.QueueObject<BinLogEvent>;
  /**
   *  The combined size in bytes of all the binlog events currently in the processing queue.
   */
  queueMemoryUsage: number;

  constructor(public options: BinLogListenerOptions) {
    this.connectionManager = options.connectionManager;
    this.eventHandler = options.eventHandler;
    this.binLogPosition = options.startPosition;
    this.currentGTID = null;

    this.processingQueue = async.queue(this.createQueueWorker(), 1);
    this.queueMemoryUsage = 0;
    this.zongji = this.createZongjiListener();
  }

  /**
   *  The queue memory limit in bytes as defined in the connection options.
   *  @private
   */
  private get queueMemoryLimit(): number {
    return this.connectionManager.options.binlog_queue_memory_limit * 1024 * 1024;
  }

  public async start(): Promise<void> {
    if (this.isStopped) {
      return;
    }
    logger.info(`Starting replication. Created replica client with serverId:${this.options.serverId}`);

    this.zongji.start({
      // We ignore the unknown/heartbeat event since it currently serves no purpose other than to keep the connection alive
      // tablemap events always need to be included for the other row events to work
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'xid', 'rotate', 'gtidlog'],
      includeSchema: { [this.connectionManager.databaseName]: this.options.includedTables },
      filename: this.binLogPosition.filename,
      position: this.binLogPosition.offset,
      serverId: this.options.serverId
    } satisfies StartOptions);

    return new Promise<void>((resolve, reject) => {
      // Handle an edge case where the listener has already been stopped before completing startup
      if (this.isStopped) {
        logger.info('BinLog listener was stopped before startup completed.');
        resolve();
      }

      this.zongji.on('error', (error) => {
        if (!this.isStopped) {
          logger.error('Binlog listener error:', error);
          this.stop();
          reject(error);
        } else {
          logger.warn('Binlog listener error during shutdown:', error);
        }
      });

      this.processingQueue.error((error) => {
        if (!this.isStopped) {
          logger.error('BinlogEvent processing error:', error);
          this.stop();
          reject(error);
        } else {
          logger.warn('BinlogEvent processing error during shutdown:', error);
        }
      });

      this.zongji.on('stopped', () => {
        resolve();
        logger.info('BinLog listener stopped. Replication ended.');
      });
    });
  }

  public stop(): void {
    if (!this.isStopped) {
      this.zongji.stop();
      this.processingQueue.kill();
    }
  }

  private get isStopped(): boolean {
    return this.zongji.stopped;
  }

  private createZongjiListener(): ZongJi {
    const zongji = this.connectionManager.createBinlogListener();

    zongji.on('binlog', async (evt) => {
      logger.info(`Received Binlog event:${evt.getEventName()}`);
      this.processingQueue.push(evt);
      this.queueMemoryUsage += evt.size;

      // When the processing queue grows past the threshold, we pause the binlog listener
      if (this.isQueueOverCapacity()) {
        logger.info(
          `Binlog processing queue has reached its memory limit of [${this.connectionManager.options.binlog_queue_memory_limit}MB]. Pausing Binlog listener.`
        );
        zongji.pause();
        await this.processingQueue.empty();
        logger.info(`Binlog processing queue backlog cleared. Resuming Binlog listener.`);
        zongji.resume();
      }
    });

    zongji.on('ready', async () => {
      // Set a heartbeat interval for the Zongji replication connection
      // Zongji does not explicitly handle the heartbeat events - they are categorized as event:unknown
      // The heartbeat events are enough to keep the connection alive for setTimeout to work on the socket.
      await new Promise((resolve, reject) => {
        this.zongji.connection.query(
          // In nanoseconds, 10^9 = 1s
          'set @master_heartbeat_period=28*1000000000',
          function (error: any, results: any, fields: any) {
            if (error) {
              reject(error);
            } else {
              logger.info('Successfully set up replication connection heartbeat...');
              resolve(results);
            }
          }
        );
      });

      // The _socket member is only set after a query is run on the connection, so we set the timeout after setting the heartbeat.
      // The timeout here must be greater than the master_heartbeat_period.
      const socket = this.zongji.connection._socket!;
      socket.setTimeout(60_000, () => {
        socket.destroy(new Error('Replication connection timeout.'));
      });
      logger.info(
        `BinLog listener setup complete. Reading binlog from: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
      );
    });

    return zongji;
  }

  private createQueueWorker() {
    return async (evt: BinLogEvent) => {
      switch (true) {
        case zongji_utils.eventIsGTIDLog(evt):
          this.currentGTID = common.ReplicatedGTID.fromBinLogEvent({
            raw_gtid: {
              server_id: evt.serverId,
              transaction_range: evt.transactionRange
            },
            position: {
              filename: this.binLogPosition.filename,
              offset: evt.nextPosition
            }
          });
          break;
        case zongji_utils.eventIsRotation(evt):
          this.binLogPosition.filename = evt.binlogName;
          this.binLogPosition.offset = evt.position;
          break;
        case zongji_utils.eventIsWriteMutation(evt):
          await this.eventHandler.onWrite(evt.rows, evt.tableMap[evt.tableId]);
          break;
        case zongji_utils.eventIsUpdateMutation(evt):
          await this.eventHandler.onUpdate(
            evt.rows.map((row) => row.after),
            evt.rows.map((row) => row.before),
            evt.tableMap[evt.tableId]
          );
          break;
        case zongji_utils.eventIsDeleteMutation(evt):
          await this.eventHandler.onDelete(evt.rows, evt.tableMap[evt.tableId]);
          break;
        case zongji_utils.eventIsXid(evt):
          const LSN = new common.ReplicatedGTID({
            raw_gtid: this.currentGTID!.raw,
            position: {
              filename: this.binLogPosition.filename,
              offset: evt.nextPosition
            }
          }).comparable;
          await this.eventHandler.onCommit(LSN);
          break;
      }

      this.queueMemoryUsage -= evt.size;
    };
  }

  isQueueOverCapacity(): boolean {
    return this.queueMemoryUsage >= this.queueMemoryLimit;
  }
}
