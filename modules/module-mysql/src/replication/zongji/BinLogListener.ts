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
  abortSignal: AbortSignal;
}

export class BinLogListener {
  private connectionManager: MySQLConnectionManager;
  private eventHandler: BinLogEventHandler;
  private binLogPosition: common.BinLogPosition;
  private currentGTID: common.ReplicatedGTID | null;

  zongji: ZongJi;
  processingQueue: async.QueueObject<BinLogEvent>;

  constructor(public options: BinLogListenerOptions) {
    this.connectionManager = options.connectionManager;
    this.eventHandler = options.eventHandler;
    this.binLogPosition = options.startPosition;
    this.currentGTID = null;

    this.processingQueue = async.queue(this.createQueueWorker(), 1);
    this.zongji = this.createZongjiListener();
  }

  public async start(): Promise<void> {
    logger.info(`Starting replication. Created replica client with serverId:${this.options.serverId}`);
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
            resolve(results);
          }
        }
      );
    });
    logger.info('Successfully set up replication connection heartbeat...');

    // The _socket member is only set after a query is run on the connection, so we set the timeout after setting the heartbeat.
    // The timeout here must be greater than the master_heartbeat_period.
    const socket = this.zongji.connection._socket!;
    socket.setTimeout(60_000, () => {
      socket.destroy(new Error('Replication connection timeout.'));
    });

    logger.info(`Reading binlog from: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`);
    this.zongji.start({
      // We ignore the unknown/heartbeat event since it currently serves no purpose other than to keep the connection alive
      // tablemap events always need to be included for the other row events to work
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'xid', 'rotate', 'gtidlog'],
      includeSchema: { [this.connectionManager.databaseName]: this.options.includedTables },
      filename: this.binLogPosition.filename,
      position: this.binLogPosition.offset,
      serverId: this.options.serverId
    } satisfies StartOptions);

    await new Promise<void>((resolve, reject) => {
      this.zongji.on('error', (error) => {
        logger.error('Binlog listener error:', error);
        this.zongji.stop();
        this.processingQueue.kill();
        reject(error);
      });

      this.processingQueue.error((error) => {
        logger.error('BinlogEvent processing error:', error);
        this.zongji.stop();
        this.processingQueue.kill();
        reject(error);
      });

      this.zongji.on('stopped', () => {
        logger.info('Binlog listener stopped. Replication ended.');
        resolve();
      });

      const stop = () => {
        logger.info('Abort signal received, stopping replication...');
        this.zongji.stop();
        this.processingQueue.kill();
        resolve();
      };

      this.options.abortSignal.addEventListener('abort', stop, { once: true });

      if (this.options.abortSignal.aborted) {
        // Generally this should have been picked up early, but we add this here as a failsafe.
        stop();
      }
    });
  }

  private createZongjiListener(): ZongJi {
    const zongji = this.connectionManager.createBinlogListener();

    zongji.on('binlog', async (evt) => {
      logger.info(`Received Binlog event:${evt.getEventName()}`);
      this.processingQueue.push(evt);

      // When the processing queue grows past the threshold, we pause the binlog listener
      if (this.processingQueue.length() > this.connectionManager.options.max_binlog_queue_size) {
        logger.info(
          `Max Binlog processing queue length [${this.connectionManager.options.max_binlog_queue_size}] reached. Pausing Binlog listener.`
        );
        zongji.pause();
        await this.processingQueue.empty();
        logger.info(`Binlog processing queue backlog cleared. Resuming Binlog listener.`);
        zongji.resume();
      }
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
    };
  }
}
