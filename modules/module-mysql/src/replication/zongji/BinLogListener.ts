import * as common from '../../common/common-index.js';
import async from 'async';
import { BinLogEvent, StartOptions, TableMapEntry, ZongJi } from '@powersync/mysql-zongji';
import * as zongji_utils from './zongji-utils.js';
import { logger } from '@powersync/lib-services-framework';
import { MySQLConnectionManager } from '../MySQLConnectionManager.js';
import { AST, BaseFrom, Parser, RenameStatement, TruncateStatement } from 'node-sql-parser';
import timers from 'timers/promises';

// Maximum time the processing queue can be paused before resuming automatically
// MySQL server will automatically terminate replication connections after 60 seconds of inactivity, so this guards against that.
const MAX_QUEUE_PAUSE_TIME_MS = 45_000;

export type Row = Record<string, any>;

export enum SchemaChangeType {
  CREATE_TABLE = 'create_table',
  RENAME_TABLE = 'rename_table',
  DROP_TABLE = 'drop_table',
  TRUNCATE_TABLE = 'truncate_table',
  MODIFY_COLUMN = 'modify_column',
  DROP_COLUMN = 'drop_column',
  ADD_COLUMN = 'add_column',
  RENAME_COLUMN = 'rename_column'
}

export interface SchemaChange {
  type: SchemaChangeType;
  /**
   *  The table that the schema change applies to.
   */
  table: string;
  newTable?: string; // Only for table renames
  column?: {
    /**
     *  The column that the schema change applies to.
     */
    column: string;
    newColumn?: string; // Only for column renames
  };
}

export interface BinLogEventHandler {
  onWrite: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onUpdate: (rowsAfter: Row[], rowsBefore: Row[], tableMap: TableMapEntry) => Promise<void>;
  onDelete: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onCommit: (lsn: string) => Promise<void>;
  onSchemaChange: (change: SchemaChange) => Promise<void>;
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
  private sqlParser: Parser;
  private connectionManager: MySQLConnectionManager;
  private eventHandler: BinLogEventHandler;
  private binLogPosition: common.BinLogPosition;
  private currentGTID: common.ReplicatedGTID | null;

  isStopped: boolean = false;
  isStopping: boolean = false;
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
    this.sqlParser = new Parser();
    this.processingQueue = this.createProcessingQueue();
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

  public async start(isRestart: boolean = false): Promise<void> {
    if (this.isStopped) {
      return;
    }

    logger.info(
      `${isRestart ? 'Restarting' : 'Starting'} BinLog Listener with replica client id:${this.options.serverId}...`
    );

    // Set a heartbeat interval for the Zongji replication connection
    // Zongji does not explicitly handle the heartbeat events - they are categorized as event:unknown
    // The heartbeat events are enough to keep the connection alive for setTimeout to work on the socket.
    // The heartbeat needs to be set before starting the listener, since the replication connection is locked once replicating
    await new Promise((resolve, reject) => {
      this.zongji.connection.query(
        // In nanoseconds, 10^9 = 1s
        'set @master_heartbeat_period=28*1000000000',
        (error: any, results: any, _fields: any) => {
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
      logger.info('Destroying socket due to replication connection timeout.');
      socket.destroy(new Error('Replication connection timeout.'));
    });

    this.zongji.start({
      // We ignore the unknown/heartbeat event since it currently serves no purpose other than to keep the connection alive
      // tablemap events always need to be included for the other row events to work
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'xid', 'rotate', 'gtidlog', 'query'],
      includeSchema: { [this.connectionManager.databaseName]: this.options.includedTables },
      filename: this.binLogPosition.filename,
      position: this.binLogPosition.offset,
      serverId: this.options.serverId
    } satisfies StartOptions);

    return new Promise((resolve) => {
      this.zongji.once('ready', () => {
        logger.info(
          `BinLog Listener ${isRestart ? 'restarted' : 'started'}. Listening for events from position: ${this.binLogPosition.filename}:${this.binLogPosition.offset}.`
        );
        resolve();
      });
    });
  }

  public async stop(): Promise<void> {
    if (!(this.isStopped || this.isStopping)) {
      logger.info('Stopping BinLog Listener...');
      this.isStopping = true;
      await new Promise<void>((resolve) => {
        if (this.isStopped) {
          resolve();
        }
        this.zongji.once('stopped', () => {
          this.isStopped = true;
          logger.info('BinLog Listener stopped. Replication ended.');
          resolve();
        });
        this.zongji.stop();
        this.processingQueue.kill();
      });
    }
  }

  public async replicateUntilStopped(): Promise<void> {
    while (!this.isStopped) {
      await timers.setTimeout(1_000);
    }
  }

  private createProcessingQueue(): async.QueueObject<BinLogEvent> {
    const queue = async.queue(this.createQueueWorker(), 1);

    queue.error((error) => {
      if (!(this.isStopped || this.isStopping)) {
        logger.error('Error processing BinLog event:', error);
        this.stop();
      } else {
        logger.warn('Error processing BinLog event during shutdown:', error);
      }
    });

    return queue;
  }

  private createZongjiListener(): ZongJi {
    const zongji = this.connectionManager.createBinlogListener();

    zongji.on('binlog', async (evt) => {
      logger.info(`Received BinLog event:${evt.getEventName()}`);
      this.processingQueue.push(evt);
      this.queueMemoryUsage += evt.size;

      // When the processing queue grows past the threshold, we pause the binlog listener
      if (this.isQueueOverCapacity()) {
        logger.info(
          `BinLog processing queue has reached its memory limit of [${this.connectionManager.options.binlog_queue_memory_limit}MB]. Pausing BinLog Listener.`
        );
        zongji.pause();
        const resumeTimeoutPromise = timers.setTimeout(MAX_QUEUE_PAUSE_TIME_MS);
        await Promise.race([this.processingQueue.empty(), resumeTimeoutPromise]);
        logger.info(`BinLog processing queue backlog cleared. Resuming BinLog Listener.`);
        zongji.resume();
      }
    });

    zongji.on('error', (error) => {
      if (!(this.isStopped || this.isStopping)) {
        logger.error('BinLog Listener error:', error);
        this.stop();
      } else {
        logger.warn('Ignored BinLog Listener error during shutdown:', error);
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
          this.binLogPosition.offset = evt.nextPosition;
          logger.info(
            `Processed GTID log event. Next position in BinLog: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
          );
          break;
        case zongji_utils.eventIsRotation(evt):
          if (this.binLogPosition.filename !== evt.binlogName) {
            logger.info(
              `Processed Rotate log event. New BinLog file is: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
            );
          }
          this.binLogPosition.filename = evt.binlogName;
          this.binLogPosition.offset = evt.position;

          break;
        case zongji_utils.eventIsWriteMutation(evt):
          await this.eventHandler.onWrite(evt.rows, evt.tableMap[evt.tableId]);
          logger.info(
            `Processed Write row event of ${evt.rows.length} rows for table: ${evt.tableMap[evt.tableId].tableName}`
          );
          break;
        case zongji_utils.eventIsUpdateMutation(evt):
          await this.eventHandler.onUpdate(
            evt.rows.map((row) => row.after),
            evt.rows.map((row) => row.before),
            evt.tableMap[evt.tableId]
          );
          logger.info(
            `Processed Update row event of ${evt.rows.length} rows for table: ${evt.tableMap[evt.tableId].tableName}`
          );
          break;
        case zongji_utils.eventIsDeleteMutation(evt):
          await this.eventHandler.onDelete(evt.rows, evt.tableMap[evt.tableId]);
          logger.info(
            `Processed Delete row event of ${evt.rows.length} rows for table: ${evt.tableMap[evt.tableId].tableName}`
          );
          break;
        case zongji_utils.eventIsXid(evt):
          this.binLogPosition.offset = evt.nextPosition;
          const LSN = new common.ReplicatedGTID({
            raw_gtid: this.currentGTID!.raw,
            position: this.binLogPosition
          }).comparable;
          await this.eventHandler.onCommit(LSN);
          logger.info(`Processed Xid log event. Transaction LSN: ${LSN}.`);
          break;
        case zongji_utils.eventIsQuery(evt):
          // Ignore BEGIN queries
          if (evt.query === 'BEGIN') {
            break;
          }
          const ast = this.sqlParser.astify(evt.query, { database: 'MySQL' });
          const statements = Array.isArray(ast) ? ast : [ast];
          const schemaChanges = this.toSchemaChanges(statements);
          if (schemaChanges.length > 0) {
            await this.handleSchemaChanges(schemaChanges, evt.nextPosition);
          } else {
            // Still have to update the binlog position, even if there were no effective schema changes
            this.binLogPosition.offset = evt.nextPosition;
          }

          break;
      }

      this.queueMemoryUsage -= evt.size;
    };
  }

  private async handleSchemaChanges(changes: SchemaChange[], nextPosition: number): Promise<void> {
    logger.info(`Stopping BinLog Listener to process ${changes.length} schema change events...`);
    // Since handling the schema changes can take a long time, we need to stop the Zongji listener instead of pausing it.
    await new Promise<void>((resolve) => {
      this.zongji.once('stopped', () => {
        resolve();
      });
      this.zongji.stop();
    });
    for (const change of changes) {
      logger.info(`Processing schema change: ${change.type} for table: ${change.table}`);
      await this.eventHandler.onSchemaChange(change);
    }

    this.binLogPosition.offset = nextPosition;
    // Wait until all the current events in the processing queue are processed and the binlog position has been updated
    // otherwise we risk processing the same events again.
    if (this.processingQueue.length() > 0) {
      await this.processingQueue.empty();
    }
    this.zongji = this.createZongjiListener();
    logger.info(`Successfully processed schema changes.`);
    // Restart the Zongji listener
    await this.start(true);
  }

  private toSchemaChanges(statements: AST[]): SchemaChange[] {
    // TODO: We need to check if schema filtering is also required
    const changes: SchemaChange[] = [];
    for (const statement of statements) {
      // @ts-ignore
      if (statement.type === 'rename') {
        const renameStatement = statement as RenameStatement;
        for (const table of renameStatement.table) {
          changes.push({
            type: SchemaChangeType.RENAME_TABLE,
            table: table[0].table,
            newTable: table[1].table
          });
        }
      } // @ts-ignore
      else if (statement.type === 'truncate' && statement.keyword === 'table') {
        const truncateStatement = statement as TruncateStatement;
        // Truncate statements can apply to multiple tables
        for (const entity of truncateStatement.name) {
          changes.push({ type: SchemaChangeType.TRUNCATE_TABLE, table: entity.table });
        }
      } else if (statement.type === 'create' && statement.keyword === 'table' && statement.temporary === null) {
        changes.push({
          type: SchemaChangeType.CREATE_TABLE,
          table: statement.table![0].table
        });
      } else if (statement.type === 'drop' && statement.keyword === 'table') {
        // Drop statements can apply to multiple tables
        for (const entity of statement.name) {
          changes.push({ type: SchemaChangeType.DROP_TABLE, table: entity.table });
        }
      } else if (statement.type === 'alter') {
        const expression = statement.expr[0];
        const fromTable = statement.table[0] as BaseFrom;
        if (expression.resource === 'table') {
          if (expression.action === 'rename') {
            changes.push({
              type: SchemaChangeType.RENAME_TABLE,
              table: fromTable.table,
              newTable: expression.table
            });
          }
        } else if (expression.resource === 'column') {
          const column = expression.column;
          if (expression.action === 'drop') {
            changes.push({
              type: SchemaChangeType.DROP_COLUMN,
              table: fromTable.table,
              column: {
                column: column.column
              }
            });
          } else if (expression.action === 'add') {
            changes.push({
              type: SchemaChangeType.ADD_COLUMN,
              table: fromTable.table,
              column: {
                column: column.column
              }
            });
          } else if (expression.action === 'modify') {
            changes.push({
              type: SchemaChangeType.MODIFY_COLUMN,
              table: fromTable.table,
              column: {
                column: column.column
              }
            });
          } else if (expression.action === 'change') {
            changes.push({
              type: SchemaChangeType.RENAME_COLUMN,
              table: fromTable.table,
              column: {
                column: expression.old_column.column,
                newColumn: column.column
              }
            });
          } else if (expression.action === 'rename') {
            changes.push({
              type: SchemaChangeType.RENAME_COLUMN,
              table: fromTable.table,
              column: {
                column: expression.old_column.column,
                newColumn: column.column
              }
            });
          }
        }
      }
    }

    // Filter out schema changes that are not relevant to the included tables
    return changes.filter(
      (change) =>
        this.options.includedTables.includes(change.table) ||
        (change.newTable && this.options.includedTables.includes(change.newTable))
    );
  }

  isQueueOverCapacity(): boolean {
    return this.queueMemoryUsage >= this.queueMemoryLimit;
  }
}
