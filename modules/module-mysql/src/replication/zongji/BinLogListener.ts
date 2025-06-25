import * as common from '../../common/common-index.js';
import async from 'async';
import { BinLogEvent, BinLogQueryEvent, StartOptions, TableMapEntry, ZongJi } from '@powersync/mysql-zongji';
import * as zongji_utils from './zongji-utils.js';
import { Logger, logger as defaultLogger } from '@powersync/lib-services-framework';
import { MySQLConnectionManager } from '../MySQLConnectionManager.js';
import timers from 'timers/promises';
import pkg, { BaseFrom, Parser as ParserType, RenameStatement, TruncateStatement } from 'node-sql-parser';

const { Parser } = pkg;

// Maximum time the Zongji listener can be paused before resuming automatically
// MySQL server automatically terminates replication connections after 60 seconds of inactivity
const MAX_PAUSE_TIME_MS = 45_000;

export type Row = Record<string, any>;

export enum SchemaChangeType {
  CREATE_TABLE = 'create_table',
  RENAME_TABLE = 'rename_table',
  DROP_TABLE = 'drop_table',
  TRUNCATE_TABLE = 'truncate_table',
  MODIFY_COLUMN = 'modify_column',
  DROP_COLUMN = 'drop_column',
  ADD_COLUMN = 'add_column',
  RENAME_COLUMN = 'rename_column',
  REPLICATION_IDENTITY = 'replication_identity'
}

export interface SchemaChange {
  type: SchemaChangeType;
  /**
   *  The table that the schema change applies to.
   */
  table: string;
  newTable?: string; // Only for table renames
  /**
   *  ColumnDetails. Only applicable for column schema changes.
   */
  column?: {
    /**
     *  The column that the schema change applies to.
     */
    column: string;
    newColumn?: string; // Only for column renames
  };
}

export interface BinLogEventHandler {
  onTransactionStart: (options: { timestamp: Date }) => Promise<void>;
  onRotate: () => Promise<void>;
  onWrite: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onUpdate: (rowsAfter: Row[], rowsBefore: Row[], tableMap: TableMapEntry) => Promise<void>;
  onDelete: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onCommit: (lsn: string) => Promise<void>;
  onSchemaChange: (change: SchemaChange) => Promise<void>;
}

export interface BinLogListenerOptions {
  connectionManager: MySQLConnectionManager;
  eventHandler: BinLogEventHandler;
  // Filter for tables to include in the replication
  tableFilter: (tableName: string) => boolean;
  serverId: number;
  startPosition: common.BinLogPosition;
  logger?: Logger;
}

/**
 *  Wrapper class for the Zongji BinLog listener. Internally handles the creation and management of the listener and posts
 *  events on the provided BinLogEventHandler.
 */
export class BinLogListener {
  private sqlParser: ParserType;
  private connectionManager: MySQLConnectionManager;
  private eventHandler: BinLogEventHandler;
  private binLogPosition: common.BinLogPosition;
  private currentGTID: common.ReplicatedGTID | null;
  private logger: Logger;

  zongji: ZongJi;
  processingQueue: async.QueueObject<BinLogEvent>;

  isStopped: boolean = false;
  isStopping: boolean = false;
  /**
   *  The combined size in bytes of all the binlog events currently in the processing queue.
   */
  queueMemoryUsage: number = 0;

  constructor(public options: BinLogListenerOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.connectionManager = options.connectionManager;
    this.eventHandler = options.eventHandler;
    this.binLogPosition = options.startPosition;
    this.currentGTID = null;
    this.sqlParser = new Parser();
    this.processingQueue = this.createProcessingQueue();
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

    this.logger.info(
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
            this.logger.info('Successfully set up replication connection heartbeat...');
            resolve(results);
          }
        }
      );
    });

    // The _socket member is only set after a query is run on the connection, so we set the timeout after setting the heartbeat.
    // The timeout here must be greater than the master_heartbeat_period.
    const socket = this.zongji.connection._socket!;
    socket.setTimeout(60_000, () => {
      this.logger.info('Destroying socket due to replication connection timeout.');
      socket.destroy(new Error('Replication connection timeout.'));
    });

    this.zongji.start({
      // We ignore the unknown/heartbeat event since it currently serves no purpose other than to keep the connection alive
      // tablemap events always need to be included for the other row events to work
      includeEvents: ['tablemap', 'writerows', 'updaterows', 'deleterows', 'xid', 'rotate', 'gtidlog', 'query'],
      includeSchema: { [this.connectionManager.databaseName]: this.options.tableFilter },
      filename: this.binLogPosition.filename,
      position: this.binLogPosition.offset,
      serverId: this.options.serverId
    } satisfies StartOptions);

    return new Promise((resolve) => {
      this.zongji.once('ready', () => {
        this.logger.info(
          `BinLog Listener ${isRestart ? 'restarted' : 'started'}. Listening for events from position: ${this.binLogPosition.filename}:${this.binLogPosition.offset}.`
        );
        resolve();
      });
    });
  }

  private async restartZongji(): Promise<void> {
    this.zongji = this.createZongjiListener();
    await this.start(true);
  }

  private async stopZongji(): Promise<void> {
    await new Promise<void>((resolve) => {
      this.zongji.once('stopped', () => {
        resolve();
      });
      this.zongji.stop();
    });

    // Wait until all the current events in the processing queue are also processed
    if (this.processingQueue.length() > 0) {
      await this.processingQueue.empty();
    }
  }

  public async stop(): Promise<void> {
    if (!(this.isStopped || this.isStopping)) {
      this.logger.info('Stopping BinLog Listener...');
      this.isStopping = true;
      await new Promise<void>((resolve) => {
        if (this.isStopped) {
          resolve();
        }
        this.zongji.once('stopped', () => {
          this.isStopped = true;
          this.logger.info('BinLog Listener stopped. Replication ended.');
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
        this.logger.error('Error processing BinLog event:', error);
        this.stop();
      } else {
        this.logger.warn('Error processing BinLog event during shutdown:', error);
      }
    });

    return queue;
  }

  private createZongjiListener(): ZongJi {
    const zongji = this.connectionManager.createBinlogListener();

    zongji.on('binlog', async (evt) => {
      this.logger.info(`Received BinLog event:${evt.getEventName()}`);
      // We have to handle schema change events before handling more binlog events,
      // This avoids a bunch of possible race conditions
      if (zongji_utils.eventIsQuery(evt)) {
        await this.processQueryEvent(evt);
      } else {
        this.processingQueue.push(evt);
        this.queueMemoryUsage += evt.size;

        // When the processing queue grows past the threshold, we pause the binlog listener
        if (this.isQueueOverCapacity()) {
          this.logger.info(
            `BinLog processing queue has reached its memory limit of [${this.connectionManager.options.binlog_queue_memory_limit}MB]. Pausing BinLog Listener.`
          );
          zongji.pause();
          const resumeTimeoutPromise = timers.setTimeout(MAX_PAUSE_TIME_MS);
          await Promise.race([this.processingQueue.empty(), resumeTimeoutPromise]);
          this.logger.info(`BinLog processing queue backlog cleared. Resuming BinLog Listener.`);
          zongji.resume();
        }
      }
    });

    zongji.on('error', (error) => {
      if (!(this.isStopped || this.isStopping)) {
        this.logger.error('BinLog Listener error:', error);
        this.stop();
      } else {
        this.logger.warn('Ignored BinLog Listener error during shutdown:', error);
      }
    });

    return zongji;
  }

  isQueueOverCapacity(): boolean {
    return this.queueMemoryUsage >= this.queueMemoryLimit;
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
          await this.eventHandler.onTransactionStart({ timestamp: new Date(evt.timestamp) });
          this.logger.info(
            `Processed GTID log event. Next position in BinLog: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
          );
          break;
        case zongji_utils.eventIsRotation(evt):
          const newFile = this.binLogPosition.filename !== evt.binlogName;
          this.binLogPosition.filename = evt.binlogName;
          this.binLogPosition.offset = evt.position;
          await this.eventHandler.onRotate();

          this.logger.info(
            `Processed Rotate log event. ${newFile ? `New BinLog file is: ${this.binLogPosition.filename}:${this.binLogPosition.offset}` : ''}`
          );

          break;
        case zongji_utils.eventIsWriteMutation(evt):
          await this.eventHandler.onWrite(evt.rows, evt.tableMap[evt.tableId]);
          this.logger.info(
            `Processed Write row event of ${evt.rows.length} rows for table: ${evt.tableMap[evt.tableId].tableName}`
          );
          break;
        case zongji_utils.eventIsUpdateMutation(evt):
          await this.eventHandler.onUpdate(
            evt.rows.map((row) => row.after),
            evt.rows.map((row) => row.before),
            evt.tableMap[evt.tableId]
          );
          this.logger.info(
            `Processed Update row event of ${evt.rows.length} rows for table: ${evt.tableMap[evt.tableId].tableName}`
          );
          break;
        case zongji_utils.eventIsDeleteMutation(evt):
          await this.eventHandler.onDelete(evt.rows, evt.tableMap[evt.tableId]);
          this.logger.info(
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
          this.logger.info(`Processed Xid log event. Transaction LSN: ${LSN}.`);
          break;
      }

      this.queueMemoryUsage -= evt.size;
    };
  }

  private async processQueryEvent(event: BinLogQueryEvent): Promise<void> {
    const { query, nextPosition } = event;

    // Ignore BEGIN queries
    if (query === 'BEGIN') {
      return;
    }

    const schemaChanges = this.toSchemaChanges(query);
    if (schemaChanges.length > 0) {
      this.logger.info(`Processing schema change query: ${query}`);
      this.logger.info(`Stopping BinLog Listener to process ${schemaChanges.length} schema change events...`);
      // Since handling the schema changes can take a long time, we need to stop the Zongji listener instead of pausing it.
      await this.stopZongji();

      for (const change of schemaChanges) {
        await this.eventHandler.onSchemaChange(change);
      }

      // DDL queries are auto commited, and so do not come with a corresponding Xid event.
      // This is problematic for DDL queries which result in row events, so we manually commit here.
      this.binLogPosition.offset = nextPosition;
      const LSN = new common.ReplicatedGTID({
        raw_gtid: this.currentGTID!.raw,
        position: this.binLogPosition
      }).comparable;
      await this.eventHandler.onCommit(LSN);

      this.logger.info(`Successfully processed schema changes.`);
      // Restart the Zongji listener
      await this.restartZongji();
    }
  }

  /**
   *  Function that interprets a DDL query for any applicable schema changes.
   *  If the query does not contain any relevant schema changes, an empty array is returned.
   *
   *  @param query
   */
  private toSchemaChanges(query: string): SchemaChange[] {
    const ast = this.sqlParser.astify(query, { database: 'MySQL' });
    const statements = Array.isArray(ast) ? ast : [ast];

    const changes: SchemaChange[] = [];
    for (const statement of statements) {
      // @ts-ignore
      if (statement.type === 'rename') {
        const renameStatement = statement as RenameStatement;
        // Rename statements can apply to multiple tables
        for (const table of renameStatement.table) {
          changes.push({
            type: SchemaChangeType.RENAME_TABLE,
            table: table[0].table,
            newTable: table[1].table
          });
        }
      } else if (statement.type === 'create' && statement.keyword === 'table' && statement.temporary === null) {
        changes.push({
          type: SchemaChangeType.CREATE_TABLE,
          table: statement.table![0].table
        });
      } // @ts-ignore
      else if (statement.type === 'truncate') {
        const truncateStatement = statement as TruncateStatement;
        // Truncate statements can apply to multiple tables
        for (const entity of truncateStatement.name) {
          changes.push({ type: SchemaChangeType.TRUNCATE_TABLE, table: entity.table });
        }
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
          const columnChange: SchemaChange = {
            type: this.toColumnSchemaChangeType(expression.action),
            table: fromTable.table,
            column: {
              column: expression.column.column
            }
          };

          if (expression.action === 'change' || expression.action === 'rename') {
            columnChange.column = {
              column: expression.old_column.column,
              newColumn: expression.column.column
            };
          }
          changes.push(columnChange);
        } else if (expression.resource === 'key' && expression.keyword === 'primary key') {
          // This is a special case for MySQL, where the primary key is being set or changed
          // We treat this as a replication identity change
          changes.push({
            type: SchemaChangeType.REPLICATION_IDENTITY,
            table: fromTable.table
          });
        }
      }
    }

    // Filter out schema changes that are not relevant to the included tables
    return changes.filter(
      (change) =>
        this.options.tableFilter(change.table) || (change.newTable && this.options.tableFilter(change.newTable))
    );
  }

  private toColumnSchemaChangeType(action: string): SchemaChangeType {
    switch (action) {
      case 'drop':
        return SchemaChangeType.DROP_COLUMN;
      case 'add':
        return SchemaChangeType.ADD_COLUMN;
      case 'modify':
        return SchemaChangeType.MODIFY_COLUMN;
      case 'change':
      case 'rename':
        return SchemaChangeType.RENAME_COLUMN;
      default:
        throw new Error(`Unknown column schema change action: ${action}`);
    }
  }
}
