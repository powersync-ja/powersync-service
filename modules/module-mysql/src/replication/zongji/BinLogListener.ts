import * as common from '../../common/common-index.js';
import async from 'async';
import { BinLogEvent, BinLogQueryEvent, StartOptions, TableMapEntry, ZongJi } from '@powersync/mysql-zongji';
import * as zongji_utils from './zongji-utils.js';
import { Logger, logger as defaultLogger } from '@powersync/lib-services-framework';
import { MySQLConnectionManager } from '../MySQLConnectionManager.js';
import timers from 'timers/promises';
import pkg, {
  BaseFrom,
  DropIndexStatement,
  Parser as ParserType,
  RenameStatement,
  TruncateStatement
} from 'node-sql-parser';
import {
  isAlterTable,
  isColumnExpression,
  isConstraintExpression,
  isCreateUniqueIndex,
  isDropIndex,
  isDropTable,
  isRenameExpression,
  isRenameTable,
  isTruncate,
  matchedSchemaChangeQuery
} from '../../utils/parser-utils.js';

const { Parser } = pkg;

// Maximum time the Zongji listener can be paused before resuming automatically
// MySQL server automatically terminates replication connections after 60 seconds of inactivity
const MAX_PAUSE_TIME_MS = 45_000;

export type Row = Record<string, any>;

/**
 *  Schema changes that can be detected by inspecting query events.
 *  Note that create table statements are not included here, since new tables are automatically detected when row events
 *  are received for them.
 */
export enum SchemaChangeType {
  RENAME_TABLE = 'rename_table',
  DROP_TABLE = 'drop_table',
  TRUNCATE_TABLE = 'truncate_table',
  ALTER_TABLE_COLUMN = 'alter_table_column',
  REPLICATION_IDENTITY = 'replication_identity'
}

export interface SchemaChange {
  type: SchemaChangeType;
  /**
   *  The table that the schema change applies to.
   */
  table: string;
  newTable?: string; // Only for table renames
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
  private listenerError: Error | null;

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
    this.listenerError = null;
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
          `BinLog Listener ${isRestart ? 'restarted' : 'started'}. Listening for events from position: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
        );
        resolve();
      });
    });
  }

  private async restartZongji(): Promise<void> {
    if (this.zongji.stopped) {
      this.zongji = this.createZongjiListener();
      await this.start(true);
    }
  }

  private async stopZongji(): Promise<void> {
    if (!this.zongji.stopped) {
      this.logger.info('Stopping BinLog Listener...');
      await new Promise<void>((resolve) => {
        this.zongji.once('stopped', () => {
          resolve();
        });
        this.zongji.stop();
      });
      this.logger.info('BinLog Listener stopped.');
    }
  }

  public async stop(): Promise<void> {
    if (!(this.isStopped || this.isStopping)) {
      this.isStopping = true;
      await this.stopZongji();
      this.processingQueue.kill();

      this.isStopped = true;
    }
  }

  public async replicateUntilStopped(): Promise<void> {
    while (!this.isStopped) {
      await timers.setTimeout(1_000);
    }

    if (this.listenerError) {
      this.logger.error('BinLog Listener stopped due to an error:', this.listenerError);
      throw this.listenerError;
    }
  }

  private createProcessingQueue(): async.QueueObject<BinLogEvent> {
    const queue = async.queue(this.createQueueWorker(), 1);

    queue.error((error) => {
      if (!(this.isStopped || this.isStopping)) {
        this.listenerError = error;
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
      this.logger.debug(`Received BinLog event:${evt.getEventName()}`);

      this.processingQueue.push(evt);
      this.queueMemoryUsage += evt.size;

      // When the processing queue grows past the threshold, we pause the binlog listener
      if (this.isQueueOverCapacity()) {
        this.logger.info(
          `BinLog processing queue has reached its memory limit of [${this.connectionManager.options.binlog_queue_memory_limit}MB]. Pausing BinLog Listener.`
        );
        zongji.pause();
        const resumeTimeoutPromise = timers.setTimeout(MAX_PAUSE_TIME_MS);
        await Promise.race([this.processingQueue.drain(), resumeTimeoutPromise]);
        this.logger.info(`BinLog processing queue backlog cleared. Resuming BinLog Listener.`);
        zongji.resume();
      }
    });

    zongji.on('error', (error) => {
      if (!(this.isStopped || this.isStopping)) {
        this.listenerError = error;
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
          this.logger.debug(
            `Processed GTID event. Next position in BinLog: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
          );
          break;
        case zongji_utils.eventIsRotation(evt):
          const newFile = this.binLogPosition.filename !== evt.binlogName;
          this.binLogPosition.filename = evt.binlogName;
          this.binLogPosition.offset = evt.position;
          await this.eventHandler.onRotate();

          if (newFile) {
            this.logger.info(
              `Processed Rotate event. New BinLog file is: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
            );
          }
          break;
        case zongji_utils.eventIsWriteMutation(evt):
          await this.eventHandler.onWrite(evt.rows, evt.tableMap[evt.tableId]);
          this.logger.info(
            `Processed Write event for table:${evt.tableMap[evt.tableId].tableName}. ${evt.rows.length} row(s) inserted.`
          );
          break;
        case zongji_utils.eventIsUpdateMutation(evt):
          await this.eventHandler.onUpdate(
            evt.rows.map((row) => row.after),
            evt.rows.map((row) => row.before),
            evt.tableMap[evt.tableId]
          );
          this.logger.info(
            `Processed Update event for table:${evt.tableMap[evt.tableId].tableName}. ${evt.rows.length} row(s) updated.`
          );
          break;
        case zongji_utils.eventIsDeleteMutation(evt):
          await this.eventHandler.onDelete(evt.rows, evt.tableMap[evt.tableId]);
          this.logger.info(
            `Processed Delete event for table:${evt.tableMap[evt.tableId].tableName}. ${evt.rows.length} row(s) deleted.`
          );
          break;
        case zongji_utils.eventIsXid(evt):
          this.binLogPosition.offset = evt.nextPosition;
          const LSN = new common.ReplicatedGTID({
            raw_gtid: this.currentGTID!.raw,
            position: this.binLogPosition
          }).comparable;
          await this.eventHandler.onCommit(LSN);
          this.logger.debug(`Processed Xid event - transaction complete. LSN: ${LSN}.`);
          break;
        case zongji_utils.eventIsQuery(evt):
          await this.processQueryEvent(evt);
          break;
      }

      this.queueMemoryUsage -= evt.size;
    };
  }

  private async processQueryEvent(event: BinLogQueryEvent): Promise<void> {
    const { query, nextPosition } = event;

    // BEGIN query events mark the start of a transaction before any row events. They are not relevant for schema changes
    if (query === 'BEGIN') {
      return;
    }

    let schemaChanges: SchemaChange[] = [];
    try {
      schemaChanges = this.toSchemaChanges(query);
    } catch (error) {
      if (matchedSchemaChangeQuery(query, this.options.tableFilter)) {
        this.logger.warn(
          `Failed to parse query: [${query}]. 
      Please review for the schema changes and manually redeploy the sync rules if required.`
        );
      }
      return;
    }
    if (schemaChanges.length > 0) {
      // Since handling the schema changes can take a long time, we need to stop the Zongji listener instead of pausing it.
      await this.stopZongji();

      for (const change of schemaChanges) {
        this.logger.info(`Processing ${change.type} for table:${change.table}...`);
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

      this.logger.info(`Successfully processed ${schemaChanges.length} schema change(s).`);

      // If there are still events in the processing queue, we need to process those before restarting Zongji
      if (!this.processingQueue.idle()) {
        this.logger.info(`Processing [${this.processingQueue.length()}] events(s) before resuming...`);
        this.processingQueue.drain(async () => {
          await this.restartZongji();
        });
      } else {
        await this.restartZongji();
      }
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
      if (isTruncate(statement)) {
        const truncateStatement = statement as TruncateStatement;
        // Truncate statements can apply to multiple tables
        for (const entity of truncateStatement.name) {
          changes.push({ type: SchemaChangeType.TRUNCATE_TABLE, table: entity.table });
        }
      } else if (isDropTable(statement)) {
        for (const entity of statement.name) {
          changes.push({ type: SchemaChangeType.DROP_TABLE, table: entity.table });
        }
      } else if (isDropIndex(statement)) {
        const dropStatement = statement as DropIndexStatement;
        changes.push({
          type: SchemaChangeType.REPLICATION_IDENTITY,
          table: dropStatement.table.table
        });
      } else if (isCreateUniqueIndex(statement)) {
        // Potential change to the replication identity if the table has no prior unique constraint
        changes.push({
          type: SchemaChangeType.REPLICATION_IDENTITY,
          // @ts-ignore - The type definitions for node-sql-parser do not reflect the correct structure here
          table: statement.table!.table
        });
      } else if (isRenameTable(statement)) {
        const renameStatement = statement as RenameStatement;
        // Rename statements can apply to multiple tables
        for (const table of renameStatement.table) {
          changes.push({
            type: SchemaChangeType.RENAME_TABLE,
            table: table[0].table,
            newTable: table[1].table
          });
        }
      } else if (isAlterTable(statement)) {
        const fromTable = statement.table[0] as BaseFrom;
        for (const expression of statement.expr) {
          if (isRenameExpression(expression)) {
            changes.push({
              type: SchemaChangeType.RENAME_TABLE,
              table: fromTable.table,
              newTable: expression.table
            });
          } else if (isColumnExpression(expression)) {
            changes.push({
              type: SchemaChangeType.ALTER_TABLE_COLUMN,
              table: fromTable.table
            });
          } else if (isConstraintExpression(expression)) {
            // Potential changes to the replication identity
            changes.push({
              type: SchemaChangeType.REPLICATION_IDENTITY,
              table: fromTable.table
            });
          }
        }
      }
    }
    // Filter out schema changes that are not relevant to the included tables
    return changes.filter(
      (change) =>
        this.options.tableFilter(change.table) || (change.newTable && this.options.tableFilter(change.newTable))
    );
  }
}
