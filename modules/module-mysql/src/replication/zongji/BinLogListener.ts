import * as common from '../../common/common-index.js';
import async from 'async';
import { BinLogEvent, BinLogQueryEvent, StartOptions, TableMapEntry, ZongJi } from '@powersync/mysql-zongji';
import * as zongji_utils from './zongji-utils.js';
import { Logger, logger as defaultLogger } from '@powersync/lib-services-framework';
import { MySQLConnectionManager } from '../MySQLConnectionManager.js';
import timers from 'timers/promises';
import pkg, {
  AST,
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
import { TablePattern } from '@powersync/service-sync-rules';

const { Parser } = pkg;

/**
 *  Seconds of inactivity after which a keepalive event is sent by the MySQL server.
 */
export const KEEPALIVE_INACTIVITY_THRESHOLD = 30;
export type Row = Record<string, any>;

/**
 *  Schema changes that are detectable by inspecting query events.
 *  Create table statements are not included here, since new tables are automatically detected when row events
 *  are received for them.
 */
export enum SchemaChangeType {
  RENAME_TABLE = 'Rename Table',
  DROP_TABLE = 'Drop Table',
  TRUNCATE_TABLE = 'Truncate Table',
  ALTER_TABLE_COLUMN = 'Alter Table Column',
  REPLICATION_IDENTITY = 'Alter Replication Identity'
}

export interface SchemaChange {
  type: SchemaChangeType;
  /**
   *  The table that the schema change applies to.
   */
  table: string;
  schema: string;
  /**
   *  Populated for table renames if the newTable was matched by the DatabaseFilter
   */
  newTable?: string;
}

export interface BinLogEventHandler {
  onTransactionStart: (options: { timestamp: Date }) => Promise<void>;
  onRotate: () => Promise<void>;
  onWrite: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onUpdate: (rowsAfter: Row[], rowsBefore: Row[], tableMap: TableMapEntry) => Promise<void>;
  onDelete: (rows: Row[], tableMap: TableMapEntry) => Promise<void>;
  onCommit: (lsn: string) => Promise<void>;
  onSchemaChange: (change: SchemaChange) => Promise<void>;
  onKeepAlive: (lsn: string) => Promise<void>;
}

export interface BinLogListenerOptions {
  connectionManager: MySQLConnectionManager;
  eventHandler: BinLogEventHandler;
  sourceTables: TablePattern[];
  serverId: number;
  startGTID: common.ReplicatedGTID;
  logger?: Logger;
  keepAliveInactivitySeconds?: number;
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
  private currentGTID: common.ReplicatedGTID;
  private logger: Logger;
  private listenerError: Error | null;
  private databaseFilter: { [schema: string]: (table: string) => boolean };

  private isStopped: boolean = false;
  private isStopping: boolean = false;

  // Flag to indicate if are currently in a transaction that involves multiple row mutation events.
  private isTransactionOpen = false;
  zongji: ZongJi;
  processingQueue: async.QueueObject<BinLogEvent>;

  /**
   *  The combined size in bytes of all the binlog events currently in the processing queue.
   */
  queueMemoryUsage: number = 0;

  constructor(public options: BinLogListenerOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.connectionManager = options.connectionManager;
    this.eventHandler = options.eventHandler;
    this.binLogPosition = options.startGTID.position;
    this.currentGTID = options.startGTID;
    this.sqlParser = new Parser();
    this.processingQueue = this.createProcessingQueue();
    this.zongji = this.createZongjiListener();
    this.listenerError = null;
    this.databaseFilter = this.createDatabaseFilter(options.sourceTables);
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

    // Set a heartbeat interval for the Zongji replication connection, these events are enough to keep the connection
    // alive for setTimeout to work on the socket.
    // The heartbeat needs to be set before starting the listener, since the replication connection is locked once replicating
    await new Promise((resolve, reject) => {
      this.zongji.connection.query(
        // In nanoseconds, 10^9 = 1s
        `set @master_heartbeat_period=${this.options.keepAliveInactivitySeconds ?? KEEPALIVE_INACTIVITY_THRESHOLD}*1000000000`,
        (error: any, results: any, _fields: any) => {
          if (error) {
            reject(error);
          } else {
            this.logger.info('Successfully set up replication connection heartbeat.');
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
      // Tablemap events always need to be included for the other row events to work
      includeEvents: [
        'tablemap',
        'writerows',
        'updaterows',
        'deleterows',
        'xid',
        'rotate',
        'gtidlog',
        'query',
        'heartbeat',
        'heartbeat_v2'
      ],
      includeSchema: this.databaseFilter,
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
        await this.stopZongji();
        await this.processingQueue.drain();
        this.logger.info(`BinLog processing queue backlog cleared. Resuming BinLog Listener.`);
        await this.restartZongji();
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
          this.logger.info(`Processed GTID event: ${this.currentGTID.comparable}`);
          break;
        case zongji_utils.eventIsRotation(evt):
          // The first event when starting replication is a synthetic Rotate event
          // It describes the last binlog file and position that the replica client processed
          this.binLogPosition.filename = evt.binlogName;
          this.binLogPosition.offset = evt.nextPosition !== 0 ? evt.nextPosition : evt.position;
          await this.eventHandler.onRotate();

          const newFile = this.binLogPosition.filename !== evt.binlogName;
          if (newFile) {
            this.logger.info(
              `Processed Rotate event. New BinLog file is: ${this.binLogPosition.filename}:${this.binLogPosition.offset}`
            );
          }

          break;
        case zongji_utils.eventIsWriteMutation(evt):
          const tableMap = evt.tableMap[evt.tableId];
          await this.eventHandler.onWrite(evt.rows, tableMap);
          this.binLogPosition.offset = evt.nextPosition;
          this.logger.info(
            `Processed Write event for table [${tableMap.parentSchema}.${tableMap.tableName}]. ${evt.rows.length} row(s) inserted.`
          );
          break;
        case zongji_utils.eventIsUpdateMutation(evt):
          await this.eventHandler.onUpdate(
            evt.rows.map((row) => row.after),
            evt.rows.map((row) => row.before),
            evt.tableMap[evt.tableId]
          );
          this.binLogPosition.offset = evt.nextPosition;
          this.logger.info(
            `Processed Update event for table [${evt.tableMap[evt.tableId].tableName}]. ${evt.rows.length} row(s) updated.`
          );
          break;
        case zongji_utils.eventIsDeleteMutation(evt):
          await this.eventHandler.onDelete(evt.rows, evt.tableMap[evt.tableId]);
          this.binLogPosition.offset = evt.nextPosition;
          this.logger.info(
            `Processed Delete event for table [${evt.tableMap[evt.tableId].tableName}]. ${evt.rows.length} row(s) deleted.`
          );
          break;
        case zongji_utils.eventIsHeartbeat(evt):
        case zongji_utils.eventIsHeartbeat_v2(evt):
          // Heartbeats are sent by the master to keep the connection alive after a period of inactivity. They are synthetic
          // so are not written to the binlog. Consequently, they have no effect on the binlog position.
          // We forward these along with the current GTID to the event handler, but don't want to do this if a transaction is in progress.
          if (!this.isTransactionOpen) {
            await this.eventHandler.onKeepAlive(this.currentGTID.comparable);
          }
          this.logger.debug(`Processed Heartbeat event. Current GTID is: ${this.currentGTID.comparable}`);
          break;
        case zongji_utils.eventIsXid(evt):
          this.isTransactionOpen = false;
          this.binLogPosition.offset = evt.nextPosition;
          const LSN = new common.ReplicatedGTID({
            raw_gtid: this.currentGTID.raw,
            position: this.binLogPosition
          }).comparable;
          await this.eventHandler.onCommit(LSN);
          this.logger.info(`Processed Xid event - transaction complete. LSN: ${LSN}.`);
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

    // BEGIN query events mark the start of a transaction before any row events. They are not schema changes so no further parsing is necessary.
    if (query === 'BEGIN') {
      this.isTransactionOpen = true;
      return;
    }

    const schemaChanges = this.toSchemaChanges(query, event.schema);
    if (schemaChanges.length > 0) {
      // Handling schema changes can take a long time, so we stop the Zongji listener whilst handling them to prevent the listener from timing out.
      await this.stopZongji();

      for (const change of schemaChanges) {
        this.logger.info(`Processing schema change ${change.type} for table [${change.schema}.${change.table}]`);
        await this.eventHandler.onSchemaChange(change);
      }

      // DDL queries are auto commited, but do not come with a corresponding Xid event, in those cases we trigger a manual commit if we are not already in a transaction.
      // Some DDL queries include row events, and in those cases will include a Xid event.
      if (!this.isTransactionOpen) {
        this.binLogPosition.offset = nextPosition;
        const LSN = new common.ReplicatedGTID({
          raw_gtid: this.currentGTID.raw,
          position: this.binLogPosition
        }).comparable;
        await this.eventHandler.onCommit(LSN);
      }

      this.logger.info(`Successfully processed ${schemaChanges.length} schema change(s).`);

      // If there are still events in the processing queue, we need to process those before restarting Zongji
      // This avoids potentially processing the same events again after a restart.
      if (!this.processingQueue.idle()) {
        this.logger.info(`Processing [${this.processingQueue.length()}] events(s) before resuming...`);
        this.processingQueue.drain(async () => {
          await this.restartZongji();
        });
      } else {
        await this.restartZongji();
      }
    } else if (!this.isTransactionOpen) {
      this.binLogPosition.offset = nextPosition;
      const LSN = new common.ReplicatedGTID({
        raw_gtid: this.currentGTID.raw,
        position: this.binLogPosition
      }).comparable;
      await this.eventHandler.onCommit(LSN);
    }
  }

  /**
   *  Function that interprets a DDL query for any applicable schema changes.
   *  If the query does not contain any relevant schema changes, an empty array is returned.
   *  The defaultSchema is derived from the database set on the MySQL Node.js connection client.
   *  It is used as a fallback when the schema/database cannot be determined from the query DDL.
   *
   *  @param query
   *  @param defaultSchema
   */
  private toSchemaChanges(query: string, defaultSchema: string): SchemaChange[] {
    let statements: AST[] = [];
    try {
      const ast = this.sqlParser.astify(query, { database: 'MySQL' });
      statements = Array.isArray(ast) ? ast : [ast];
    } catch (error) {
      if (matchedSchemaChangeQuery(query, Object.values(this.databaseFilter))) {
        this.logger.warn(
          `Failed to parse query: [${query}]. 
      Please review for the schema changes and manually redeploy the sync rules if required.`
        );
      }
      return [];
    }

    const changes: SchemaChange[] = [];
    for (const statement of statements) {
      if (isTruncate(statement)) {
        const truncateStatement = statement as TruncateStatement;
        // Truncate statements can apply to multiple tables
        for (const entity of truncateStatement.name) {
          changes.push({
            type: SchemaChangeType.TRUNCATE_TABLE,
            table: entity.table,
            schema: entity.db ?? defaultSchema
          });
        }
      } else if (isDropTable(statement)) {
        for (const entity of statement.name) {
          changes.push({ type: SchemaChangeType.DROP_TABLE, table: entity.table, schema: entity.db ?? defaultSchema });
        }
      } else if (isDropIndex(statement)) {
        const dropStatement = statement as DropIndexStatement;
        changes.push({
          type: SchemaChangeType.REPLICATION_IDENTITY,
          table: dropStatement.table.table,
          schema: dropStatement.table.db ?? defaultSchema
        });
      } else if (isCreateUniqueIndex(statement)) {
        // Potential change to the replication identity if the table has no prior unique constraint
        changes.push({
          type: SchemaChangeType.REPLICATION_IDENTITY,
          // @ts-ignore - The type definitions for node-sql-parser do not reflect the correct structure here
          table: statement.table!.table,
          // @ts-ignore
          schema: statement.table!.db ?? defaultSchema
        });
      } else if (isRenameTable(statement)) {
        const renameStatement = statement as RenameStatement;
        // Rename statements can apply to multiple tables
        for (const table of renameStatement.table) {
          const schema = table[0].db ?? defaultSchema;
          const isNewTableIncluded = this.databaseFilter[schema](table[1].table);
          changes.push({
            type: SchemaChangeType.RENAME_TABLE,
            table: table[0].table,
            newTable: isNewTableIncluded ? table[1].table : undefined,
            schema
          });
        }
      } else if (isAlterTable(statement)) {
        const fromTable = statement.table[0] as BaseFrom;
        for (const expression of statement.expr) {
          if (isRenameExpression(expression)) {
            changes.push({
              type: SchemaChangeType.RENAME_TABLE,
              table: fromTable.table,
              newTable: expression.table,
              schema: fromTable.db ?? defaultSchema
            });
          } else if (isColumnExpression(expression)) {
            changes.push({
              type: SchemaChangeType.ALTER_TABLE_COLUMN,
              table: fromTable.table,
              schema: fromTable.db ?? defaultSchema
            });
          } else if (isConstraintExpression(expression)) {
            // Potential changes to the replication identity
            changes.push({
              type: SchemaChangeType.REPLICATION_IDENTITY,
              table: fromTable.table,
              schema: fromTable.db ?? defaultSchema
            });
          }
        }
      }
    }
    // Filter out schema changes that are not relevant to the included tables
    return changes.filter(
      (change) =>
        this.isTableIncluded(change.table, change.schema) ||
        (change.newTable && this.isTableIncluded(change.newTable, change.schema))
    );
  }

  private isTableIncluded(tableName: string, schema: string): boolean {
    return this.databaseFilter[schema] && this.databaseFilter[schema](tableName);
  }

  private createDatabaseFilter(sourceTables: TablePattern[]): { [schema: string]: (table: string) => boolean } {
    // Group sync rule tables by schema
    const schemaMap = new Map<string, TablePattern[]>();
    for (const table of sourceTables) {
      if (!schemaMap.has(table.schema)) {
        const tables = [table];
        schemaMap.set(table.schema, tables);
      } else {
        schemaMap.get(table.schema)!.push(table);
      }
    }

    const databaseFilter: { [schema: string]: (table: string) => boolean } = {};
    for (const entry of schemaMap.entries()) {
      const [schema, sourceTables] = entry;
      databaseFilter[schema] = (table: string) =>
        sourceTables.findIndex((sourceTable) =>
          sourceTable.isWildcard
            ? table.startsWith(sourceTable.tablePattern.substring(0, sourceTable.tablePattern.length - 1))
            : table === sourceTable.name
        ) !== -1;
    }

    return databaseFilter;
  }
}
