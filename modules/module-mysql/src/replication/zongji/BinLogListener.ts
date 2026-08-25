import { Logger, ReplicationAssertionError, logger as defaultLogger } from '@powersync/lib-services-framework';
import {
  BinLogEvent,
  BinLogQueryEvent,
  MySQLConnection,
  StartOptions,
  TableMapEntry,
  ZongJi
} from '@powersync/mysql-zongji';
import { TablePattern } from '@powersync/service-sync-rules';
import async from 'async';
import pkg, {
  AST,
  BaseFrom,
  DropIndexStatement,
  Parser as ParserType,
  RenameStatement,
  TruncateStatement
} from 'node-sql-parser';
import timers from 'timers/promises';
import * as common from '../../common/common-index.js';
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
import { BinlogListenerConnections, MySQLConnectionManager } from '../MySQLConnectionManager.js';
import * as zongji_utils from './zongji-utils.js';

const { Parser } = pkg;

/**
 *  Seconds of inactivity after which a keepalive event is sent by the MySQL server.
 */
export const KEEPALIVE_INACTIVITY_THRESHOLD = 30;

/**
 *  Maximum time in milliseconds to wait for Zongji to stop before force-closing its control connection.
 */
export const ZONGJI_STOP_TIMEOUT = 5_000;

/**
 *  Maximum time in milliseconds a control connection liveness probe may execute before the
 *  connection is considered dead. Time the probe spends queued behind other control queries
 *  does not count towards this.
 */
export const CTRL_CONNECTION_PROBE_TIMEOUT = 5_000;

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
  /**
   *  Id that identifies this replication client.
   */
  serverId: number;
  /**
   *  The server uuid of the source MySQL server that is being replicated.
   */
  activeServerUuid: string;
  startGTID: common.ReplicatedGTID;
  logger?: Logger;
  keepAliveInactivitySeconds?: number;
  ctrlConnectionProbeTimeoutMs?: number;
}

/**
 *  Wrapper class for the Zongji BinLog listener. Internally handles the creation and management of the listener and posts
 *  events on the provided BinLogEventHandler.
 */
export class BinLogListener {
  private sqlParser: ParserType;
  private binLogPosition: common.BinLogPosition;
  private currentGTID: common.ReplicatedGTID;
  private logger: Logger;
  private listenerError: Error | null;
  private databaseFilter: { [schema: string]: (table: string) => boolean };

  private isStopped: boolean = false;
  private isStopping: boolean = false;

  // Set while a control connection probe is awaiting a response, so repeated probes do not pile up behind it.
  private probePending: boolean = false;

  // Flag to indicate if are currently in a transaction that involves multiple row mutation events.
  private isTransactionOpen = false;

  zongji: ZongJi;
  /**
   *  The connection Zongji uses for table metadata queries and its shutdown KILL query. We create
   *  it ourselves so that we keep a handle on it for liveness probes and cleanup: Zongji does not
   *  destroy connections it did not create.
   */
  controlConnection: MySQLConnection;
  processingQueue: async.QueueObject<BinLogEvent>;

  /**
   *  The combined size in bytes of all the binlog events currently in the processing queue.
   */
  queueMemoryUsage: number = 0;

  constructor(public options: BinLogListenerOptions) {
    this.logger = options.logger ?? defaultLogger;
    // Copy the position: the listener mutates it as events are processed, and the caller's startGTID must not change
    this.binLogPosition = { ...options.startGTID.position };
    this.currentGTID = options.startGTID;
    this.sqlParser = new Parser();
    this.processingQueue = this.createProcessingQueue();
    const { zongji, controlConnection } = this.createZongjiListener();
    this.zongji = zongji;
    this.controlConnection = controlConnection;
    this.listenerError = null;
    this.databaseFilter = this.createDatabaseFilter(options.sourceTables);
  }

  private get connectionManager(): MySQLConnectionManager {
    return this.options.connectionManager;
  }

  private get eventHandler(): BinLogEventHandler {
    return this.options.eventHandler;
  }

  private get activeServerUuid(): string {
    return this.options.activeServerUuid;
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
      const { zongji, controlConnection } = this.createZongjiListener();
      this.zongji = zongji;
      this.controlConnection = controlConnection;
      await this.start(true);
    }
  }

  private async stopZongji(): Promise<void> {
    if (!this.zongji.stopped) {
      this.logger.info('Stopping BinLog Listener...');
      let stopped = false;
      const stopPromise = new Promise<void>((resolve) => {
        this.zongji.once('stopped', () => {
          stopped = true;
          resolve();
        });
        this.zongji.stop();
      });
      // Zongji only emits 'stopped' once the KILL query on its control connection has completed.
      // If that connection has been dead for a while, the query can block on TCP retransmissions
      // for many minutes, so we destroy the socket after a timeout to unblock the stop.
      const timeout = timers.setTimeout(ZONGJI_STOP_TIMEOUT, undefined, { ref: false }).then(() => {
        if (!stopped) {
          this.logger.warn('Timed out waiting for the BinLog Listener to stop. Closing the control connection.');
          this.controlConnection._socket?.destroy();
        }
      });
      await Promise.race([stopPromise, timeout]);
      // Zongji does not destroy connections it did not create.
      this.controlConnection.destroy();
      // destroy() drops pending query callbacks, so a probe waiting on this connection would
      // otherwise stay pending forever and disable probing after a restart.
      this.probePending = false;
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

  /**
   *  The binlog connection is kept alive by the MySQL server heartbeat, but the control connection
   *  carries no traffic between metadata queries. TCP keepalive stops it from being dropped when idle,
   *  but detects an already dead connection slowly, so the replication job's keepAlive additionally
   *  probes it with a lightweight query and stops the listener (restarting replication) if the probe
   *  does not respond in time.
   *
   *  The driver starts the query timeout when the query begins executing, not when it is queued, so a
   *  probe waiting behind a legitimately slow metadata query does not produce a false failure. A probe
   *  queued on a dead socket is failed together with the queued query by TCP keepalive on the socket.
   */
  public probeControlConnection(): void {
    if (this.probePending || this.zongji.stopped || this.isStopped || this.isStopping) {
      return;
    }
    this.probePending = true;
    const controlConnection = this.controlConnection;
    const timeout = this.options.ctrlConnectionProbeTimeoutMs ?? CTRL_CONNECTION_PROBE_TIMEOUT;
    controlConnection.query({ sql: 'SELECT 1', timeout }, (error) => {
      this.probePending = false;
      // Only act if the probe failed on a connection that is still supposed to be alive.
      if (
        error != null &&
        this.controlConnection === controlConnection &&
        !this.zongji.stopped &&
        !(this.isStopped || this.isStopping)
      ) {
        this.logger.warn('MySQL control connection is unresponsive. Stopping the BinLog Listener...');
        this.listenerError = new Error('MySQL control connection is unresponsive.');
        controlConnection._socket?.destroy();
        this.stop();
      }
    });
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

  private createZongjiListener(): BinlogListenerConnections {
    const connections = this.connectionManager.createBinlogListener();
    const { zongji } = connections;

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

    return connections;
  }

  isQueueOverCapacity(): boolean {
    return this.queueMemoryUsage >= this.queueMemoryLimit;
  }

  private createQueueWorker() {
    return async (evt: BinLogEvent) => {
      switch (true) {
        case zongji_utils.eventIsGTIDLog(evt):
          const transactionGTID = common.ReplicatedGTID.fromBinLogEvent({
            rawGtid: {
              serverUuid: evt.serverId, // The server uuid this transaction originated from
              transactionId: evt.transactionRange
            },
            position: {
              filename: this.binLogPosition.filename,
              offset: evt.nextPosition
            }
          });

          if (transactionGTID.serverUuid !== this.activeServerUuid) {
            throw new ReplicationAssertionError(
              `Detected a transaction from a different MySQL server UUID: ${transactionGTID.serverUuid} than the server that is currently being replicated from: ${this.activeServerUuid}. ` +
                `A re-snapshot is required to ensure consistency.`
            );
          }

          this.currentGTID = transactionGTID;
          this.binLogPosition.offset = evt.nextPosition;

          await this.eventHandler.onTransactionStart({ timestamp: new Date(evt.timestamp) });
          this.logger.info(`Processed GTID event: ${this.currentGTID.comparable}`);
          break;
        case zongji_utils.eventIsRotation(evt):
          // The first event when starting replication is a synthetic Rotate event
          // It describes the the position and file that the replica requested to start from
          const isNewFile = this.binLogPosition.filename !== evt.binlogName;

          this.binLogPosition.filename = evt.binlogName;
          this.binLogPosition.offset = evt.position;

          await this.eventHandler.onRotate();

          if (isNewFile) {
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
          const LSN = this.advanceCommitPosition(evt.nextPosition);
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

  /**
   *  Advances the binlog position to the end of a committed transaction and updates the currentGTID to match.
   *  This ensures subsequent heartbeat keepalives report an LSN that is not behind the last commit LSN,
   *  which would otherwise block checkpoint creation until the next transaction arrives.
   *  Returns the commit LSN.
   */
  private advanceCommitPosition(nextPosition: number): string {
    this.binLogPosition.offset = nextPosition;
    this.currentGTID = new common.ReplicatedGTID({
      rawGtid: this.currentGTID.raw,
      position: { ...this.binLogPosition }
    });
    return this.currentGTID.comparable;
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
        const LSN = this.advanceCommitPosition(nextPosition);
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
      const LSN = this.advanceCommitPosition(nextPosition);
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
Please review for the schema changes and manually redeploy the sync config if required.`
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
    // Group sync config tables by schema
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
