import { Logger, logger as defaultLogger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import timers from 'timers/promises';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { LSN } from '../common/LSN.js';
import sql from 'mssql';
import { getMinLSN, incrementLSN } from '../utils/mssql.js';

enum Operation {
  DELETE = 1,
  INSERT = 2,
  UPDATE_BEFORE = 3,
  UPDATE_AFTER = 4
}
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

export interface CDCEventHandler {
  onInsert: (row: any, table: MSSQLSourceTable, collumns: sql.IColumnMetadata) => Promise<void>;
  onUpdate: (rowAfter: any, rowBefore: any, table: MSSQLSourceTable, collumns: sql.IColumnMetadata) => Promise<void>;
  onDelete: (row: any, table: MSSQLSourceTable, collumns: sql.IColumnMetadata) => Promise<void>;
  onCommit: (lsn: string, transactionCount: number) => Promise<void>;
  onSchemaChange: (change: SchemaChange) => Promise<void>;
}

export interface CDCPollerOptions {
  connectionManager: MSSQLConnectionManager;
  eventHandler: CDCEventHandler;
  sourceTables: MSSQLSourceTable[];
  startLSN: LSN;
  pollingBatchSize?: number;
  pollingIntervalMs?: number;
  logger?: Logger;
}

/**
 *
 */
export class CDCPoller {
  private connectionManager: MSSQLConnectionManager;
  private eventHandler: CDCEventHandler;
  private currentLSN: LSN;
  private logger: Logger;
  private listenerError: Error | null;

  private isStopped: boolean = false;
  private isStopping: boolean = false;
  private isPolling: boolean = false;

  constructor(public options: CDCPollerOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.connectionManager = options.connectionManager;
    this.eventHandler = options.eventHandler;
    this.currentLSN = options.startLSN;
    this.listenerError = null;
  }

  private get pollingBatchSize(): number {
    return this.options.pollingBatchSize ?? 10;
  }

  private get pollingIntervalMs(): number {
    return this.options.pollingIntervalMs ?? 1000;
  }

  private get sourceTables(): MSSQLSourceTable[] {
    return this.options.sourceTables;
  }

  public async stop(): Promise<void> {
    if (!(this.isStopped || this.isStopping)) {
      this.isStopping = true;
      this.isStopped = true;
    }
  }

  public async replicateUntilStopped(): Promise<void> {
    this.logger.info(`CDC polling started...`);
    while (!this.isStopped) {
      // Skip cycle if already polling (concurrency guard)
      if (this.isPolling) {
        await timers.setTimeout(this.pollingIntervalMs);
        continue;
      }

      try {
        const hasChanges = await this.poll();
        if (!hasChanges) {
          // No changes found, wait before next poll
          await timers.setTimeout(this.pollingIntervalMs);
        }
        // If changes were found, poll immediately again (no wait)
      } catch (error) {
        if (!(this.isStopped || this.isStopping)) {
          this.listenerError = error as Error;
          this.logger.error('Error during CDC polling:', error);
          this.stop();
        }
        break;
      }
    }

    if (this.listenerError) {
      this.logger.error('CDC polling was stopped due to an error:', this.listenerError);
      throw this.listenerError;
    }

    this.logger.info(`CDC polling stopped...`);
  }

  private async poll(): Promise<boolean> {
    // Set polling flag to prevent concurrent polling cycles
    this.isPolling = true;

    try {
      // Calculate the polling LSN bounds for this batch
      const startLSN = await incrementLSN(this.currentLSN, this.connectionManager);

      const { recordset: results } = await this.connectionManager.query(
        `SELECT TOP (${this.pollingBatchSize}) start_lsn
          FROM cdc.lsn_time_mapping
          WHERE start_lsn >= @startLSN
          ORDER BY start_lsn ASC
        `,
        [{ name: 'startLSN', type: sql.VarBinary, value: startLSN.toBinary() }]
      );

      // Handle case where no results returned (no new changes available)
      if (results.length === 0) {
        return false;
      }

      const endLSN = LSN.fromBinary(results[results.length - 1].start_lsn);

      // If startLSN is greater than or equal to endLSN, no new changes are available
      if (startLSN.compare(endLSN) >= 0) {
        return false;
      }

      this.logger.info(`Polling bounds are ${startLSN} -> ${endLSN}. Total potential transactions: ${results.length}`);

      // Poll each source table using existing pollTable() method
      let transactionCount = 0;
      for (const table of this.sourceTables) {
        const tableTransactionCount = await this.pollTable(table, { startLSN, endLSN });
        // We poll for batch size transactions, but these include transactions not applicable to our Source Tables.
        // Each Source Table may or may not have transactions that are applicable to it, so just keep track of the highest number of transactions processedfor any Source Table.
        if (tableTransactionCount > transactionCount) {
          transactionCount = tableTransactionCount;
        }
      }

      // Call eventHandler.onCommit() with toLSN after processing all tables
      await this.eventHandler.onCommit(endLSN.toString(), transactionCount);

      // Update currentLSN to toLSN
      this.currentLSN = endLSN;
      this.logger.info(`Source Table transactions processed: ${transactionCount}.`);

      return true;
    } finally {
      // Always clear polling flag, even on error
      this.isPolling = false;
    }
  }

  private async pollTable(table: MSSQLSourceTable, bounds: { startLSN: LSN; endLSN: LSN }): Promise<number> {
    // Check that the minimum LSN is within the bounds
    const minLSN = await getMinLSN(this.connectionManager, table);
    if (minLSN > bounds.endLSN) {
      return 0;
    } else if (minLSN >= bounds.startLSN) {
      bounds.startLSN = minLSN;
    }
    const { recordset: results } = await this.connectionManager.query(
      `
        SELECT * FROM ${table.allChangesFunction}(@from_lsn, @to_lsn, 'all update old') ORDER BY __$start_lsn, __$seqval
    `,
      [
        { name: 'from_lsn', type: sql.VarBinary, value: bounds.startLSN.toBinary() },
        { name: 'to_lsn', type: sql.VarBinary, value: bounds.endLSN.toBinary() }
      ]
    );

    for (const row of results) {
      const transactionLSN = LSN.fromBinary(row.__$start_lsn);
      let updateBefore: any = null;
      switch (row.__$operation) {
        case Operation.DELETE:
          await this.eventHandler.onDelete(row, table, results.columns);
          this.logger.info(`Processed DELETE row: ${transactionLSN}`);
          break;
        case Operation.INSERT:
          await this.eventHandler.onInsert(row, table, results.columns);
          this.logger.info(`Processed INSERT row: ${transactionLSN}`);
          break;
        case Operation.UPDATE_BEFORE:
          updateBefore = row;
          this.logger.info(`Processed UPDATE, before row: ${transactionLSN}`);
          break;
        case Operation.UPDATE_AFTER:
          if (updateBefore === null) {
            throw new ReplicationAssertionError('Missing before image for update event.');
          }
          await this.eventHandler.onUpdate(row, updateBefore, table, results.columns);
          this.logger.info(`Processed UPDATE, after row: ${transactionLSN}`);
          break;
        default:
          this.logger.warn(`Unknown operation type [${row.__$operation}] encountered in CDC changes.`);
      }
    }

    return results.length;
  }
}
