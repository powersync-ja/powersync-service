import { Logger, logger as defaultLogger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import timers from 'timers/promises';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { LSN } from '../common/LSN.js';
import sql from 'mssql';
import { getMinLSN, incrementLSN } from '../utils/mssql.js';
import { AdditionalConfig } from '../types/types.js';

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
  logger?: Logger;
  additionalConfig: AdditionalConfig;
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
    return this.options.additionalConfig.pollingBatchSize;
  }

  private get pollingIntervalMs(): number {
    return this.options.additionalConfig.pollingIntervalMs;
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
    this.logger.info(`CDC polling started with interval of ${this.pollingIntervalMs}ms...`);
    this.logger.info(`Polling a maximum of ${this.pollingBatchSize} transactions per polling cycle.`);
    while (!this.isStopped) {
      // Don't poll if already polling (concurrency guard)
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
      // Calculate the LSN bounds for this batch
      // CDC bounds are inclusive, so the new startLSN is the currentLSN incremented by 1
      const startLSN = await incrementLSN(this.currentLSN, this.connectionManager);

      const { recordset: results } = await this.connectionManager.query(
        `SELECT TOP (${this.pollingBatchSize}) start_lsn
          FROM cdc.lsn_time_mapping
          WHERE start_lsn >= @startLSN
          ORDER BY start_lsn ASC
        `,
        [{ name: 'startLSN', type: sql.VarBinary, value: startLSN.toBinary() }]
      );

      // No new LSNs found, no changes to process
      if (results.length === 0) {
        return false;
      }

      // The new endLSN is the largest LSN in the result
      const endLSN = LSN.fromBinary(results[results.length - 1].start_lsn);

      this.logger.info(`Polling bounds are ${startLSN} -> ${endLSN} spanning ${results.length} transaction(s).`);

      let transactionCount = 0;
      for (const table of this.sourceTables) {
        const tableTransactionCount = await this.pollTable(table, { startLSN, endLSN });
        // We poll for batch size transactions, but these include transactions not applicable to our Source Tables.
        // Each Source Table may or may not have transactions that are applicable to it, so just keep track of the highest number of transactions processed for any Source Table.
        if (tableTransactionCount > transactionCount) {
          transactionCount = tableTransactionCount;
        }
      }

      this.logger.info(
        `Processed ${results.length} transaction(s), including ${transactionCount} Source Table transaction(s).`
      );
      // Call eventHandler.onCommit() with toLSN after processing all tables
      await this.eventHandler.onCommit(endLSN.toString(), transactionCount);

      this.currentLSN = endLSN;

      return true;
    } finally {
      // Always clear polling flag, even on error
      this.isPolling = false;
    }
  }

  private async pollTable(table: MSSQLSourceTable, bounds: { startLSN: LSN; endLSN: LSN }): Promise<number> {
    // Ensure that the startLSN is not before the minimum LSN for the table
    const minLSN = await getMinLSN(this.connectionManager, table.captureInstance);
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

    let transactionCount = 0;
    let updateBefore: any = null;
    let lastTransactionLSN: LSN | null = null;
    for (const row of results) {
      const transactionLSN = LSN.fromBinary(row.__$start_lsn);
      switch (row.__$operation) {
        case Operation.DELETE:
          await this.eventHandler.onDelete(row, table, results.columns);
          this.logger.info(`Processed DELETE row LSN: ${transactionLSN}`);
          break;
        case Operation.INSERT:
          await this.eventHandler.onInsert(row, table, results.columns);
          this.logger.info(`Processed INSERT row LSN: ${transactionLSN}`);
          break;
        case Operation.UPDATE_BEFORE:
          updateBefore = row;
          this.logger.debug(`Processed UPDATE, before row LSN: ${transactionLSN}`);
          break;
        case Operation.UPDATE_AFTER:
          if (updateBefore === null) {
            throw new ReplicationAssertionError('Missing before image for update event.');
          }
          await this.eventHandler.onUpdate(row, updateBefore, table, results.columns);
          updateBefore = null;
          this.logger.info(`Processed UPDATE row LSN: ${transactionLSN}`);
          break;
        default:
          this.logger.warn(`Unknown operation type [${row.__$operation}] encountered in CDC changes.`);
      }

      // Increment transaction count when we encounter a new transaction LSN (except for UPDATE_BEFORE rows)
      if (transactionLSN != lastTransactionLSN) {
        lastTransactionLSN = transactionLSN;
        if (row.__$operation !== Operation.UPDATE_BEFORE) {
          transactionCount++;
        }
      }
    }

    return transactionCount;
  }
}
