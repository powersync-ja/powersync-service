import {
  DatabaseQueryError,
  ErrorCode,
  Logger,
  logger as defaultLogger,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import timers from 'timers/promises';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { LSN } from '../common/LSN.js';
import sql from 'mssql';
import { CaptureInstanceDetails, getCaptureInstances, incrementLSN, toQualifiedTableName } from '../utils/mssql.js';
import { isDeadlockError } from '../utils/deadlock.js';
import { AdditionalConfig } from '../types/types.js';
import { getPendingSchemaChanges, tableExists } from '../utils/schema.js';
import { TablePattern } from '@powersync/service-sync-rules';
import { CaptureInstance } from '../common/CaptureInstance.js';
import { SourceEntityDescriptor } from '@powersync/service-core';

enum Operation {
  DELETE = 1,
  INSERT = 2,
  UPDATE_BEFORE = 3,
  UPDATE_AFTER = 4
}

export enum SchemaChangeType {
  TABLE_RENAME = 'table_rename',
  TABLE_DROP = 'table_drop',
  TABLE_CREATE = 'table_create',
  TABLE_COLUMN_CHANGES = 'table_column_changes',
  NEW_CAPTURE_INSTANCE = 'new_capture_instance',
  MISSING_CAPTURE_INSTANCE = 'missing_capture_instance'
}

export interface SchemaChange {
  type: SchemaChangeType;
  /**
   *  The table that the schema change applies to. Populated for table drops, renames, new capture instances, and DDL changes.
   */
  table?: MSSQLSourceTable;
  /**
   *  Populated for new tables or renames, but only if the new table matches a sync rule source table.
   */
  newTable?: Omit<SourceEntityDescriptor, 'replicaIdColumns'>;

  newCaptureInstance?: CaptureInstance;
}

export interface CDCEventHandler {
  onInsert: (row: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => Promise<void>;
  onUpdate: (rowAfter: any, rowBefore: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => Promise<void>;
  onDelete: (row: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => Promise<void>;
  onCommit: (lsn: string, transactionCount: number) => Promise<void>;
  onSchemaChange: (change: SchemaChange) => Promise<void>;
}

export interface CDCPollerOptions {
  connectionManager: MSSQLConnectionManager;
  eventHandler: CDCEventHandler;
  /** CDC enabled source tables from the sync rules to replicate */
  getReplicatedTables: () => MSSQLSourceTable[];
  /** All table patterns from the sync rules. Can contain tables that need to be replicated
   *  but do not yet have CDC enabled
   */
  sourceTables: TablePattern[];
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
  private captureInstances: Map<number, CaptureInstanceDetails>;

  private isStopped: boolean = false;
  private isStopping: boolean = false;
  private isPolling: boolean = false;

  constructor(public options: CDCPollerOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.connectionManager = options.connectionManager;
    this.eventHandler = options.eventHandler;
    this.currentLSN = options.startLSN;
    this.listenerError = null;
    this.captureInstances = new Map<number, CaptureInstanceDetails>();
  }

  private get pollingBatchSize(): number {
    return this.options.additionalConfig.pollingBatchSize;
  }

  private get pollingIntervalMs(): number {
    return this.options.additionalConfig.pollingIntervalMs;
  }

  private get replicatedTables(): MSSQLSourceTable[] {
    return this.options.getReplicatedTables();
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
        throw new ReplicationAssertionError('A polling cycle is already in progress.');
      }

      try {
        // Refresh capture instance details
        this.captureInstances = await getCaptureInstances({ connectionManager: this.connectionManager });
        // Check and handle any schema changes before polling
        const schemaChanges = await this.checkForSchemaChanges();
        for (const schemaChange of schemaChanges) {
          await this.eventHandler.onSchemaChange(schemaChange);
        }

        const hasChanges = await this.poll();
        if (!hasChanges) {
          // No changes found, wait before polling again
          await timers.setTimeout(this.pollingIntervalMs);
        }

        // If changes were found, poll immediately again (no wait)
      } catch (error) {
        if (!(this.isStopped || this.isStopping)) {
          // Recoverable errors
          if (error instanceof DatabaseQueryError) {
            this.logger.warn(error.message);
            continue;
          }
          // Deadlock errors are transient — even if all retries within retryOnDeadlock were
          // exhausted, we should not crash the poller. Instead, log and retry the entire cycle.
          if (isDeadlockError(error)) {
            this.logger.warn(
              `Deadlock persisted after all retry attempts during CDC polling cycle. Will retry on next cycle: ${(error as Error).message}`
            );
            continue;
          }

          // Non-recoverable errors
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
      this.logger.debug(
        `Currently replicating tables: ${this.replicatedTables.map((table) => table.toQualifiedName()).join(', ')}`
      );
      for (const table of this.replicatedTables) {
        if (table.enabledForCDC()) {
          const tableTransactionCount = await this.pollTable(table, { startLSN, endLSN });
          // We poll for batch size transactions, but these include transactions not applicable to our Source Tables.
          // Each Source Table may or may not have transactions that are applicable to it, so just keep track of the highest number of transactions processed for any Source Table.
          if (tableTransactionCount > transactionCount) {
            transactionCount = tableTransactionCount;
          }
        }
      }

      this.logger.info(
        `Processed ${results.length} transaction(s), including ${transactionCount} Source Table transaction(s). Commited LSN: ${endLSN.toString()}`
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
    const minLSN = this.captureInstances.get(table.objectId)!.instances[0].minLSN;
    if (minLSN > bounds.endLSN) {
      return 0;
    } else if (minLSN >= bounds.startLSN) {
      bounds.startLSN = minLSN;
    }

    try {
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
    } catch (error) {
      this.logger.error(`Error polling table ${table.toQualifiedName()}:`, error);
      if (error.message.includes(`Invalid object name '${table.allChangesFunction}'`)) {
        throw new DatabaseQueryError(
          ErrorCode.PSYNC_S1601,
          `Capture instance for table ${table.toQualifiedName()} has been dropped during a polling cycle.`,
          error
        );
      }
      throw error;
    }
  }

  /**
   * Checks the given table for pending schema changes that can lead to inconsistencies in the replicated data if not handled.
   * Returns the SchemaChange if any are found, null otherwise.
   */
  private async checkForSchemaChanges(): Promise<SchemaChange[]> {
    const schemaChanges: SchemaChange[] = [];

    const newTables = await this.checkForNewTables();
    for (const table of newTables) {
      this.logger.info(
        `New table ${toQualifiedTableName(table.sourceTable.schema, table.sourceTable.name)} matching the sync rules has been created. Handling schema change...`
      );
      schemaChanges.push({
        type: SchemaChangeType.TABLE_CREATE,
        newTable: {
          name: table.sourceTable.name,
          schema: table.sourceTable.schema,
          objectId: table.sourceTable.objectId
        },
        newCaptureInstance: table.instances[0]
      });
    }

    for (const table of this.replicatedTables) {
      const exists = await tableExists(table.objectId, this.connectionManager);
      if (!exists) {
        this.logger.info(`Table ${table.toQualifiedName()} has been dropped. Handling schema change...`);
        schemaChanges.push({
          type: SchemaChangeType.TABLE_DROP,
          table
        });
        continue;
      }

      const captureInstanceDetails = this.captureInstances.get(table.objectId);
      if (!captureInstanceDetails) {
        // Table had a capture instance but no longer does.
        schemaChanges.push({
          type: SchemaChangeType.MISSING_CAPTURE_INSTANCE,
          table
        });

        continue;
      }

      const latestCaptureInstance = captureInstanceDetails.instances[0];
      // If the table is not enabled for CDC or the capture instance is different, we need to re-snapshot the source table
      if (!table.enabledForCDC() || table.captureInstance!.objectId !== latestCaptureInstance.objectId) {
        schemaChanges.push({
          type: SchemaChangeType.NEW_CAPTURE_INSTANCE,
          table,
          newCaptureInstance: latestCaptureInstance
        });
        continue;
      }

      // The table has been renamed.
      if (table.sourceTable.name !== captureInstanceDetails.sourceTable.name) {
        this.logger.info(
          `Table ${table.sourceTable.name} has been renamed to ${captureInstanceDetails.sourceTable.name}. Handling schema change...`
        );
        const newTable = this.tableMatchesSyncRules(
          captureInstanceDetails.sourceTable.schema,
          captureInstanceDetails.sourceTable.name
        )
          ? {
              name: captureInstanceDetails.sourceTable.name,
              schema: captureInstanceDetails.sourceTable.schema,
              objectId: captureInstanceDetails.sourceTable.objectId
            }
          : undefined;

        schemaChanges.push({
          type: SchemaChangeType.TABLE_RENAME,
          table,
          newTable,
          newCaptureInstance: latestCaptureInstance
        });
        continue;
      }

      latestCaptureInstance.pendingSchemaChanges = await getPendingSchemaChanges({
        connectionManager: this.connectionManager,
        captureInstance: latestCaptureInstance
      });

      if (latestCaptureInstance.pendingSchemaChanges.length > 0) {
        schemaChanges.push({
          type: SchemaChangeType.TABLE_COLUMN_CHANGES,
          table,
          newCaptureInstance: latestCaptureInstance
        });
      }
    }

    return schemaChanges;
  }

  private async checkForNewTables(): Promise<CaptureInstanceDetails[]> {
    const newTables: CaptureInstanceDetails[] = [];
    for (const [objectId, captureInstanceDetails] of this.captureInstances.entries()) {
      // If a source table is not in the replicated tables array, but a capture instance exists for it, is is potentially a new table to replicate.
      if (!this.replicatedTables.some((table) => table.objectId === objectId)) {
        // Check if the new table matches any of the sync rules source tables.
        if (
          this.tableMatchesSyncRules(captureInstanceDetails.sourceTable.schema, captureInstanceDetails.sourceTable.name)
        ) {
          newTables.push(captureInstanceDetails);
        }
      }
    }

    return newTables;
  }

  private tableMatchesSyncRules(schema: string, tableName: string): boolean {
    return this.options.sourceTables.some((tablePattern) =>
      tablePattern.matches({
        connectionTag: this.connectionManager.connectionTag,
        schema: schema,
        name: tableName
      })
    );
  }
}
