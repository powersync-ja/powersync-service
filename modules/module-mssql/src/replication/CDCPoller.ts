import {
  DatabaseQueryError,
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import { TablePattern } from '@powersync/service-sync-rules';
import sql from 'mssql';
import timers from 'timers/promises';
import { CaptureInstance } from '../common/CaptureInstance.js';
import { LSN } from '../common/LSN.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { AdditionalConfig } from '../types/types.js';
import { isDeadlockError } from '../utils/deadlock.js';
import { CaptureInstanceDetails, getCaptureInstances, incrementLSN, toQualifiedTableName } from '../utils/mssql.js';
import { SourceTableChangeRef, tableExists } from '../utils/schema.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';

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
   *  Populated for new tables or renames, but only if the new table matches a sync config source table.
   */
  newTable?: SourceTableChangeRef;

  newCaptureInstance?: CaptureInstance;
}

export interface CDCEventHandler {
  onInsert: (row: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => Promise<void>;
  onUpdate: (rowAfter: any, rowBefore: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => Promise<void>;
  onDelete: (row: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => Promise<void>;
  onCommit: (lsn: string, transactionCount: number) => Promise<void>;
  onSchemaChange: (change: SchemaChange) => Promise<void>;
}

export const DEFAULT_SCHEMA_CHECK_INTERVAL_MS = 60_000;

export interface CDCPollerOptions {
  connectionManager: MSSQLConnectionManager;
  eventHandler: CDCEventHandler;
  /** CDC enabled source tables from the sync config to replicate */
  getReplicatedTables: () => MSSQLSourceTable[];
  /** All table patterns from the sync config. Can contain tables that need to be replicated
   *  but do not yet have CDC enabled
   */
  sourceTables: TablePattern[];
  startLSN: LSN;
  logger?: Logger;
  additionalConfig: AdditionalConfig;
  /**
   * Interval in milliseconds between schema change checks.
   * Schema checks also run immediately after a recoverable error during polling
   * (e.g. a dropped capture instance).
   */
  schemaCheckIntervalMs?: number;
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
  private lastSchemaCheckTime: number = 0;

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

  private get schemaCheckIntervalMs(): number {
    return this.options.schemaCheckIntervalMs ?? DEFAULT_SCHEMA_CHECK_INTERVAL_MS;
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
        if (this.shouldCheckSchema()) {
          this.captureInstances = await getCaptureInstances({ connectionManager: this.connectionManager });
          const schemaChanges = await this.checkForSchemaChanges();
          for (const schemaChange of schemaChanges) {
            await this.eventHandler.onSchemaChange(schemaChange);
          }
          this.lastSchemaCheckTime = Date.now();

          this.logger.debug(
            `Schema change check complete. Schema changes found: ${schemaChanges.map((c) => c.type).join(', ')}`
          );
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
            // Force schema check on next iteration to detect breaking changes
            this.lastSchemaCheckTime = 0;
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

      // We poll for batch size transactions, but these include transactions not applicable to our Source Tables.
      // A single transaction can also span several Source Tables, so collect the distinct transaction LSNs
      // that produced changes rather than counting per table, which would either double count the
      // transactions spanning tables or miss the transactions applicable to only one of them.
      let transactionLSNs = new Set<string>();
      this.logger.debug(
        `Currently replicating tables: ${this.replicatedTables.map((table) => table.toQualifiedName()).join(', ')}`
      );
      for (const table of this.replicatedTables) {
        if (table.enabledForCDC()) {
          const transactions = await this.pollTable(table, { startLSN, endLSN });
          transactions.forEach((t) => transactionLSNs.add(t));
        }
      }
      const transactionCount = transactionLSNs.size;

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

  /**
   *  Emits the changes this table has within the given bounds, and returns the LSNs of the
   *  transactions those changes belong to. The LSNs are returned in their string form so that the
   *  caller can deduplicate them across tables by value.
   */
  private async pollTable(table: MSSQLSourceTable, bounds: { startLSN: LSN; endLSN: LSN }): Promise<Set<string>> {
    const transactionLSNs = new Set<string>();

    // Ensure that the startLSN is not before the minimum LSN for the table
    const minLSN = this.captureInstances.get(table.objectId)!.instances[0].minLSN;
    if (minLSN > bounds.endLSN) {
      return transactionLSNs;
    } else if (minLSN >= bounds.startLSN) {
      bounds.startLSN = minLSN;
    }

    try {
      const { recordset: results } = await this.connectionManager.query(
        `
        SELECT * FROM ${table.allChangesFunction}(@from_lsn, @to_lsn, 'all update old') ORDER BY __$start_lsn, __$seqval, __$operation
    `,
        [
          { name: 'from_lsn', type: sql.VarBinary, value: bounds.startLSN.toBinary() },
          { name: 'to_lsn', type: sql.VarBinary, value: bounds.endLSN.toBinary() }
        ]
      );

      for (const { transactionLSN, type, rows } of groupLogicalChanges(results, table)) {
        switch (type) {
          case LogicalChangeType.DELETE:
            await this.eventHandler.onDelete(rows[0], table, results.columns);
            break;
          case LogicalChangeType.INSERT:
            await this.eventHandler.onInsert(rows[0], table, results.columns);
            break;
          case LogicalChangeType.UPDATE:
          case LogicalChangeType.DEFERRED_UPDATE:
            const [rowBefore, rowAfter] = rows;
            await this.eventHandler.onUpdate(rowAfter, rowBefore, table, results.columns);
            break;
        }
        this.logger.info(`Processed ${type}. Transaction LSN: ${transactionLSN}`);

        transactionLSNs.add(transactionLSN.toString());
      }

      return transactionLSNs;
    } catch (error) {
      // This Covers both deleted tables and capture instances
      if (error.message.includes(`Invalid object name`)) {
        throw new DatabaseQueryError(
          ErrorCode.PSYNC_S1601,
          `Capture instance for table ${table.toQualifiedName()} is no longer available.`,
          error
        );
      }
      throw error;
    }
  }

  private shouldCheckSchema(): boolean {
    return Date.now() - this.lastSchemaCheckTime >= this.schemaCheckIntervalMs;
  }

  /**
   * Checks the given table for pending schema changes that can lead to inconsistencies in the replicated data if not handled.
   * Returns the SchemaChange if any are found, null otherwise.
   */
  private async checkForSchemaChanges(): Promise<SchemaChange[]> {
    const schemaChanges: SchemaChange[] = [];

    const newTables = this.checkForNewTables();
    for (const table of newTables) {
      this.logger.info(
        `New table ${toQualifiedTableName(table.sourceTable.schema, table.sourceTable.name)} matching the sync config has been created. Handling schema change...`
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
        if (table.enabledForCDC()) {
          // Table had a capture instance but no longer does.
          schemaChanges.push({
            type: SchemaChangeType.MISSING_CAPTURE_INSTANCE,
            table
          });
        }
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

      // One of the replicated tables has been renamed
      if (table.ref.name !== captureInstanceDetails.sourceTable.name) {
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

  private checkForNewTables(): CaptureInstanceDetails[] {
    const newTables: CaptureInstanceDetails[] = [];
    for (const [objectId, captureInstanceDetails] of this.captureInstances.entries()) {
      // If a source table is not in the replicated tables array, but a capture instance exists for it, it is potentially a new table to replicate.
      if (!this.replicatedTables.some((table) => table.objectId === objectId)) {
        // Check if the new table matches any of the sync config source tables.
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

enum LogicalChangeType {
  INSERT = 'INSERT',
  DELETE = 'DELETE',
  UPDATE = 'UPDATE',
  DEFERRED_UPDATE = 'DEFERRED UPDATE'
}

/**
 *  One logical change to a single row, made up of the one or two CDC rows that describe it.
 */
interface LogicalChange {
  transactionLSN: LSN;
  type: LogicalChangeType;
  /**
   *  Inserts and Deletes resolve to 1 row.
   *  Updates resolve to 2 rows, the row values before and after the update: [rowBefore, rowAfter].
   */
  rows: any[];
}

/**
 *  Groups CDC change rows into the logical row changes they describe.
 *
 *  SQL Server records a logical change as either one row (a plain insert or delete) or two rows that share
 *  a `__$seqval`. `__$seqval` represents the ordering of the changes to a row within a transaction.
 *  CDC operations that can share a `__$seqval` are:
 *  - The before and after operations of an in-place update
 *  - The delete and insert operations of a deferred update.
 *
 *  This method groups and emits rows in the same transaction based on their `__$seqval`
 *
 *  The source table is only used to identify the table in the errors raised for change rows that
 *  do not describe a valid logical change.
 */
function* groupLogicalChanges(rows: any[], table: MSSQLSourceTable): Generator<LogicalChange> {
  let currentRows: any[] = [];
  let currentTransactionLSN: Buffer | null = null;
  let currentSequence: Buffer | null = null;

  for (const row of rows) {
    const nextTransactionLSN: Buffer = row.__$start_lsn;
    const nextSequence: Buffer = row.__$seqval;

    if (
      currentRows.length > 0 &&
      !(nextTransactionLSN.equals(currentTransactionLSN!) && nextSequence!.equals(currentSequence!))
    ) {
      yield toLogicalChange(currentRows, currentTransactionLSN!, table);
      currentRows = [];
    }
    currentTransactionLSN = nextTransactionLSN;
    currentSequence = nextSequence;
    currentRows.push(row);
  }

  if (currentRows.length > 0) {
    yield toLogicalChange(currentRows, currentTransactionLSN!, table);
  }
}

function toLogicalChange(rows: any[], startLSN: Buffer, table: MSSQLSourceTable): LogicalChange {
  // The rows are ordered by operation in the query, but this is an extra safeguard
  const orderedRows = [...rows].sort((a, b) => a.__$operation - b.__$operation);
  const transactionLSN = LSN.fromBinary(startLSN);
  return {
    transactionLSN,
    type: resolveLogicalChangeType(orderedRows, transactionLSN, table),
    rows: orderedRows
  };
}

function resolveLogicalChangeType(orderedRows: any[], transactionLSN: LSN, table: MSSQLSourceTable): LogicalChangeType {
  if (orderedRows.length === 1) {
    const operation = orderedRows[0].__$operation;
    if (operation === Operation.UPDATE_BEFORE || operation === Operation.UPDATE_AFTER) {
      throw new ReplicationAssertionError(
        `Incomplete update for table ${table.toQualifiedName()} in transaction LSN ${transactionLSN}: an update must have both a before and an after image.`
      );
    }

    if (operation === Operation.INSERT) {
      return LogicalChangeType.INSERT;
    } else if (operation === Operation.DELETE) {
      return LogicalChangeType.DELETE;
    } else {
      throw new ReplicationAssertionError(
        `Unrecognized operation: ${operation} for table ${table.toQualifiedName()} in transaction LSN ${transactionLSN}.`
      );
    }
  } else if (orderedRows.length === 2) {
    const [first, second] = orderedRows;
    if (first.__$operation === Operation.UPDATE_BEFORE && second.__$operation === Operation.UPDATE_AFTER) {
      return LogicalChangeType.UPDATE;
    } else if (first.__$operation === Operation.DELETE && second.__$operation === Operation.INSERT) {
      return LogicalChangeType.DEFERRED_UPDATE;
    }

    throw new ReplicationAssertionError(
      `Unexpected CDC operations [${first.__$operation}, ${second.__$operation}] for a single logical change on table ${table.toQualifiedName()} in transaction LSN ${transactionLSN}.`
    );
  }

  throw new ReplicationAssertionError(
    `Unexpected number of CDC operations [${orderedRows.length}] for a single logical change on table ${table.toQualifiedName()} in transaction LSN ${transactionLSN}.`
  );
}
