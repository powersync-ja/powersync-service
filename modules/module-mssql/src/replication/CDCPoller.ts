import {
  DatabaseQueryError,
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import sql from 'mssql';
import timers from 'timers/promises';
import { LSN } from '../common/LSN.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { AdditionalConfig } from '../types/types.js';
import { isDeadlockError } from '../utils/deadlock.js';
import { CaptureInstanceDetails, getCaptureInstances, incrementLSN } from '../utils/mssql.js';
import { tableExists } from '../utils/schema.js';
import { CaptureInstanceMissingError } from './CaptureReconciler.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import { SchemaChange, SchemaChangeType } from './SchemaChange.js';

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

export const DEFAULT_SCHEMA_CHECK_INTERVAL_MS = 60_000;

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
  /** CDC enabled source tables from the sync config to replicate */
  getReplicatedTables: () => MSSQLSourceTable[];
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
 * Polls SQL Server CDC change tables for changes at a configurable interval.
 *
 * Processes changes in commit order, groups CDC rows into logical insert,
 * update, and delete operations. It only commits once all the operations recorded in a polling cycle have been processed.
 * Periodically runs checks to detect schema and capture-instance changes.
 */
export class CDCPoller {
  private connectionManager: MSSQLConnectionManager;
  private eventHandler: CDCEventHandler;
  private currentLSN: LSN;
  private logger: Logger;
  private captureInstances: Map<number, CaptureInstanceDetails>;

  private pollingError: Error | null = null;
  private isStopped: boolean = false;
  private isStopping: boolean = false;
  private isPolling: boolean = false;
  private lastSchemaCheckTime: number = 0;

  constructor(private options: CDCPollerOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.connectionManager = options.connectionManager;
    this.eventHandler = options.eventHandler;
    this.currentLSN = options.startLSN;
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
          this.pollingError = error as Error;
          this.logger.error('Error during CDC polling:', error);
          this.stop();
        }
        break;
      }
    }

    if (this.pollingError) {
      this.logger.error('CDC polling was stopped due to an error:', this.pollingError);
      throw this.pollingError;
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
   *  Processes the changes this table has within the given bounds, and returns the LSNs of the
   *  transactions those changes belong to. The LSNs are returned in their string form so that the
   *  caller can deduplicate them across tables by value.
   */
  private async pollTable(table: MSSQLSourceTable, bounds: { startLSN: LSN; endLSN: LSN }): Promise<Set<string>> {
    const transactionLSNs = new Set<string>();

    // CDC cleanup can advance minLSN while the capture-table identity remains unchanged, so use
    // the latest metadata loaded by the schema check rather than the instance bound at startup.
    const availableInstances = this.captureInstances.get(table.objectId)?.instances ?? [];
    table.setCaptureInstance(availableInstances);
    const boundInstance = table.captureInstance;
    if (boundInstance == null) {
      // The pinned instance can be dropped between schema checks.
      throw new CaptureInstanceMissingError(
        `The CDC capture instance for table ${table.toQualifiedName()} (pinned to object id ` +
          `${table.pinnedCaptureObjectId}) is no longer available. Deploy a new sync config to replicate ` +
          `this table against an available capture instance.`
      );
    }
    const minLSN = boundInstance.minLSN;
    if (minLSN > bounds.endLSN) {
      return transactionLSNs;
    } else if (minLSN >= bounds.startLSN) {
      bounds.startLSN = minLSN;
    }

    try {
      const request = await this.connectionManager.createRequest();
      request.input('from_lsn', sql.VarBinary, bounds.startLSN.toBinary());
      request.input('to_lsn', sql.VarBinary, bounds.endLSN.toBinary());

      let columns: sql.IColumnMetadata | null = null;
      request.on('recordset', (recordsetColumns) => {
        columns = recordsetColumns;
      });
      const stream = request.toReadableStream();
      request.query(`
        SELECT * FROM ${table.allChangesFunction}(@from_lsn, @to_lsn, 'all update old') ORDER BY __$start_lsn, __$seqval, __$operation
      `);

      for await (const { transactionLSN, type, rows } of groupLogicalChanges(stream, table)) {
        if (columns == null) {
          throw new ReplicationAssertionError(
            `Missing CDC column metadata while polling for updates for table ${table.toQualifiedName()}.`
          );
        }
        switch (type) {
          case LogicalChangeType.DELETE:
            await this.eventHandler.onDelete(rows[0], table, columns);
            break;
          case LogicalChangeType.INSERT:
            await this.eventHandler.onInsert(rows[0], table, columns);
            break;
          case LogicalChangeType.UPDATE:
          case LogicalChangeType.DEFERRED_UPDATE:
            const [rowBefore, rowAfter] = rows;
            await this.eventHandler.onUpdate(rowAfter, rowBefore, table, columns);
            break;
        }
        this.logger.info(`Processed ${type}. Transaction LSN: ${transactionLSN}`);

        transactionLSNs.add(transactionLSN.toString());
      }

      return transactionLSNs;
    } catch (error) {
      // This Covers both deleted tables and capture instances. Unlike the check above, this cannot
      // tell the two apart, so it stays recoverable: the forced schema check classifies it as a
      // dropped table or a missing capture instance and fails with the matching error.
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

  private async checkForSchemaChanges(): Promise<SchemaChange[]> {
    const schemaChanges: SchemaChange[] = [];

    // Exact table names keep the replicated set fixed until the next deploy.
    for (const table of this.replicatedTables) {
      const exists = await tableExists(table.objectId, this.connectionManager);
      if (!exists) {
        this.logger.info(`Table ${table.toQualifiedName()} has been dropped.`);
        schemaChanges.push({
          type: SchemaChangeType.TABLE_DROP,
          table
        });
        continue;
      }

      const captureInstanceDetails = this.captureInstances.get(table.objectId);
      if (!captureInstanceDetails) {
        // The table had a capture instance when the stream started, but no longer does.
        schemaChanges.push({
          type: SchemaChangeType.MISSING_CAPTURE_INSTANCE,
          table
        });
        continue;
      }

      const latestCaptureInstance = captureInstanceDetails.instances[0];

      table.setCaptureInstance(captureInstanceDetails.instances);
      const boundInstance = table.captureInstance;
      if (boundInstance == null) {
        // Include the replacement so the error can suggest the right recovery step.
        schemaChanges.push({
          type: SchemaChangeType.MISSING_CAPTURE_INSTANCE,
          table,
          replacementInstance: latestCaptureInstance
        });
        continue;
      }
      if (latestCaptureInstance.objectId !== boundInstance.objectId) {
        // Keep checking for rename and column changes against the pinned instance.
        schemaChanges.push({
          type: SchemaChangeType.NEW_CAPTURE_INSTANCE,
          table,
          newCaptureInstance: latestCaptureInstance
        });
      }

      // The new name is only used in the error message.
      if (table.ref.name !== captureInstanceDetails.sourceTable.name) {
        schemaChanges.push({
          type: SchemaChangeType.TABLE_RENAME,
          table,
          newTable: {
            name: captureInstanceDetails.sourceTable.name,
            schema: captureInstanceDetails.sourceTable.schema,
            objectId: captureInstanceDetails.sourceTable.objectId
          }
        });
        continue;
      }

      // Report drift against the capture instance this stream uses.
      if (boundInstance.pendingSchemaChanges.length > 0) {
        schemaChanges.push({
          type: SchemaChangeType.TABLE_COLUMN_CHANGES,
          table,
          captureInstance: boundInstance
        });
      }
    }

    return schemaChanges;
  }
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
 */
async function* groupLogicalChanges(rows: AsyncIterable<any>, table: MSSQLSourceTable): AsyncGenerator<LogicalChange> {
  interface PendingGroup {
    transactionLSN: Buffer;
    sequence: Buffer;
    rows: any[];
  }

  let current: PendingGroup | null = null;

  for await (const row of rows) {
    const transactionLSN: Buffer = row.__$start_lsn;
    const sequence: Buffer = row.__$seqval;

    if (current && !(transactionLSN.equals(current.transactionLSN) && sequence.equals(current.sequence))) {
      yield toLogicalChange(current.rows, current.transactionLSN, table);
      current = null;
    }
    current ??= {
      transactionLSN,
      sequence,
      rows: []
    };
    current.rows.push(row);
  }

  if (current) {
    yield toLogicalChange(current.rows, current.transactionLSN, table);
  }
}

function toLogicalChange(rows: any[], startLSN: Buffer, table: MSSQLSourceTable): LogicalChange {
  const transactionLSN = LSN.fromBinary(startLSN);
  return {
    transactionLSN,
    type: resolveLogicalChangeType(rows, transactionLSN, table),
    rows: rows
  };
}

function resolveLogicalChangeType(orderedRows: any[], transactionLSN: LSN, table: MSSQLSourceTable): LogicalChangeType {
  // This matches the actual CDC operation codes:https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver17#table-returned
  enum Operation {
    DELETE = 1,
    INSERT = 2,
    UPDATE_BEFORE = 3,
    UPDATE_AFTER = 4
  }

  if (orderedRows.length === 1) {
    const operation = orderedRows[0].__$operation;

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
