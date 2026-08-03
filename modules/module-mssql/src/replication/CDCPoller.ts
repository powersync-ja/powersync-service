import {
  DatabaseQueryError,
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import sql from 'mssql';
import timers from 'timers/promises';
import { CaptureInstance } from '../common/CaptureInstance.js';
import { LSN } from '../common/LSN.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { AdditionalConfig } from '../types/types.js';
import { isDeadlockError } from '../utils/deadlock.js';
import { CaptureInstanceDetails, getCaptureInstances, incrementLSN } from '../utils/mssql.js';
import { SourceTableChangeRef, tableExists } from '../utils/schema.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';

enum Operation {
  DELETE = 1,
  INSERT = 2,
  UPDATE_BEFORE = 3,
  UPDATE_AFTER = 4
}

/**
 * Schema changes are detected to warn about, not to act on. Acting on one requires having detected
 * it, and detection is polling - which can never be atomic with a commit. Whatever the interval,
 * there is a window where checkpoints are committed against a schema we believe is current and is
 * not. A checkpoint built that way is a state the source was never in, and clients cannot tell:
 * an absent row looks the same as one that has not arrived. So the schema a stream replicates is
 * fixed at deploy, and every change below either warns and stops replicating that table, or fails
 * the job. A new sync deploy is how any of them is actually adopted.
 *
 * There is no table-create event: wildcard patterns are not supported and every configured table
 * must exist at deploy, so no table can enter scope while a stream is running.
 *
 * Renames and drops retain the table's replicated data rather than deleting it. Both are observed
 * in the catalog rather than in the change stream, so we learn that they happened but not the LSN
 * they happened at, and changes from before that point may still be unreplicated - there is no
 * moment we can identify as safe to delete. We might get away with tracking a barrier LSN, but that also gets complicated.
 * That same missing LSN is why they fail the job: this check runs before polling in a cycle, so
 * carrying on would commit the cycle's end LSN past changes for the departed table that were never
 * read.
 */
export enum SchemaChangeType {
  /**
   * A replicated table was renamed. Fails the job: schema checks run before polling within a cycle,
   * so continuing would commit past changes for this table that were never read. Its replicated data
   * is retained for a redeploy to clean up.
   */
  TABLE_RENAME = 'table_rename',
  /**
   * A replicated table was dropped from the source. Handled the same as a rename.
   */
  TABLE_DROP = 'table_drop',
  /**
   * The source table's columns changed. The pinned capture instance keeps capturing its original
   * schema, so replication continues unchanged and this only warns about the drift.
   */
  TABLE_COLUMN_CHANGES = 'table_column_changes',
  /**
   * A newer capture instance exists while the bound one is still usable. Adopting it would change
   * the captured schema mid-stream, so this only warns.
   */
  NEW_CAPTURE_INSTANCE = 'new_capture_instance',
  /**
   * The capture instance this table is pinned to is gone. The one change that fails the job rather
   * than warning: there is nothing left to poll, so continuing would serve checkpoints that silently
   * omit the table. A dropped capture instance cannot be restored, and a replacement is never
   * adopted in place. When one is available it is carried in `newCaptureInstance`, only so the error
   * can name the right remedy.
   */
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
    // Ensure that the startLSN is not before the minimum LSN for the bound capture instance.
    const boundInstance = table.captureInstance;
    if (boundInstance == null) {
      throw new ReplicationAssertionError(`No capture instance bound for table ${table.toQualifiedName()}`);
    }
    const minLSN = boundInstance.minLSN;
    // TODO(investigate): This seems fishy at first glance. Distinguish a newly snapshotted capture instance (where starting at minLSN is safe)
    // from expired CDC history. If cleanup advanced minLSN past required changes, returning early or
    // clamping startLSN would silently skip updates; that case must trigger a re-snapshot/restart.
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

    // No new-table detection: table patterns are exact names and must exist at deploy, so the
    // replicated set cannot grow mid-stream. See SchemaChangeType.
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

      // Table-cache population ensures every streamed table has one persisted capture-instance
      // binding before polling begins.
      const boundObjectId = table.pinnedCaptureObjectId;
      if (boundObjectId == null) {
        throw new ReplicationAssertionError(`No persisted capture instance for table ${table.toQualifiedName()}`);
      }
      const boundInstance = captureInstanceDetails.instances.find((instance) => instance.objectId === boundObjectId);
      if (boundInstance == null) {
        // The bound capture instance was dropped while a replacement exists. The handler fails the
        // job either way; the replacement is reported only so the error can name the right remedy.
        schemaChanges.push({
          type: SchemaChangeType.MISSING_CAPTURE_INSTANCE,
          table,
          newCaptureInstance: latestCaptureInstance
        });
        continue;
      }
      if (latestCaptureInstance.objectId !== boundObjectId) {
        // A newer capture instance exists. Warning-only - the handler keeps polling the bound
        // instance. Deliberately no `continue`: renames and column changes must still be detected
        // while a newer instance sits there unadopted, which can be indefinitely.
        schemaChanges.push({
          type: SchemaChangeType.NEW_CAPTURE_INSTANCE,
          table,
          newCaptureInstance: latestCaptureInstance
        });
      }

      // One of the replicated tables has been renamed. The new name is reported for the message
      // only - whether it matches the sync configuration no longer changes what happens, since a
      // table can only enter scope at deploy.
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

      // Drift is reported against the bound instance, not the newest one - that is the schema this
      // stream actually replicates, and the two differ whenever a newer instance is sitting
      // unadopted.
      if (boundInstance.pendingSchemaChanges.length > 0) {
        schemaChanges.push({
          type: SchemaChangeType.TABLE_COLUMN_CHANGES,
          table,
          newCaptureInstance: boundInstance
        });
      }
    }

    return schemaChanges;
  }
}
