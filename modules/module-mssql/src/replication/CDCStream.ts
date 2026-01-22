import {
  container,
  DatabaseConnectionError,
  ErrorCode,
  Logger,
  logger as defaultLogger,
  ReplicationAbortedError,
  ReplicationAssertionError,
  ServiceAssertionError
} from '@powersync/lib-services-framework';
import { getUuidReplicaIdentityBson, MetricsEngine, SourceEntityDescriptor, storage } from '@powersync/service-core';

import {
  SqliteInputRow,
  SqliteRow,
  SqlSyncRules,
  HydratedSyncRules,
  TablePattern
} from '@powersync/service-sync-rules';

import { ReplicationMetric } from '@powersync/service-types';
import { BatchedSnapshotQuery, MSSQLSnapshotQuery, SimpleSnapshotQuery } from './MSSQLSnapshotQuery.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import { getReplicationIdentityColumns, getTablesFromPattern, ResolvedTable } from '../utils/schema.js';
import {
  checkSourceConfiguration,
  createCheckpoint,
  getCaptureInstance,
  getLatestLSN,
  getLatestReplicatedLSN,
  isIColumnMetadata,
  isTableEnabledForCDC,
  isWithinRetentionThreshold,
  toQualifiedTableName
} from '../utils/mssql.js';
import sql from 'mssql';
import { CDCToSqliteRow, toSqliteInputRow } from '../common/mssqls-to-sqlite.js';
import { LSN } from '../common/LSN.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { MSSQLSourceTableCache } from '../common/MSSQLSourceTableCache.js';
import { CDCEventHandler, CDCPoller } from './CDCPoller.js';
import { AdditionalConfig } from '../types/types.js';

export interface CDCStreamOptions {
  connections: MSSQLConnectionManager;
  storage: storage.SyncRulesBucketStorage;
  metrics: MetricsEngine;
  abortSignal: AbortSignal;
  logger?: Logger;
  /**
   * Override snapshot batch size for testing.
   * Defaults to 10_000.
   * Note that queries are streamed, so we don't keep that much data in memory.
   */
  snapshotBatchSize?: number;

  additionalConfig: AdditionalConfig;
}

export enum SnapshotStatus {
  IN_PROGRESS = 'in-progress',
  DONE = 'done',
  RESTART_REQUIRED = 'restart-required'
}

export interface SnapshotStatusResult {
  status: SnapshotStatus;
  snapshotLSN: string | null;
}

export class CDCConfigurationError extends Error {
  constructor(message: string) {
    super(message);
  }
}

/**
 * Thrown when required updates in the CDC instance tables are no longer available
 *
 * Possible reasons:
 *  * Older data has been cleaned up due to exceeding the retention period.
 *    This can happen if PowerSync was stopped for a long period of time.
 */
export class CDCDataExpiredError extends DatabaseConnectionError {
  constructor(message: string, cause: any) {
    super(ErrorCode.PSYNC_S1500, message, cause);
  }
}

export class CDCStream {
  private readonly syncRules: HydratedSyncRules;
  private readonly storage: storage.SyncRulesBucketStorage;
  private readonly connections: MSSQLConnectionManager;
  private readonly abortSignal: AbortSignal;
  private readonly logger: Logger;

  private tableCache = new MSSQLSourceTableCache();

  /**
   * Time of the oldest uncommitted change, according to the source db.
   * This is used to determine the replication lag.
   */
  private oldestUncommittedChange: Date | null = null;
  /**
   * Keep track of whether we have done a commit or keepalive yet.
   * We can only compute replication lag if isStartingReplication == false, or oldestUncommittedChange is present.
   */
  public isStartingReplication = true;

  constructor(private options: CDCStreamOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.storage = options.storage;
    this.syncRules = options.storage.getHydratedSyncRules({ defaultSchema: options.connections.schema });
    this.connections = options.connections;
    this.abortSignal = options.abortSignal;
  }

  private get metrics() {
    return this.options.metrics;
  }

  get stopped() {
    return this.abortSignal.aborted;
  }

  get defaultSchema() {
    return this.connections.schema;
  }

  get groupId() {
    return this.options.storage.group_id;
  }

  get connectionId() {
    const { connectionId } = this.connections;
    // Default to 1 if not set
    if (!connectionId) {
      return 1;
    }
    /**
     * This is often `"default"` (string) which will parse to `NaN`
     */
    const parsed = Number.parseInt(connectionId);
    if (isNaN(parsed)) {
      return 1;
    }
    return parsed;
  }

  get connectionTag() {
    return this.connections.connectionTag;
  }

  get snapshotBatchSize() {
    return this.options.snapshotBatchSize ?? 10_000;
  }

  async replicate() {
    try {
      await this.initReplication();
      await this.streamChanges();
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  async populateTableCache() {
    const sourceTables = this.syncRules.getSourceTables();
    await using writer = await this.storage.createWriter({
      logger: this.logger,
      zeroLSN: LSN.ZERO,
      defaultSchema: this.defaultSchema,
      storeCurrentData: true
    });

    for (let tablePattern of sourceTables) {
      const tables = await this.getQualifiedTableNames(writer, tablePattern);
      for (const table of tables) {
        this.tableCache.set(table);
      }
    }
  }

  async getQualifiedTableNames(
    writer: storage.BucketDataWriter,
    tablePattern: TablePattern
  ): Promise<MSSQLSourceTable[]> {
    if (tablePattern.connectionTag != this.connections.connectionTag) {
      return [];
    }

    const matchedTables: ResolvedTable[] = await getTablesFromPattern(this.connections, tablePattern);

    const tables: MSSQLSourceTable[] = [];
    for (const matchedTable of matchedTables) {
      const isEnabled = await isTableEnabledForCDC({
        connectionManager: this.connections,
        table: matchedTable.name,
        schema: matchedTable.schema
      });

      if (!isEnabled) {
        this.logger.info(`Skipping ${matchedTable.schema}.${matchedTable.name} - table is not enabled for CDC.`);
        continue;
      }

      // TODO: Check RLS settings for table

      const replicaIdColumns = await getReplicationIdentityColumns({
        connectionManager: this.connections,
        tableName: matchedTable.name,
        schema: matchedTable.schema
      });

      const tables = await this.processTable(
        writer,
        {
          name: matchedTable.name,
          schema: matchedTable.schema,
          objectId: matchedTable.objectId,
          replicaIdColumns: replicaIdColumns.columns
        },
        false,
        tablePattern
      );

      tables.push(...tables);
    }
    return tables;
  }

  async processTable(
    writer: storage.BucketDataWriter,
    table: SourceEntityDescriptor,
    snapshot: boolean,
    pattern: TablePattern
  ): Promise<MSSQLSourceTable[]> {
    if (!table.objectId && typeof table.objectId != 'number') {
      throw new ReplicationAssertionError(`objectId expected, got ${typeof table.objectId}`);
    }

    const resolved = await writer.resolveTables({
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: table,
      pattern
    });

    // Drop conflicting tables. This includes for example renamed tables.
    await writer.drop(resolved.dropTables);

    let resultingTables: MSSQLSourceTable[] = [];

    for (let table of resolved.tables) {
      const captureInstance = await getCaptureInstance({
        connectionManager: this.connections,
        tableName: table.name,
        schema: table.schema
      });
      if (!captureInstance) {
        throw new ServiceAssertionError(
          `Missing capture instance for table ${toQualifiedTableName(table.schema, table.name)}`
        );
      }
      const resolvedTable = new MSSQLSourceTable({
        sourceTable: table,
        captureInstance: captureInstance
      });

      // Snapshot if:
      // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
      // 2. Snapshot is not already done, AND:
      // 3. The table is used in sync rules.
      const shouldSnapshot = snapshot && !table.snapshotComplete && table.syncAny;

      if (shouldSnapshot) {
        // Truncate this table in case a previous snapshot was interrupted.
        await writer.truncate([table]);

        // Start the snapshot inside a transaction.
        try {
          await this.snapshotTableInTx(writer, resolvedTable);
        } finally {
          // TODO Cleanup?
        }
      }

      resultingTables.push(resolvedTable);
    }

    return resultingTables;
  }

  private async snapshotTableInTx(writer: storage.BucketDataWriter, table: MSSQLSourceTable): Promise<void> {
    // Note: We use the "Read Committed" isolation level here, not snapshot isolation.
    // The data may change during the transaction, but that is compensated for in the streaming
    // replication afterward.
    const transaction = await this.connections.createTransaction();
    await transaction.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);
    try {
      await this.snapshotTable(writer, transaction, table);

      // Get the current LSN.
      // The data will only be consistent once incremental replication has passed that point.
      // We have to get this LSN _after_ we have finished the table snapshot.
      //
      // There are basically two relevant LSNs here:
      // A: PreSnapshot: The LSN before the snapshot starts.
      // B: PostSnapshot: The LSN after the table snapshot is complete, which is what we get here.
      // When we do the snapshot queries, the data that we get back for each batch could match the state
      // anywhere between A and B. To actually have a consistent state on our side, we need to:
      // 1. Complete the snapshot.
      // 2. Wait until logical replication has caught up with all the changes between A and B.
      // Calling `markSnapshotDone(LSN B)` covers that.
      const postSnapshotLSN = await getLatestLSN(this.connections);
      // Side note: A ROLLBACK would probably also be fine here, since we only read in this transaction.
      await transaction.commit();
      const [updatedSourceTable] = await writer.markTableSnapshotDone([table.sourceTable], postSnapshotLSN.toString());
      this.tableCache.updateSourceTable(updatedSourceTable);
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }

  private async snapshotTable(writer: storage.BucketDataWriter, transaction: sql.Transaction, table: MSSQLSourceTable) {
    let totalEstimatedCount = table.sourceTable.snapshotStatus?.totalEstimatedCount;
    let replicatedCount = table.sourceTable.snapshotStatus?.replicatedCount ?? 0;
    let lastCountTime = 0;
    let query: MSSQLSnapshotQuery;
    // We do streaming on two levels:
    // 1. Coarse select from the entire table, stream rows 1 by one
    // 2. Fine level: Stream batches of rows with each fetch call
    if (BatchedSnapshotQuery.supports(table)) {
      // Single primary key - we can use the primary key for chunking
      const orderByKey = table.sourceTable.replicaIdColumns[0];
      query = new BatchedSnapshotQuery(
        transaction,
        table,
        this.snapshotBatchSize,
        table.sourceTable.snapshotStatus?.lastKey ?? null
      );
      if (table.sourceTable.snapshotStatus?.lastKey != null) {
        this.logger.info(
          `Replicating ${table.toQualifiedName()} ${table.sourceTable.formatSnapshotProgress()} - resuming from ${orderByKey.name} > ${(query as BatchedSnapshotQuery).lastKey}`
        );
      } else {
        this.logger.info(
          `Replicating ${table.toQualifiedName()} ${table.sourceTable.formatSnapshotProgress()} - resumable`
        );
      }
    } else {
      // Fallback case - query the entire table
      this.logger.info(
        `Replicating ${table.toQualifiedName()} ${table.sourceTable.formatSnapshotProgress()} - not resumable`
      );
      query = new SimpleSnapshotQuery(transaction, table);
      replicatedCount = 0;
    }
    await query.initialize();

    let hasRemainingData = true;
    while (hasRemainingData) {
      // Fetch 10k at a time.
      // The balance here is between latency overhead per FETCH call,
      // and not spending too much time on each FETCH call.
      // We aim for a couple of seconds on each FETCH call.
      let batchReplicatedCount = 0;
      let columns: sql.IColumnMetadata | null = null;
      const cursor = query.next();
      for await (const result of cursor) {
        if (columns == null && isIColumnMetadata(result)) {
          columns = result;
          continue;
        } else {
          if (!columns) {
            throw new ReplicationAssertionError(`Missing column metadata`);
          }
          const inputRow: SqliteInputRow = toSqliteInputRow(result, columns);
          const row = this.syncRules.applyRowContext<never>(inputRow);
          // This auto-flushes when the batch reaches its size limit
          await writer.save({
            tag: storage.SaveOperationTag.INSERT,
            sourceTable: table.sourceTable,
            before: undefined,
            beforeReplicaId: undefined,
            after: row,
            afterReplicaId: getUuidReplicaIdentityBson(row, table.sourceTable.replicaIdColumns)
          });

          replicatedCount++;
          batchReplicatedCount++;
          this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        }

        this.touch();
      }

      // Important: flush before marking progress
      await writer.flush();

      let lastKey: Uint8Array | undefined;
      if (query instanceof BatchedSnapshotQuery) {
        lastKey = query.getLastKeySerialized();
      }
      if (lastCountTime < performance.now() - 10 * 60 * 1000) {
        // Even though we're doing the snapshot inside a transaction, the transaction uses
        // the default "Read Committed" isolation level. This means we can get new data
        // within the transaction, so we re-estimate the count every 10 minutes when replicating
        // large tables.
        totalEstimatedCount = await this.estimatedCountNumber(table, transaction);
        lastCountTime = performance.now();
      }
      const updatedSourceTable = await writer.updateTableProgress(table.sourceTable, {
        lastKey: lastKey,
        replicatedCount: replicatedCount,
        totalEstimatedCount: totalEstimatedCount
      });
      this.tableCache.updateSourceTable(updatedSourceTable);

      this.logger.info(`Replicating ${table.toQualifiedName()} ${table.sourceTable.formatSnapshotProgress()}`);

      if (this.abortSignal.aborted) {
        // We only abort after flushing
        throw new ReplicationAbortedError(`Initial replication interrupted`);
      }

      // When the batch of rows is smaller than the requested batch size we know it is the final batch
      if (batchReplicatedCount < this.snapshotBatchSize) {
        hasRemainingData = false;
      }
    }
  }

  /**
   *  Estimate the number of rows in a table. This query uses partition stats view to get a fast estimate of the row count.
   *  This requires that the MSSQL DB user has the VIEW DATABASE PERFORMANCE STATE permission.
   * @param table
   * @param transaction
   */
  async estimatedCountNumber(table: MSSQLSourceTable, transaction?: sql.Transaction): Promise<number> {
    const request = transaction ? transaction.request() : await this.connections.createRequest();
    const { recordset: result } = await request.input('tableName', table.toQualifiedName()).query(
      `SELECT SUM(row_count) AS total_rows
       FROM sys.dm_db_partition_stats
       WHERE object_id = OBJECT_ID(@tableName)
       AND index_id < 2;`
    );
    return result[0].total_rows ?? -1;
  }

  /**
   * Start initial replication.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async startInitialReplication(snapshotStatus: SnapshotStatusResult) {
    let { status, snapshotLSN } = snapshotStatus;

    if (status === SnapshotStatus.RESTART_REQUIRED) {
      this.logger.info(`Snapshot restart required, clearing state.`);
      // This happens if the last replicated checkpoint LSN is no longer available in the CDC tables.
      await this.storage.clear({ signal: this.abortSignal });
    }

    await using writer = await await this.storage.createWriter({
      logger: this.logger,
      zeroLSN: LSN.ZERO,
      defaultSchema: this.defaultSchema,
      storeCurrentData: false,
      skipExistingRows: true
    });
    if (snapshotLSN == null) {
      // First replication attempt - set the snapshot LSN to the current LSN before starting
      snapshotLSN = (await getLatestReplicatedLSN(this.connections)).toString();
      await writer.setResumeLsn(snapshotLSN);
      const latestLSN = (await getLatestLSN(this.connections)).toString();
      this.logger.info(`Marking snapshot at ${snapshotLSN}, Latest DB LSN ${latestLSN}.`);
    } else {
      this.logger.info(`Resuming snapshot at ${snapshotLSN}.`);
    }

    const tablesToSnapshot: MSSQLSourceTable[] = [];
    for (const table of this.tableCache.getAll()) {
      if (table.sourceTable.snapshotComplete) {
        this.logger.info(`Skipping table [${table.toQualifiedName()}] - snapshot already done.`);
        continue;
      }

      const count = await this.estimatedCountNumber(table);
      const updatedSourceTable = await writer.updateTableProgress(table.sourceTable, {
        totalEstimatedCount: count
      });
      this.tableCache.updateSourceTable(updatedSourceTable);
      tablesToSnapshot.push(table);

      this.logger.info(`To replicate: ${table.toQualifiedName()} ${table.sourceTable.formatSnapshotProgress()}`);
    }

    for (const table of tablesToSnapshot) {
      await this.snapshotTableInTx(writer, table);
      this.touch();
    }

    // This will not create a consistent checkpoint yet, but will persist the op.
    // Actual checkpoint will be created when streaming replication caught up.
    const postSnapshotLSN = await getLatestLSN(this.connections);
    await writer.markAllSnapshotDone(postSnapshotLSN.toString());
    await writer.commit(snapshotLSN);

    this.logger.info(`Snapshot done. Need to replicate from ${snapshotLSN} to ${postSnapshotLSN} to be consistent`);
  }

  async initReplication() {
    const errors = await checkSourceConfiguration(this.connections);
    if (errors.length > 0) {
      throw new CDCConfigurationError(`CDC Configuration Errors: ${errors.join(', ')}`);
    }

    await this.populateTableCache();
    const snapshotStatus = await this.checkSnapshotStatus();
    if (snapshotStatus.status !== SnapshotStatus.DONE) {
      await this.startInitialReplication(snapshotStatus);
    }
  }

  /**
   * Checks if the initial sync has already been completed and if updates from the last checkpoint are still available
   * in the CDC instances.
   */
  private async checkSnapshotStatus(): Promise<SnapshotStatusResult> {
    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      // Snapshot is done, but we still need to check that the last known checkpoint LSN is still
      // within the threshold of the CDC tables
      this.logger.info(`Initial replication already done`);

      const lastCheckpointLSN = LSN.fromString(status.checkpoint_lsn);
      // Check that the CDC tables still have valid data
      const isAvailable = await isWithinRetentionThreshold({
        checkpointLSN: lastCheckpointLSN,
        tables: this.tableCache.getAll(),
        connectionManager: this.connections
      });
      if (!isAvailable) {
        this.logger.warn(
          `Updates from the last checkpoint are no longer available in the CDC instance, starting initial replication again.`
        );
      }
      return { status: isAvailable ? SnapshotStatus.DONE : SnapshotStatus.RESTART_REQUIRED, snapshotLSN: null };
    } else {
      return { status: SnapshotStatus.IN_PROGRESS, snapshotLSN: status.snapshot_lsn };
    }
  }

  async streamChanges() {
    await using writer = await this.storage.createWriter({
      logger: this.logger,
      zeroLSN: LSN.ZERO,
      defaultSchema: this.defaultSchema,
      storeCurrentData: false,
      skipExistingRows: false
    });

    if (writer.resumeFromLsn == null) {
      throw new ReplicationAssertionError(`No LSN found to resume replication from.`);
    }
    const startLSN = LSN.fromString(writer.resumeFromLsn);
    const sourceTables: MSSQLSourceTable[] = this.tableCache.getAll();
    const eventHandler = this.createEventHandler(writer);

    const poller = new CDCPoller({
      connectionManager: this.connections,
      eventHandler,
      sourceTables,
      startLSN,
      logger: this.logger,
      additionalConfig: this.options.additionalConfig
    });

    this.abortSignal.addEventListener(
      'abort',
      async () => {
        await poller.stop();
      },
      { once: true }
    );

    await createCheckpoint(this.connections);

    this.logger.info(`Streaming changes from: ${startLSN}`);
    await poller.replicateUntilStopped();
  }

  private createEventHandler(writer: storage.BucketDataWriter): CDCEventHandler {
    return {
      onInsert: async (row: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => {
        const afterRow = this.toSqliteRow(row, columns);
        await writer.save({
          tag: storage.SaveOperationTag.INSERT,
          sourceTable: table.sourceTable,
          before: undefined,
          beforeReplicaId: undefined,
          after: afterRow,
          afterReplicaId: getUuidReplicaIdentityBson(afterRow, table.sourceTable.replicaIdColumns)
        });
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
      },
      onUpdate: async (rowAfter: any, rowBefore: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => {
        const beforeRow = this.toSqliteRow(rowBefore, columns);
        const afterRow = this.toSqliteRow(rowAfter, columns);
        await writer.save({
          tag: storage.SaveOperationTag.UPDATE,
          sourceTable: table.sourceTable,
          before: beforeRow,
          beforeReplicaId: getUuidReplicaIdentityBson(beforeRow, table.sourceTable.replicaIdColumns),
          after: afterRow,
          afterReplicaId: getUuidReplicaIdentityBson(afterRow, table.sourceTable.replicaIdColumns)
        });
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
      },
      onDelete: async (row: any, table: MSSQLSourceTable, columns: sql.IColumnMetadata) => {
        const beforeRow = this.toSqliteRow(row, columns);
        await writer.save({
          tag: storage.SaveOperationTag.DELETE,
          sourceTable: table.sourceTable,
          before: beforeRow,
          beforeReplicaId: getUuidReplicaIdentityBson(beforeRow, table.sourceTable.replicaIdColumns),
          after: undefined,
          afterReplicaId: undefined
        });
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
      },
      onCommit: async (lsn: string, transactionCount: number) => {
        await writer.commit(lsn);
        this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(transactionCount);
        this.isStartingReplication = false;
      },
      onSchemaChange: async () => {
        // TODO: Handle schema changes
      }
    };
  }

  /**
   * Convert CDC row data to SqliteRow format.
   * CDC rows include table columns plus CDC metadata columns (__$operation, __$start_lsn, etc.).
   * We filter out the CDC metadata columns.
   */
  private toSqliteRow(row: any, columns: sql.IColumnMetadata): SqliteRow {
    const inputRow: SqliteInputRow = CDCToSqliteRow({ row, columns });

    return this.syncRules.applyRowContext<never>(inputRow);
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    if (this.oldestUncommittedChange == null) {
      if (this.isStartingReplication) {
        // We don't have anything to compute replication lag with yet.
        return undefined;
      } else {
        // We don't have any uncommitted changes, so replication is up-to-date.
        return 0;
      }
    }
    return Date.now() - this.oldestUncommittedChange.getTime();
  }

  private touch() {
    container.probes.touch().catch((e) => {
      this.logger.error(`Error touching probe`, e);
    });
  }
}
