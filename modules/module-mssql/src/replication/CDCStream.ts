import {
  container,
  DatabaseConnectionError,
  ErrorCode,
  errors,
  Logger,
  logger as defaultLogger,
  ReplicationAssertionError,
  ReplicationAbortedError,
  ServiceAssertionError
} from '@powersync/lib-services-framework';
import {
  ColumnDescriptor,
  getUuidReplicaIdentityBson,
  MetricsEngine,
  SaveUpdate,
  SourceEntityDescriptor,
  storage
} from '@powersync/service-core';

import {
  applyValueContext,
  CompatibilityContext,
  SqliteInputRow,
  SqlSyncRules,
  TablePattern
} from '@powersync/service-sync-rules';

import { ReplicationMetric } from '@powersync/service-types';
import {
  BatchedSnapshotQuery,
  IdSnapshotQuery,
  PrimaryKeyValue,
  SimpleSnapshotQuery,
  MSSQLSnapshotQuery
} from './MSSQLSnapshotQuery.js';
import { MSSQLConnectionManager } from './MSSQLConnectionManager.js';
import * as schema_utils from '../utils/schema.js';
import {
  checkSourceConfiguration,
  getCaptureInstance,
  getLatestLSN,
  isIColumnMetadata,
  isTableEnabledForCDC,
  isWithinRetentionThreshold
} from '../utils/mssql.js';
import { ResolvedTable } from '../utils/schema.js';
import sql from 'mssql';
import { toSqliteInputRow } from '../common/mssqls-to-sqlite.js';
import { LSN } from '../common/LSN.js';
import { MSSQLSourceTable } from '../common/MSSQLSourceTable.js';
import { MSSQLSourceTableCache } from '../common/MSSQLSourceTableCache.js';

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
}

export enum SnapshotStatus {
  IN_PROGRESS = 'in-progress',
  DONE = 'done',
  RESTART_REQUIRED = 'restart-required'
}

interface WriteChangePayload {
  type: storage.SaveOperationTag;
  row: sql.IRecordSet<any>;
  previous_row?: sql.IRecordSet<any>;
  schema: string;
  table: string;
  sourceTable: storage.SourceTable;
  columns: Map<string, ColumnDescriptor>;
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
  private readonly syncRules: SqlSyncRules;
  private readonly storage: storage.SyncRulesBucketStorage;
  private readonly connections: MSSQLConnectionManager;
  private readonly abortSignal: AbortSignal;
  private readonly logger: Logger;

  private tableCache = new MSSQLSourceTableCache();

  private startedPolling = false;

  /**
   * Time of the oldest uncommitted change, according to the source db.
   * This is used to determine the replication lag.
   */
  private oldestUncommittedChange: Date | null = null;
  /**
   * Keep track of whether we have done a commit or keepalive yet.
   * We can only compute replication lag if isStartingReplication == false, or oldestUncommittedChange is present.
   */
  private isStartingReplication = true;

  constructor(private options: CDCStreamOptions) {
    this.logger = options.logger ?? defaultLogger;
    this.storage = options.storage;
    this.syncRules = options.storage.getParsedSyncRules({ defaultSchema: options.connections.schema });
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

  async getQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    tablePattern: TablePattern
  ): Promise<MSSQLSourceTable[]> {
    if (tablePattern.connectionTag != this.connections.connectionTag) {
      return [];
    }

    const matchedTables: ResolvedTable[] = await schema_utils.getTablesFromPattern(this.connections, tablePattern);

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

      const replicaIdColumns = await schema_utils.getReplicationIdentityColumns({
        connectionManager: this.connections,
        tableName: matchedTable.name,
        schema: matchedTable.schema
      });

      const table = await this.processTable(
        batch,
        {
          name: matchedTable.name,
          schema: matchedTable.schema,
          objectId: matchedTable.objectId,
          replicaIdColumns: replicaIdColumns.columns
        },
        false
      );

      tables.push(table);
    }
    return tables;
  }

  async processTable(
    batch: storage.BucketStorageBatch,
    table: SourceEntityDescriptor,
    snapshot: boolean
  ): Promise<MSSQLSourceTable> {
    if (!table.objectId && typeof table.objectId != 'number') {
      throw new ReplicationAssertionError(`objectId expected, got ${typeof table.objectId}`);
    }
    const resolved = await this.storage.resolveTable({
      group_id: this.groupId,
      connection_id: this.connectionId,
      connection_tag: this.connectionTag,
      entity_descriptor: table,
      sync_rules: this.syncRules
    });
    const captureInstance = await getCaptureInstance(this.connections, resolved.table);
    if (!captureInstance) {
      throw new ServiceAssertionError(`Missing capture instance for table ${resolved.table}`);
    }
    const resolvedTable = new MSSQLSourceTable({
      sourceTable: resolved.table,
      captureInstance: captureInstance
    });
    this.tableCache.set(resolvedTable);

    // Drop conflicting tables. This includes for example renamed tables.
    await batch.drop(resolved.dropTables);

    // Snapshot if:
    // 1. Snapshot is requested (false for initial snapshot, since that process handles it elsewhere)
    // 2. Snapshot is not already done, AND:
    // 3. The table is used in sync rules.
    const shouldSnapshot = snapshot && !resolved.table.snapshotComplete && resolved.table.syncAny;

    if (shouldSnapshot) {
      // Truncate this table in case a previous snapshot was interrupted.
      await batch.truncate([resolved.table]);

      // Start the snapshot inside a transaction.
      try {
        await this.snapshotTableInTx(batch, resolvedTable);
      } finally {
        // TODO Cleanup?
      }
    }

    return resolvedTable;
  }

  private async snapshotTableInTx(
    batch: storage.BucketStorageBatch,
    table: MSSQLSourceTable,
    limited?: PrimaryKeyValue[]
  ): Promise<void> {
    // Note: We use the "Read Committed" isolation level here, not snapshot isolation.
    // The data may change during the transaction, but that is compensated for in the streaming
    // replication afterward.
    const transaction = await this.connections.createTransaction();
    await transaction.begin(sql.ISOLATION_LEVEL.READ_COMMITTED);
    try {
      await this.snapshotTable(batch, transaction, table, limited);

      // Get the current LSN.
      // The data will only be consistent once incremental replication has passed that point.
      // We have to get this LSN _after_ we have finished the table snapshot.
      //
      // There are basically two relevant LSNs here:
      // A: The LSN before the snapshot starts. We don't explicitly record this on the PowerSync side,
      //    but it is implicitly recorded in the replication slot.
      // B: The LSN after the table snapshot is complete, which is what we get here.
      // When we do the snapshot queries, the data that we get back for each batch could match the state
      // anywhere between A and B. To actually have a consistent state on our side, we need to:
      // 1. Complete the snapshot.
      // 2. Wait until logical replication has caught up with all the changes between A and B.
      // Calling `markSnapshotDone(LSN B)` covers that.
      const tableLsnNotBefore = await getLatestLSN(this.connections);
      // Side note: A ROLLBACK would probably also be fine here, since we only read in this transaction.
      await transaction.commit();
      const [updatedSourceTable] = await batch.markSnapshotDone([table.sourceTable], tableLsnNotBefore.toString());
      this.tableCache.updateSourceTable(updatedSourceTable);
    } catch (e) {
      await transaction.rollback();
      throw e;
    }
  }

  private async snapshotTable(
    batch: storage.BucketStorageBatch,
    transaction: sql.Transaction,
    table: MSSQLSourceTable,
    limited?: PrimaryKeyValue[]
  ) {
    let totalEstimatedCount = table.sourceTable.snapshotStatus?.totalEstimatedCount;
    let replicatedCount = table.sourceTable.snapshotStatus?.replicatedCount ?? 0;
    let lastCountTime = 0;
    let query: MSSQLSnapshotQuery;
    // We do streaming on two levels:
    // 1. Coarse select from the entire table, stream rows 1 by one
    // 2. Fine level: Stream batches of rows with each fetch call
    if (limited) {
      query = new IdSnapshotQuery(transaction, table, limited);
    } else if (BatchedSnapshotQuery.supports(table)) {
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

    let columns: sql.IColumnMetadata | null = null;
    let hasRemainingData = true;
    while (hasRemainingData) {
      // Fetch 10k at a time.
      // The balance here is between latency overhead per FETCH call,
      // and not spending too much time on each FETCH call.
      // We aim for a couple of seconds on each FETCH call.
      const cursor = query.next();
      hasRemainingData = false;
      // MSSQL streams rows one by one
      for await (const result of cursor) {
        if (isIColumnMetadata(result)) {
          columns = result;
          continue;
        } else {
          if (!columns) {
            throw new ReplicationAssertionError(`Missing column metadata`);
          }
          const row: SqliteInputRow = toSqliteInputRow(result, columns!);

          // This auto-flushes when the batch reaches its size limit
          await batch.save({
            tag: storage.SaveOperationTag.INSERT,
            sourceTable: table.sourceTable,
            before: undefined,
            beforeReplicaId: undefined,
            after: row,
            afterReplicaId: getUuidReplicaIdentityBson(row, table.sourceTable.replicaIdColumns)
          });

          replicatedCount++;
          this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
        }

        this.touch();
      }

      // Important: flush before marking progress
      await batch.flush();
      if (limited == null) {
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
        const updatedSourceTable = await batch.updateTableProgress(table.sourceTable, {
          lastKey: lastKey,
          replicatedCount: replicatedCount,
          totalEstimatedCount: totalEstimatedCount
        });
        this.tableCache.updateSourceTable(updatedSourceTable);

        this.logger.info(`Replicating ${table.toQualifiedName()} ${table.sourceTable.formatSnapshotProgress()}`);
      } else {
        this.logger.info(`Replicating ${table.toQualifiedName()} ${replicatedCount}/${limited.length} for resnapshot`);
      }

      if (this.abortSignal.aborted) {
        // We only abort after flushing
        throw new ReplicationAbortedError(`Initial replication interrupted`);
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
    const { recordset: result } = await request.query(
      `SELECT SUM(row_count) AS total_rows
       FROM sys.dm_db_partition_stats
       WHERE object_id = OBJECT_ID('${table.toQualifiedName()}')
       AND index_id < 2;`
    );
    // TODO Fallback query in case user does not have permission?
    return result[0].total_rows ?? -1;
  }

  /**
   * Start initial replication.
   *
   * If (partial) replication was done before on this slot, this clears the state
   * and starts again from scratch.
   */
  async startInitialReplication(status: SnapshotStatus) {
    if (status === SnapshotStatus.RESTART_REQUIRED) {
      // This happens if the last replicated checkpoint LSN is no longer available in the CDC tables.
      await this.storage.clear({ signal: this.abortSignal });
    }

    const sourceTables = this.syncRules.getSourceTables();
    await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: LSN.ZERO,
        defaultSchema: this.defaultSchema,
        storeCurrentData: true,
        skipExistingRows: true
      },
      async (batch) => {
        const tablesWithStatus: MSSQLSourceTable[] = [];
        for (const tablePattern of sourceTables) {
          const tables = await this.getQualifiedTableNames(batch, tablePattern);
          // Pre-get counts
          for (const table of tables) {
            if (table.sourceTable.snapshotComplete) {
              this.logger.info(`Skipping ${table.toQualifiedName()} - snapshot already done.`);
              continue;
            }
            const count = await this.estimatedCountNumber(table);
            const updatedSourceTable = await batch.updateTableProgress(table.sourceTable, {
              totalEstimatedCount: count
            });
            this.tableCache.updateSourceTable(updatedSourceTable);
            tablesWithStatus.push(table);

            this.logger.info(`To replicate: ${table.toQualifiedName()} ${table.sourceTable.formatSnapshotProgress()}`);
          }
        }

        for (const table of tablesWithStatus) {
          await this.snapshotTableInTx(batch, table);
          this.touch();
        }

        // Always commit the initial snapshot at zero.
        // This makes sure we don't skip any changes applied before starting this snapshot,
        // in the case of snapshot retries.
        await batch.commit(LSN.ZERO);
      }
    );
  }

  private getTable(tableId: number): MSSQLSourceTable {
    const table = this.tableCache.get(tableId);
    if (table == null) {
      // We should always receive a replication message before the relation is used.
      // If we can't find it, it's a bug.
      throw new ReplicationAssertionError(`Table with ${tableId} not found in cache`);
    }
    return table;
  }

  // async writeChange(
  //   batch: storage.BucketStorageBatch,
  //   payload: WriteChangePayload
  // ): Promise<storage.FlushedResult | null> {
  //   switch (payload.type) {
  //     case storage.SaveOperationTag.INSERT:
  //       this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
  //       const record = toSqliteInputRow(payload.row, payload.columns);
  //       return await batch.save({
  //         tag: storage.SaveOperationTag.INSERT,
  //         sourceTable: payload.sourceTable,
  //         before: undefined,
  //         beforeReplicaId: undefined,
  //         after: record,
  //         afterReplicaId: getUuidReplicaIdentityBson(record, payload.sourceTable.replicaIdColumns)
  //       });
  //     case storage.SaveOperationTag.UPDATE:
  //       this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
  //       // The previous row may be null if the replica id columns are unchanged.
  //       // It's fine to treat that the same as an insert.
  //       const beforeUpdated = payload.previous_row
  //         ? toSqliteInputRow(payload.previous_row, payload.columns)
  //         : undefined;
  //       const after = toSqliteInputRow(payload.row, payload.columns);
  //
  //       return await batch.save({
  //         tag: storage.SaveOperationTag.UPDATE,
  //         sourceTable: payload.sourceTable,
  //         before: beforeUpdated,
  //         beforeReplicaId: beforeUpdated
  //           ? getUuidReplicaIdentityBson(beforeUpdated, payload.sourceTable.replicaIdColumns)
  //           : undefined,
  //         after: after,
  //         afterReplicaId: getUuidReplicaIdentityBson(after, payload.sourceTable.replicaIdColumns)
  //       });
  //
  //     case storage.SaveOperationTag.DELETE:
  //       this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
  //       const beforeDeleted = toSqliteInputRow(payload.row, payload.columns);
  //
  //       return await batch.save({
  //         tag: storage.SaveOperationTag.DELETE,
  //         sourceTable: payload.sourceTable,
  //         before: beforeDeleted,
  //         beforeReplicaId: getUuidReplicaIdentityBson(beforeDeleted, payload.sourceTable.replicaIdColumns),
  //         after: undefined,
  //         afterReplicaId: undefined
  //       });
  //     default:
  //       return null;
  //   }
  // }

  async replicate() {
    try {
      await this.initReplication();
      //await this.streamChanges();
    } catch (e) {
      await this.storage.reportError(e);
      throw e;
    }
  }

  async initReplication() {
    const errors = await checkSourceConfiguration(this.connections);
    if (errors.length > 0) {
      throw new CDCConfigurationError(`CDC Configuration Errors: ${errors.join(', ')}`);
    }

    const snapshotStatus = await this.checkSnapshotStatus();
    if (snapshotStatus !== SnapshotStatus.DONE) {
      await this.startInitialReplication(snapshotStatus);
    }
  }

  /**
   * Checks if the initial sync has already been completed and if updates from the last checkpoint are still available
   * in the CDC instances.
   */
  private async checkSnapshotStatus(): Promise<SnapshotStatus> {
    const status = await this.storage.getStatus();
    const snapshotDone = status.snapshot_done && status.checkpoint_lsn != null;
    if (snapshotDone) {
      // Snapshot is done, but we still need to check that the last known checkpoint LSN is still
      // within the threshold of the CDC tables
      this.logger.info(`Initial replication already done`);

      const lastCheckpointLSN = LSN.fromString(status.checkpoint_lsn!);
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
      return isAvailable ? SnapshotStatus.DONE : SnapshotStatus.RESTART_REQUIRED;
    }

    return SnapshotStatus.IN_PROGRESS;
  }

  // async streamChanges() {
  //   // When changing any logic here, check /docs/wal-lsns.md.
  //   const { createEmptyCheckpoints } = await this.ensureStorageCompatibility();
  //
  //   const replicationOptions: Record<string, string> = {
  //     proto_version: '1',
  //     publication_names: PUBLICATION_NAME
  //   };
  //
  //   /**
  //    * Viewing the contents of logical messages emitted with `pg_logical_emit_message`
  //    * is only supported on Postgres >= 14.0.
  //    * https://www.postgresql.org/docs/14/protocol-logical-replication.html
  //    */
  //   const exposesLogicalMessages = await this.checkLogicalMessageSupport();
  //   if (exposesLogicalMessages) {
  //     /**
  //      * Only add this option if the Postgres server supports it.
  //      * Adding the option to a server that doesn't support it will throw an exception when starting logical replication.
  //      * Error: `unrecognized pgoutput option: messages`
  //      */
  //     replicationOptions['messages'] = 'true';
  //   }
  //
  //   const replicationStream = replicationConnection.logicalReplication({
  //     slot: this.slot_name,
  //     options: replicationOptions
  //   });
  //
  //   this.startedStreaming = true;
  //
  //   let resnapshot: { table: storage.SourceTable; key: PrimaryKeyValue }[] = [];
  //
  //   const markRecordUnavailable = (record: SaveUpdate) => {
  //     if (!IdSnapshotQuery.supports(record.sourceTable)) {
  //       // If it's not supported, it's also safe to ignore
  //       return;
  //     }
  //     let key: PrimaryKeyValue = {};
  //     for (let column of record.sourceTable.replicaIdColumns) {
  //       const name = column.name;
  //       const value = record.after[name];
  //       if (value == null) {
  //         // We don't expect this to actually happen.
  //         // The key should always be present in the "after" record.
  //         return;
  //       }
  //       // We just need a consistent representation of the primary key, and don't care about fixed quirks.
  //       key[name] = applyValueContext(value, CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY);
  //     }
  //     resnapshot.push({
  //       table: record.sourceTable,
  //       key: key
  //     });
  //   };
  //
  //   await this.storage.startBatch(
  //     {
  //       logger: this.logger,
  //       zeroLSN: ZERO_LSN,
  //       defaultSchema: POSTGRES_DEFAULT_SCHEMA,
  //       storeCurrentData: true,
  //       skipExistingRows: false,
  //       markRecordUnavailable
  //     },
  //     async (batch) => {
  //       // We don't handle any plain keepalive messages while we have transactions.
  //       // While we have transactions, we use that to advance the position.
  //       // Replication never starts in the middle of a transaction, so this starts as false.
  //       let skipKeepalive = false;
  //       let count = 0;
  //
  //       for await (const chunk of replicationStream.pgoutputDecode()) {
  //         this.touch();
  //
  //         if (this.abortSignal.aborted) {
  //           break;
  //         }
  //
  //         // chunkLastLsn may come from normal messages in the chunk,
  //         // or from a PrimaryKeepalive message.
  //         const { messages, lastLsn: chunkLastLsn } = chunk;
  //
  //         /**
  //          * We can check if an explicit keepalive was sent if `exposesLogicalMessages == true`.
  //          * If we can't check the logical messages, we should assume a keepalive if we
  //          * receive an empty array of messages in a replication event.
  //          */
  //         const assumeKeepAlive = !exposesLogicalMessages;
  //         let keepAliveDetected = false;
  //         const lastCommit = messages.findLast((msg) => msg.tag == 'commit');
  //
  //         for (const msg of messages) {
  //           if (msg.tag == 'relation') {
  //             await this.handleRelation(batch, getPgOutputRelation(msg), true);
  //           } else if (msg.tag == 'begin') {
  //             // This may span multiple transactions in the same chunk, or even across chunks.
  //             skipKeepalive = true;
  //             if (this.oldestUncommittedChange == null) {
  //               this.oldestUncommittedChange = new Date(Number(msg.commitTime / 1000n));
  //             }
  //           } else if (msg.tag == 'commit') {
  //             this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(1);
  //             if (msg == lastCommit) {
  //               // Only commit if this is the last commit in the chunk.
  //               // This effectively lets us batch multiple transactions within the same chunk
  //               // into a single flush, increasing throughput for many small transactions.
  //               skipKeepalive = false;
  //               // flush() must be before the resnapshot check - that is
  //               // typically what reports the resnapshot records.
  //               await batch.flush({ oldestUncommittedChange: this.oldestUncommittedChange });
  //               // This _must_ be checked after the flush(), and before
  //               // commit() or ack(). We never persist the resnapshot list,
  //               // so we have to process it before marking our progress.
  //               if (resnapshot.length > 0) {
  //                 await this.resnapshot(batch, resnapshot);
  //                 resnapshot = [];
  //               }
  //               const didCommit = await batch.commit(msg.lsn!, {
  //                 createEmptyCheckpoints,
  //                 oldestUncommittedChange: this.oldestUncommittedChange
  //               });
  //               await this.ack(msg.lsn!, replicationStream);
  //               if (didCommit) {
  //                 this.oldestUncommittedChange = null;
  //                 this.isStartingReplication = false;
  //               }
  //             }
  //           } else {
  //             if (count % 100 == 0) {
  //               this.logger.info(`Replicating op ${count} ${msg.lsn}`);
  //             }
  //
  //             /**
  //              * If we can see the contents of logical messages, then we can check if a keepalive
  //              * message is present. We only perform a keepalive (below) if we explicitly detect a keepalive message.
  //              * If we can't see the contents of logical messages, then we should assume a keepalive is required
  //              * due to the default value of `assumeKeepalive`.
  //              */
  //             if (exposesLogicalMessages && isKeepAliveMessage(msg)) {
  //               keepAliveDetected = true;
  //             }
  //
  //             count += 1;
  //             const flushResult = await this.writeChange(batch, msg);
  //             if (flushResult != null && resnapshot.length > 0) {
  //               // If we have large transactions, we also need to flush the resnapshot list
  //               // periodically.
  //               // TODO: make sure this bit is actually triggered
  //               await this.resnapshot(batch, resnapshot);
  //               resnapshot = [];
  //             }
  //           }
  //         }
  //
  //         if (!skipKeepalive) {
  //           if (assumeKeepAlive || keepAliveDetected) {
  //             // Reset the detection flag.
  //             keepAliveDetected = false;
  //
  //             // In a transaction, we ack and commit according to the transaction progress.
  //             // Outside transactions, we use the PrimaryKeepalive messages to advance progress.
  //             // Big caveat: This _must not_ be used to skip individual messages, since this LSN
  //             // may be in the middle of the next transaction.
  //             // It must only be used to associate checkpoints with LSNs.
  //             const didCommit = await batch.keepalive(chunkLastLsn);
  //             if (didCommit) {
  //               this.oldestUncommittedChange = null;
  //             }
  //
  //             this.isStartingReplication = false;
  //           }
  //
  //           // We receive chunks with empty messages often (about each second).
  //           // Acknowledging here progresses the slot past these and frees up resources.
  //           await this.ack(chunkLastLsn, replicationStream);
  //         }
  //
  //         this.metrics.getCounter(ReplicationMetric.CHUNKS_REPLICATED).add(1);
  //       }
  //     }
  //   );
  // }

  // async ack(lsn: string, replicationStream: pgwire.ReplicationStream) {
  //   if (lsn == ZERO_LSN) {
  //     return;
  //   }
  //
  //   replicationStream.ack(lsn);
  // }

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
