import {
  container,
  DatabaseConnectionError,
  logger as defaultLogger,
  ErrorCode,
  Logger,
  ReplicationAbortedError,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import {
  MetricsEngine,
  RelationCache,
  ReplicationLagTracker,
  SaveOperationTag,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import { HydratedSyncConfig, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { setTimeout as delay } from 'timers/promises';
import { ConvexListSnapshotResult, ConvexRawDocument } from '../client/ConvexAPITypes.js';
import { isCursorExpiredError } from '../client/ConvexApiClient.js';
import { isConvexCheckpointTable } from '../common/ConvexCheckpoints.js';
import { lsnCursorToDate, parseConvexLsn, ZERO_LSN } from '../common/ConvexLSN.js';
import { toSqliteInputRow } from '../common/convex-to-sqlite.js';
import { ConvexConnectionManager } from './ConvexConnectionManager.js';
import { BinaryConvexSnapshotProgressCursor, decodeSnapshotProgressCursor } from './ConvexSnapshotProgressCursor.js';

export interface ConvexStreamOptions {
  connections: ConvexConnectionManager;
  storage: storage.SyncRulesBucketStorage;
  metrics: MetricsEngine;
  abortSignal: AbortSignal;
  logger?: Logger;
}

export class ConvexCursorExpiredError extends DatabaseConnectionError {
  constructor(message: string, cause: unknown) {
    super(ErrorCode.PSYNC_S1500, message, cause);
  }
}

export class ConvexStream {
  private readonly storage: storage.SyncRulesBucketStorage;
  private readonly metrics: MetricsEngine;
  private readonly syncConfig: HydratedSyncConfig;
  private readonly logger: Logger;

  private readonly relationCache = new RelationCache(getCacheIdentifier);
  private replicationLag = new ReplicationLagTracker();

  private lastKeepaliveAt = 0;
  private lastTouchedAt = performance.now();

  private initialSnapshotPromise: Promise<void> | null = null;

  constructor(private readonly options: ConvexStreamOptions) {
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.syncConfig = options.storage.getParsedSyncRules({ defaultSchema: options.connections.schema });
    this.logger = options.logger ?? defaultLogger;
  }

  get isStartingReplication() {
    return this.replicationLag.isStartingReplication;
  }

  private get connections() {
    return this.options.connections;
  }

  private get abortSignal() {
    return this.options.abortSignal;
  }

  private get defaultSchema() {
    return this.connections.schema;
  }

  private get pollingIntervalMs() {
    return this.connections.config.polling_interval_ms;
  }

  get stopped() {
    return this.abortSignal.aborted;
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

  async replicate() {
    try {
      this.initialSnapshotPromise = this.initReplication();
      // This pattern/member is used for tests
      await this.initialSnapshotPromise;

      await this.streamChanges();
    } catch (error) {
      await this.storage.reportError(error);
      throw error;
    }
  }

  /**
   * After calling replicate(), call this to wait for the initial snapshot to complete.
   *
   * For tests only.
   */
  async waitForInitialSnapshot() {
    if (this.initialSnapshotPromise == null) {
      throw new ReplicationAssertionError(`Initial snapshot not started yet`);
    }
    return this.initialSnapshotPromise;
  }

  async initReplication() {
    const status = await this.initSlot();
    if (!status.needsInitialSync) {
      return;
    }

    if (status.snapshotLsn == null) {
      await this.storage.clear({ signal: this.abortSignal });
    }

    const { lastOpId } = await this.initialReplication(status.snapshotLsn);
    if (lastOpId != null) {
      await this.storage.populatePersistentChecksumCache({
        signal: this.abortSignal,
        maxOpId: lastOpId
      });
    }
  }

  async streamChanges() {
    await using batch = await this.storage.createWriter({
      logger: this.logger,
      zeroLSN: ZERO_LSN,
      defaultSchema: this.defaultSchema,
      // Convex document_deltas include the full document state after each mutation,
      // so storage does not need to keep current row data to apply partial updates.
      storeCurrentData: false,
      skipExistingRows: false
    });

    let resumeFromLsn = batch.resumeFromLsn;
    if (resumeFromLsn == null) {
      throw new ReplicationAssertionError(`No LSN found to resume replication from.`);
    }

    // Resolve source tables up-front to warm table metadata and sync-config matching.
    await this.resolveAllSourceTables(batch);

    let cursor: string = resumeFromLsn;
    let lastTransactionTimestamp: bigint | null = null;

    while (!this.abortSignal.aborted) {
      const page = await this.connections.client
        .documentDeltas({
          cursor: cursor,
          signal: this.abortSignal
        })
        .catch((error) => {
          if (isCursorExpiredError(error)) {
            throw new ConvexCursorExpiredError('Convex cursor expired; initial replication restart required', error);
          }
          throw error;
        });

      // We receive the cursor as a bigint, but we track it as a string
      const nextCursor = page.cursor.toString();
      const pageLsn = parseConvexLsn(nextCursor);

      let changesInPage = 0;
      const transactionTimestampsInPage = new Set<string>();
      let sawCheckpointMarker = false;
      let didMarkOldestUncommitedChange = false;

      /**
       * Convex returns document_deltas in mutation order by _ts (corresponding to mutation/transaction).
       * The row order inside each transaction is out-of-order.
       * It looks like Convex squashes multiple mutations on rows before storing deltas.
       * We currently don't sort values by their `_creationTime` value.
       */
      for (const change of page.values) {
        if (this.abortSignal.aborted) {
          throw new ReplicationAbortedError('Replication interrupted');
        }

        const tableName = readTableName(change);
        if (tableName == null) {
          continue;
        }

        if (isConvexCheckpointTable(tableName)) {
          sawCheckpointMarker = true;
          continue;
        }

        const transactionTimestamp = change._ts;
        if (lastTransactionTimestamp != null && transactionTimestamp < lastTransactionTimestamp) {
          throw new ReplicationAssertionError(
            `Convex document_deltas returned out-of-order _ts values: ${transactionTimestamp} after ${lastTransactionTimestamp}`
          );
        }
        lastTransactionTimestamp = transactionTimestamp;

        const tables = await this.getOrResolveTables(batch, tableName, pageLsn);
        if (tables.length == 0) {
          continue;
        }

        /**
         * This tracks the begining of a new transaction which is not yet commited.
         * This uses the current page's cursor as the timestamp since this is the closes timestamp
         * to the mutation.
         * We should only track the first op for a new transaction.
         * Note that the document-deltas aren't filtered, so we only
         * mark the start after this point - which means we will have an uncommited change.
         */
        if (!didMarkOldestUncommitedChange) {
          this.replicationLag.trackUncommittedChange(lsnCursorToDate(page.cursor));
          didMarkOldestUncommitedChange = true;
        }

        let wroteChange = false;
        for (const table of tables) {
          if (!table.syncAny) {
            continue;
          }
          const changed = await this.writeChange(batch, table, change);
          wroteChange = wroteChange || changed;
        }

        if (wroteChange) {
          // Convex assigns one _ts commit timestamp to every write in a mutation.
          // document_deltas may return multiple mutations in one page, so transaction
          // metrics are counted by distinct _ts values, not by delta pages.
          changesInPage += 1;
          transactionTimestampsInPage.add(transactionTimestamp.toString());
        }
      }

      if (changesInPage > 0) {
        /**
         * It looks like the document-deltas api won't split transactions between pages,
         * That means it should be safe to commit after each page.
         * Each page could contain many smaller transactions - that should also be fine.
         */
        const { checkpointBlocked } = await batch.commit(pageLsn, {
          createEmptyCheckpoints: false,
          oldestUncommittedChange: this.replicationLag.oldestUncommittedChange
        });

        this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(transactionTimestampsInPage.size);
        if (!checkpointBlocked) {
          this.replicationLag.markCommitted();
        }
      } else if (sawCheckpointMarker) {
        /**
         * This is only reached if the checkpoint marker was the only change observed in a page.
         */
        const { checkpointBlocked } = await batch.keepalive(pageLsn);
        if (!checkpointBlocked) {
          this.replicationLag.clearUncommittedChange();
        }
        this.lastKeepaliveAt = Date.now();
      } else if (nextCursor == cursor && Date.now() - this.lastKeepaliveAt > 60_000) {
        const { checkpointBlocked } = await batch.keepalive(pageLsn);
        if (!checkpointBlocked) {
          this.replicationLag.clearUncommittedChange();
        }
        this.replicationLag.markStarted();
        this.lastKeepaliveAt = Date.now();
      }

      cursor = nextCursor;

      if (!page.hasMore) {
        await delay(this.pollingIntervalMs, undefined, { signal: this.abortSignal }).catch((error) => {
          if (this.abortSignal.aborted) {
            return;
          }
          throw error;
        });
      }

      this.touch();
    }
  }

  getReplicationLagMillis(): number | undefined {
    return this.replicationLag.getLagMillis();
  }

  private async initSlot(): Promise<{ needsInitialSync: boolean; snapshotLsn: string | null }> {
    const status = await this.storage.getStatus();
    if (status.snapshot_done && status.checkpoint_lsn) {
      this.logger.info('Initial replication already done');
      return {
        needsInitialSync: false,
        snapshotLsn: null
      };
    }

    return {
      needsInitialSync: true,
      snapshotLsn: status.resume_lsn
    };
  }

  private async initialReplication(snapshotLsn: string | null) {
    await using batch = await this.storage.createWriter({
      logger: this.logger,
      zeroLSN: ZERO_LSN,
      defaultSchema: this.defaultSchema,
      // Convex snapshots emit complete documents, so no current row state is needed.
      storeCurrentData: false,
      skipExistingRows: true
    });

    const snapshotCursor = await this.resolveSnapshotBoundary(snapshotLsn);
    const snapshotLsnValue = parseConvexLsn(snapshotCursor);
    await batch.setResumeLsn(snapshotLsnValue);

    const sourceTables = await this.resolveAllSourceTables(batch);

    for (const sourceTable of sourceTables) {
      if (sourceTable.snapshotComplete) {
        this.logger.info(`Skipping table [${sourceTable.qualifiedName}] - snapshot already done.`);
        continue;
      }

      const tableWithProgress =
        sourceTable.snapshotStatus == null
          ? await batch.updateTableProgress(sourceTable, {
              totalEstimatedCount: -1,
              replicatedCount: 0,
              lastKey: null
            })
          : sourceTable;
      this.relationCache.update(tableWithProgress);

      await this.snapshotTable(batch, tableWithProgress, snapshotCursor);
    }

    await batch.markAllSnapshotDone(snapshotLsnValue);

    await batch.commit(snapshotLsnValue);

    this.logger.info(`Snapshot done. Need to replicate from ${snapshotLsnValue} for consistency.`);

    return {
      lastOpId: batch.last_flushed_op
    };
  }

  private async snapshotTable(
    batch: storage.BucketStorageBatch,
    table: SourceTable,
    snapshotCursor: string
  ): Promise<{ table: SourceTable }> {
    const snapshotProgress = decodeSnapshotProgressCursor(table.snapshotStatus?.lastKey);
    let pageCursor = snapshotProgress.cursor;
    let replicatedCount = table.snapshotStatus?.replicatedCount ?? 0;
    let latestTable = table;

    if (snapshotProgress.finished) {
      this.logger.info(`Finishing table snapshot from persisted progress for [${table.qualifiedName}]`);
    } else if (pageCursor != null) {
      this.logger.info(`Resuming table snapshot from persisted cursor for [${table.qualifiedName}]`);
    } else {
      this.logger.info(`Starting table snapshot from first page for [${table.qualifiedName}]`);
    }

    if (this.abortSignal.aborted) {
      throw new ReplicationAbortedError('Initial replication interrupted');
    }

    if (snapshotProgress.finished) {
      return {
        table: await this.markSnapshotDone(batch, latestTable, snapshotCursor)
      };
    }

    while (!this.abortSignal.aborted) {
      const page: ConvexListSnapshotResult = await this.connections.client
        .listSnapshot({
          tableName: table.name,
          snapshot: snapshotCursor,
          cursor: pageCursor ?? undefined,
          signal: this.abortSignal
        })
        .catch((error) => {
          if (isCursorExpiredError(error)) {
            throw new ConvexCursorExpiredError('Convex snapshot cursor expired; restart required', error);
          }
          throw error;
        });

      if (snapshotCursor != page.snapshot.toString()) {
        throw new ReplicationAssertionError(
          `Convex snapshot cursor changed while snapshotting ${table.qualifiedName}: ${snapshotCursor} -> ${page.snapshot}`
        );
      }

      for (const rawDocument of page.values) {
        if (rawDocument._deleted) {
          continue;
        }

        const replicaId = rawDocument._id;
        if (replicaId == null) {
          this.logger.warn(`Skipping Convex document without _id on table ${table.qualifiedName}`);
          continue;
        }

        const row = this.toSqliteRow(rawDocument);
        await batch.save({
          tag: SaveOperationTag.INSERT,
          sourceTable: latestTable,
          before: undefined,
          beforeReplicaId: undefined,
          after: row,
          afterReplicaId: replicaId
        });
        replicatedCount += 1;
        this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
      }

      await batch.flush();

      pageCursor = page.cursor;
      latestTable = await batch.updateTableProgress(latestTable, {
        replicatedCount,
        totalEstimatedCount: -1,
        lastKey: BinaryConvexSnapshotProgressCursor.encode({
          cursor: pageCursor,
          finished: !page.hasMore
        })
      });
      this.relationCache.update(latestTable);

      if (!page.hasMore) {
        break;
      }

      this.touch();
    }

    if (this.abortSignal.aborted) {
      throw new ReplicationAbortedError('Initial replication interrupted');
    }

    return {
      table: await this.markSnapshotDone(batch, latestTable, snapshotCursor)
    };
  }

  private async markSnapshotDone(
    batch: storage.BucketStorageBatch,
    table: SourceTable,
    snapshotCursor: string
  ): Promise<SourceTable> {
    const snapshotLsnValue = parseConvexLsn(snapshotCursor);
    const [doneTable] = await batch.markTableSnapshotDone([table], snapshotLsnValue);
    this.relationCache.update(doneTable);
    return doneTable;
  }

  private async resolveSnapshotBoundary(snapshotLsn: string | null): Promise<string> {
    if (snapshotLsn != null) {
      const snapshotCursor = parseConvexLsn(snapshotLsn);
      this.logger.info(`Using existing global snapshot ${snapshotCursor}`);
      return snapshotCursor;
    }

    const snapshotCursor = await this.connections.client.getGlobalSnapshotCursor({ signal: this.abortSignal });
    this.logger.info(`Pinned global snapshot ${snapshotCursor}`);
    return snapshotCursor;
  }

  private async resolveAllSourceTables(batch: storage.BucketStorageBatch): Promise<SourceTable[]> {
    const sourceTablePatterns = this.syncConfig.getSourceTables();
    const resolved: SourceTable[] = [];
    const seenSourceTableIds = new Set<string>();

    for (const tablePattern of sourceTablePatterns) {
      const tables = await this.resolveQualifiedTableNames(batch, tablePattern);
      for (const table of tables) {
        const id = `${table.id}`;
        if (seenSourceTableIds.has(id)) {
          continue;
        }
        seenSourceTableIds.add(id);
        resolved.push(table);
      }
    }

    return resolved;
  }

  private async resolveQualifiedTableNames(
    batch: storage.BucketStorageBatch,
    tablePattern: TablePattern
  ): Promise<SourceTable[]> {
    if (tablePattern.connectionTag != this.connections.connectionTag) {
      return [];
    }

    if (tablePattern.schema != this.defaultSchema) {
      return [];
    }

    const matchedTableNames = await this.resolveTablePattern(tablePattern);

    if (!tablePattern.isWildcard && matchedTableNames.length == 0) {
      this.logger.warn(`Table ${tablePattern.schema}.${tablePattern.name} not found`);
    }

    const resolved: SourceTable[] = [];
    for (const tableName of matchedTableNames) {
      const tables = await this.processTables(batch, {
        connectionTag: this.connections.connectionTag,
        schema: this.defaultSchema,
        name: tableName,
        objectId: tableName,
        replicaIdColumns: [{ name: '_id' }]
      });
      resolved.push(...tables);
    }

    return resolved;
  }

  private async getOrResolveTables(
    batch: storage.BucketStorageBatch,
    tableName: string,
    snapshotLSN: string
  ): Promise<SourceTable[]> {
    if (!this.isTableSelectedBySyncConfig(tableName)) {
      return [];
    }

    const descriptor: SourceEntityDescriptor = {
      schema: this.defaultSchema,
      connectionTag: this.connections.connectionTag,
      name: tableName,
      objectId: tableName,
      replicaIdColumns: [{ name: '_id' }]
    };

    const existing = this.relationCache.getAll(descriptor);
    if (existing) {
      return existing;
    }

    let tables = await this.processTables(batch, descriptor);
    const snapshotCandidates = tables.filter((table) => !table.snapshotComplete && table.syncAny);
    if (snapshotCandidates.length > 0) {
      this.logger.info(
        `New table discovered while streaming: [${descriptor.schema}.${descriptor.name}], applying deltas without snapshot`
      );
      const doneTables = await batch.markTableSnapshotDone(snapshotCandidates, snapshotLSN);
      const doneTableById = new Map(doneTables.map((table) => [table.id, table]));
      tables = tables.map((table) => doneTableById.get(table.id) ?? table);
      this.relationCache.updateAll(descriptor, tables);
    }

    return tables;
  }

  private isTableSelectedBySyncConfig(tableName: string): boolean {
    for (const sourceTablePattern of this.syncConfig.getSourceTables()) {
      if (sourceTablePattern.connectionTag != this.connections.connectionTag) {
        continue;
      }
      if (sourceTablePattern.schema != this.defaultSchema) {
        continue;
      }

      if (sourceTablePattern.isWildcard) {
        if (tableName.startsWith(sourceTablePattern.tablePrefix)) {
          return true;
        }
      } else if (sourceTablePattern.name == tableName) {
        return true;
      }
    }

    return false;
  }

  private async processTables(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor
  ): Promise<SourceTable[]> {
    const resolved = await batch.resolveTables({
      connection_id: this.connectionId,
      source: descriptor
    });

    if (resolved.dropTables.length > 0) {
      await batch.drop(resolved.dropTables);
    }

    this.relationCache.updateAll(descriptor, resolved.tables);
    return resolved.tables;
  }

  private async resolveTablePattern(tablePattern: TablePattern): Promise<string[]> {
    const schema = await this.connections.client.getJsonSchemas({ signal: this.abortSignal });
    const availableTableNames = schema.tables
      .map((table) => table.tableName)
      .filter((tableName) => !isConvexCheckpointTable(tableName))
      .sort();

    if (!tablePattern.isWildcard) {
      return availableTableNames.includes(tablePattern.name) ? [tablePattern.name] : [];
    }

    return availableTableNames.filter((tableName) => tableName.startsWith(tablePattern.tablePrefix));
  }

  private async writeChange(
    batch: storage.BucketStorageBatch,
    table: SourceTable,
    change: ConvexRawDocument
  ): Promise<boolean> {
    const replicaId = change._id;
    if (replicaId == null) {
      this.logger.warn(`Skipping Convex change without _id for ${table.qualifiedName}`);
      return false;
    }

    if (change._deleted) {
      await batch.save({
        tag: SaveOperationTag.DELETE,
        sourceTable: table,
        before: undefined,
        beforeReplicaId: replicaId,
        after: undefined,
        afterReplicaId: undefined
      });
      this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
      return true;
    }

    const after = this.toSqliteRow(change);
    await batch.save({
      tag: SaveOperationTag.UPDATE,
      sourceTable: table,
      before: undefined,
      beforeReplicaId: undefined,
      after,
      afterReplicaId: replicaId
    });
    this.metrics.getCounter(ReplicationMetric.ROWS_REPLICATED).add(1);
    return true;
  }

  private toSqliteRow(change: ConvexRawDocument) {
    return this.syncConfig.applyRowContext<never>(toSqliteInputRow(change));
  }

  private touch() {
    if (performance.now() - this.lastTouchedAt < 1_000) {
      return;
    }

    this.lastTouchedAt = performance.now();
    container.probes.touch().catch((error) => {
      this.logger.error(`Failed to touch the container probe: ${error instanceof Error ? error.message : `${error}`}`);
    });
  }
}

function getCacheIdentifier(source: SourceEntityDescriptor | SourceTable): string {
  const connectionTag = source instanceof SourceTable ? source.ref.connectionTag : source.connectionTag;
  return `${connectionTag}.${source.schema}.${source.name}`;
}

function readTableName(change: ConvexRawDocument): string | null {
  const table = change._table;
  if (typeof table != 'string' || table.length == 0) {
    return null;
  }
  return table;
}
