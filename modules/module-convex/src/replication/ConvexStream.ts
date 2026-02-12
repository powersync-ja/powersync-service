import {
  container,
  DatabaseConnectionError,
  ErrorCode,
  Logger,
  logger as defaultLogger,
  ReplicationAbortedError,
  ReplicationAssertionError
} from '@powersync/lib-services-framework';
import {
  MetricsEngine,
  RelationCache,
  SaveOperationTag,
  SourceEntityDescriptor,
  SourceTable,
  storage
} from '@powersync/service-core';
import { HydratedSyncRules, SqliteInputRow, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { setTimeout as delay } from 'timers/promises';
import {
  ConvexListSnapshotResult,
  ConvexRawDocument,
  ConvexTableSchema,
  isCursorExpiredError
} from '../client/ConvexApiClient.js';
import { isConvexCheckpointTable } from '../common/ConvexCheckpoints.js';
import { ConvexLSN } from '../common/ConvexLSN.js';
import { ConvexConnectionManager } from './ConvexConnectionManager.js';

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
  private readonly syncRules: HydratedSyncRules;
  private readonly logger: Logger;

  private readonly relationCache = new RelationCache(getCacheIdentifier);

  private tableSchemaCache: ConvexTableSchema[] | null = null;

  private oldestUncommittedChange: Date | null = null;
  private lastKeepaliveAt = 0;
  private isStartingReplication = true;
  private lastTouchedAt = performance.now();

  constructor(private readonly options: ConvexStreamOptions) {
    this.storage = options.storage;
    this.metrics = options.metrics;
    this.syncRules = options.storage.getParsedSyncRules({ defaultSchema: options.connections.schema });
    this.logger = options.logger ?? defaultLogger;
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
    return this.connections.config.pollingIntervalMs;
  }

  get stopped() {
    return this.abortSignal.aborted;
  }

  async replicate() {
    try {
      await this.initReplication();
      await this.streamChanges();
    } catch (error) {
      await this.storage.reportError(error);
      throw error;
    }
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
    await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: ConvexLSN.ZERO.comparable,
        defaultSchema: this.defaultSchema,
        storeCurrentData: false,
        skipExistingRows: false
      },
      async (batch) => {
        let resumeFromLsn = batch.resumeFromLsn;
        if (resumeFromLsn == null) {
          throw new ReplicationAssertionError(`No LSN found to resume replication from.`);
        }

        // Resolve source tables up-front to warm table metadata and sync-rule matching.
        await this.resolveAllSourceTables(batch);

        let cursor = ConvexLSN.fromSerialized(resumeFromLsn).toCursorString();

        while (!this.abortSignal.aborted) {
          const page = await this.connections.client
            .documentDeltas({
              cursor,
              signal: this.abortSignal
            })
            .catch((error) => {
              if (isCursorExpiredError(error)) {
                throw new ConvexCursorExpiredError('Convex cursor expired; initial replication restart required', error);
              }
              throw error;
            });

          const nextCursor = page.cursor;
          const pageLsn = ConvexLSN.fromCursor(nextCursor).comparable;

          let changesInPage = 0;
          let sawCheckpointMarker = false;
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

            const table = await this.getOrResolveTable(batch, tableName);
            if (table == null || !table.syncAny) {
              continue;
            }

            const changed = await this.writeChange(batch, table, change);
            if (!changed) {
              continue;
            }

            changesInPage += 1;
            if (this.oldestUncommittedChange == null) {
              this.oldestUncommittedChange = new Date();
            }
          }

          if (changesInPage > 0) {
            const didCommit = await batch.commit(pageLsn, {
              createEmptyCheckpoints: false,
              oldestUncommittedChange: this.oldestUncommittedChange
            });

            if (didCommit) {
              this.metrics.getCounter(ReplicationMetric.TRANSACTIONS_REPLICATED).add(1);
              this.oldestUncommittedChange = null;
              this.isStartingReplication = false;
            }
          } else if (sawCheckpointMarker) {
            await batch.keepalive(pageLsn);
            this.lastKeepaliveAt = Date.now();
            this.isStartingReplication = false;
          } else if (nextCursor != cursor && Date.now() - this.lastKeepaliveAt > 60_000) {
            await batch.keepalive(pageLsn);
            this.lastKeepaliveAt = Date.now();
            this.isStartingReplication = false;
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
    );
  }

  async getReplicationLagMillis(): Promise<number | undefined> {
    if (this.oldestUncommittedChange == null) {
      if (this.isStartingReplication) {
        return undefined;
      }
      return 0;
    }

    return Date.now() - this.oldestUncommittedChange.getTime();
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
      snapshotLsn: status.snapshot_lsn
    };
  }

  private async initialReplication(snapshotLsn: string | null) {
    const flushResult = await this.storage.startBatch(
      {
        logger: this.logger,
        zeroLSN: ConvexLSN.ZERO.comparable,
        defaultSchema: this.defaultSchema,
        storeCurrentData: false,
        skipExistingRows: true
      },
      async (batch) => {
        const snapshotCursor = await this.resolveSnapshotBoundary(snapshotLsn);
        const snapshotComparable = ConvexLSN.fromCursor(snapshotCursor).comparable;
        await batch.setResumeLsn(snapshotComparable);

        const sourceTables = await this.resolveAllSourceTables(batch);

        for (const sourceTable of sourceTables) {
          if (sourceTable.snapshotComplete) {
            this.logger.info(`Skipping table [${sourceTable.qualifiedName}] - snapshot already done.`);
            continue;
          }

          const tableWithProgress = await batch.updateTableProgress(sourceTable, {
            totalEstimatedCount: -1,
            replicatedCount: 0,
            lastKey: null
          });
          this.relationCache.update(tableWithProgress);
          this.logger.info(`Starting table snapshot from first page for [${tableWithProgress.qualifiedName}]`);

          await this.snapshotTable(batch, tableWithProgress, snapshotCursor);
        }

        await batch.commit(snapshotComparable);

        this.logger.info(`Snapshot done. Need to replicate from ${snapshotComparable} for consistency.`);
      }
    );

    return {
      lastOpId: flushResult?.flushed_op
    };
  }

  private async snapshotTable(
    batch: storage.BucketStorageBatch,
    table: SourceTable,
    snapshotCursor: string
  ): Promise<{ table: SourceTable }> {
    let pageCursor: string | null = null;
    let replicatedCount = 0;
    let latestTable = table;
    let firstPage = true;

    while (!this.abortSignal.aborted) {
      const requestCursor: string | undefined = firstPage ? undefined : (pageCursor ?? undefined);
      const page: ConvexListSnapshotResult = await this.connections.client
        .listSnapshot({
          tableName: table.name,
          snapshot: snapshotCursor,
          cursor: requestCursor,
          signal: this.abortSignal
        })
        .catch((error) => {
          if (isCursorExpiredError(error)) {
            throw new ConvexCursorExpiredError('Convex snapshot cursor expired; restart required', error);
          }
          throw error;
        });
      firstPage = false;

      if (snapshotCursor != page.snapshot) {
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
        lastKey: encodeSnapshotProgressCursor(pageCursor)
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

    const snapshotComparable = ConvexLSN.fromCursor(snapshotCursor).comparable;
    const [doneTable] = await batch.markSnapshotDone([latestTable], snapshotComparable);
    this.relationCache.update(doneTable);

    return {
      table: doneTable
    };
  }

  private async resolveSnapshotBoundary(snapshotLsn: string | null): Promise<string> {
    if (snapshotLsn != null) {
      const snapshotCursor = ConvexLSN.fromSerialized(snapshotLsn).toCursorString();
      this.logger.info(`Using existing global snapshot ${snapshotCursor}`);
      return snapshotCursor;
    }

    const snapshotCursor = await this.connections.client.getGlobalSnapshotCursor({ signal: this.abortSignal });
    this.logger.info(`Pinned global snapshot ${snapshotCursor}`);
    return snapshotCursor;
  }

  private async resolveAllSourceTables(batch: storage.BucketStorageBatch): Promise<SourceTable[]> {
    const sourceTables = this.syncRules.getSourceTables();
    const resolved: SourceTable[] = [];

    for (const tablePattern of sourceTables) {
      const tables = await this.resolveQualifiedTableNames(batch, tablePattern);
      resolved.push(...tables);
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

    const availableTableNames = (await this.getAllTableSchemas()).map((table) => table.tableName);

    const matchedTableNames = availableTableNames
      .filter((tableName) => {
        if (tablePattern.isWildcard) {
          return tableName.startsWith(tablePattern.tablePrefix);
        }
        return tableName == tablePattern.name;
      })
      .filter((tableName) => !isConvexCheckpointTable(tableName))
      .sort();

    if (!tablePattern.isWildcard && matchedTableNames.length == 0) {
      this.logger.warn(`Table ${tablePattern.schema}.${tablePattern.name} not found`);
    }

    const resolved: SourceTable[] = [];
    for (const tableName of matchedTableNames) {
      const table = await this.processTable(
        batch,
        {
          schema: this.defaultSchema,
          name: tableName,
          objectId: tableName,
          replicaIdColumns: [{ name: '_id' }]
        },
        false
      );
      resolved.push(table);
    }

    return resolved;
  }

  private async getOrResolveTable(batch: storage.BucketStorageBatch, tableName: string): Promise<SourceTable | null> {
    const descriptor: SourceEntityDescriptor = {
      schema: this.defaultSchema,
      name: tableName,
      objectId: tableName,
      replicaIdColumns: [{ name: '_id' }]
    };

    const existing = this.relationCache.get(descriptor);
    if (existing) {
      return existing;
    }

    if (!this.isTableSelectedBySyncRules(tableName)) {
      return null;
    }

    // Refresh schema cache when we discover a new table while streaming.
    await this.getAllTableSchemas({ force: true });

    return await this.processTable(batch, descriptor, false);
  }

  private isTableSelectedBySyncRules(tableName: string): boolean {
    for (const sourceTablePattern of this.syncRules.getSourceTables()) {
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

  private async processTable(
    batch: storage.BucketStorageBatch,
    descriptor: SourceEntityDescriptor,
    snapshot: boolean
  ): Promise<SourceTable> {
    const resolved = await this.storage.resolveTable({
      group_id: this.storage.group_id,
      connection_id: Number.parseInt(this.connections.connectionId) || 1,
      connection_tag: this.connections.connectionTag,
      entity_descriptor: descriptor,
      sync_rules: this.syncRules
    });

    if (resolved.dropTables.length > 0) {
      await batch.drop(resolved.dropTables);
    }

    this.relationCache.update(resolved.table);

    if (snapshot && !resolved.table.snapshotComplete && resolved.table.syncAny) {
      await batch.truncate([resolved.table]);
    }

    return resolved.table;
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
    const row: SqliteInputRow = {};

    for (const [key, value] of Object.entries(change)) {
      if (key == '_table' || key == '_deleted') {
        continue;
      }
      row[key] = toConvexSyncValue(value);
    }

    return this.syncRules.applyRowContext<never>(row);
  }

  private async getAllTableSchemas(options?: { force?: boolean }): Promise<ConvexTableSchema[]> {
    if (!options?.force && this.tableSchemaCache != null) {
      return this.tableSchemaCache;
    }

    const schema = await this.connections.client.getJsonSchemas({ signal: this.abortSignal });
    this.tableSchemaCache = schema.tables;
    return schema.tables;
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
  if (source instanceof SourceTable) {
    return `${source.schema}.${source.name}`;
  }

  return `${source.schema}.${source.name}`;
}

function readTableName(change: ConvexRawDocument, fallback?: string): string | null {
  const table = change._table ?? change.tableName ?? change.table_name ?? fallback;
  if (typeof table != 'string' || table.length == 0) {
    return null;
  }
  return table;
}

function toConvexSyncValue(value: unknown): any {
  if (value == null) {
    return null;
  }

  if (typeof value == 'string') {
    return value;
  }

  if (typeof value == 'number') {
    if (Number.isInteger(value)) {
      return BigInt(value);
    }
    return value;
  }

  if (typeof value == 'bigint') {
    return value;
  }

  if (typeof value == 'boolean') {
    return value ? 1n : 0n;
  }

  if (value instanceof Uint8Array) {
    return value;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (Array.isArray(value) || typeof value == 'object') {
    return JSON.stringify(value);
  }

  return null;
}

function encodeSnapshotProgressCursor(cursor: string | null): Uint8Array | null {
  if (cursor == null) {
    return null;
  }

  return Buffer.from(cursor, 'utf8');
}
