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
import { HydratedSyncRules, TablePattern } from '@powersync/service-sync-rules';
import { ReplicationMetric } from '@powersync/service-types';
import { setTimeout as delay } from 'timers/promises';
import {
  ConvexListSnapshotResult,
  ConvexRawDocument,
  ConvexTableSchema,
  isCursorExpiredError
} from '../client/ConvexApiClient.js';
import { isConvexCheckpointTable } from '../common/ConvexCheckpoints.js';
import { parseConvexLsn, toConvexLsn, ZERO_LSN } from '../common/ConvexLSN.js';
import { extractProperties, toSqliteInputRow } from '../common/convex-to-sqlite.js';
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
  private tableSchemaPropertiesByName = new Map<string, Record<string, unknown>>();

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
        zeroLSN: ZERO_LSN,
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

        let cursor = parseConvexLsn(resumeFromLsn);

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
          const pageLsn = toConvexLsn(nextCursor);

          let changesInPage = 0;
          let sawCheckpointMarker = false;
          const snapshottedTablesInPage = new Set<string>();
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

            const table = await this.getOrResolveTable(batch, tableName, nextCursor, snapshottedTablesInPage);
            if (table == null || !table.syncAny) {
              continue;
            }
            if (snapshottedTablesInPage.has(tableName)) {
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
        zeroLSN: ZERO_LSN,
        defaultSchema: this.defaultSchema,
        storeCurrentData: false,
        skipExistingRows: true
      },
      async (batch) => {
        const snapshotCursor = await this.resolveSnapshotBoundary(snapshotLsn);
        const snapshotLsnValue = toConvexLsn(snapshotCursor);
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
    const tableProperties = this.getTableSchemaProperties(table.name);
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

        const row = this.toSqliteRow(rawDocument, tableProperties);
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
        lastKey: encodeSnapshotProgressCursor({
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
    const snapshotLsnValue = toConvexLsn(snapshotCursor);
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
      const table = await this.processTable(batch, {
        schema: this.defaultSchema,
        name: tableName,
        objectId: tableName,
        replicaIdColumns: [{ name: '_id' }]
      });
      resolved.push(table);
    }

    return resolved;
  }

  private async getOrResolveTable(
    batch: storage.BucketStorageBatch,
    tableName: string,
    snapshotCursor: string,
    snapshottedTablesInPage: Set<string>
  ): Promise<SourceTable | null> {
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

    await this.getAllTableSchemas({ force: true });

    let table = await this.processTable(batch, descriptor);
    if (!table.snapshotComplete && table.syncAny) {
      this.logger.info(`New table discovered while streaming: [${table.qualifiedName}]`);
      await batch.truncate([table]);
      table = await batch.updateTableProgress(table, {
        totalEstimatedCount: -1,
        replicatedCount: 0,
        lastKey: null
      });
      this.relationCache.update(table);
      table = (await this.snapshotTable(batch, table, snapshotCursor)).table;
      snapshottedTablesInPage.add(tableName);
    }

    return table;
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
    descriptor: SourceEntityDescriptor
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

    const after = this.toSqliteRow(change, this.getTableSchemaProperties(table.name));
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

  private toSqliteRow(change: ConvexRawDocument, properties?: Record<string, unknown>) {
    return this.syncRules.applyRowContext<never>(toSqliteInputRow(change, properties));
  }

  private async getAllTableSchemas(options?: { force?: boolean }): Promise<ConvexTableSchema[]> {
    if (!options?.force && this.tableSchemaCache != null) {
      return this.tableSchemaCache;
    }

    const schema = await this.connections.client.getJsonSchemas({ signal: this.abortSignal });
    this.tableSchemaCache = schema.tables;
    this.tableSchemaPropertiesByName = new Map(
      schema.tables.map((table) => [table.tableName, extractProperties(table.schema)])
    );
    return schema.tables;
  }

  private getTableSchemaProperties(tableName: string): Record<string, unknown> | undefined {
    return this.tableSchemaPropertiesByName.get(tableName);
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
  return `${source.schema}.${source.name}`;
}

function readTableName(change: ConvexRawDocument): string | null {
  const table = change._table;
  if (typeof table != 'string' || table.length == 0) {
    return null;
  }
  return table;
}

interface ConvexSnapshotProgressCursor {
  cursor: string | null;
  finished: boolean;
}

const SNAPSHOT_PROGRESS_PREFIX = 'convex-snapshot-progress:';

function encodeSnapshotProgressCursor(progress: ConvexSnapshotProgressCursor): Uint8Array | null {
  if (!progress.finished && progress.cursor == null) {
    return null;
  }

  if (!progress.finished) {
    return Buffer.from(progress.cursor!, 'utf8');
  }

  return Buffer.from(`${SNAPSHOT_PROGRESS_PREFIX}${JSON.stringify(progress)}`, 'utf8');
}

function decodeSnapshotProgressCursor(value: Uint8Array | null | undefined): ConvexSnapshotProgressCursor {
  if (value == null) {
    return {
      cursor: null,
      finished: false
    };
  }

  const serialized = Buffer.from(value).toString('utf8');
  if (!serialized.startsWith(SNAPSHOT_PROGRESS_PREFIX)) {
    return {
      cursor: serialized,
      finished: false
    };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(serialized.slice(SNAPSHOT_PROGRESS_PREFIX.length));
  } catch (error) {
    throw new ReplicationAssertionError(
      `Convex snapshot progress cursor is not valid JSON: ${error instanceof Error ? error.message : `${error}`}`
    );
  }

  if (typeof parsed != 'object' || parsed == null || Array.isArray(parsed)) {
    throw new ReplicationAssertionError('Convex snapshot progress cursor must decode to an object');
  }

  const parsedProgress = parsed as { cursor?: unknown; finished?: unknown };
  const cursor = parsedProgress.cursor;
  const finished = parsedProgress.finished;
  if (cursor != null && typeof cursor != 'string') {
    throw new ReplicationAssertionError('Convex snapshot progress cursor must contain a string cursor or null');
  }
  if (typeof finished != 'boolean') {
    throw new ReplicationAssertionError('Convex snapshot progress cursor must contain a boolean finished flag');
  }

  return {
    cursor: cursor ?? null,
    finished
  };
}
