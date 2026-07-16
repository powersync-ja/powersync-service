import { BaseObserver, DO_NOT_LOG, Logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, storage, utils } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import { and, count, eq, inArray, isNull, lte, ne } from 'drizzle-orm';
import * as uuid from 'uuid';
import type { SourceTableRow } from '../drivers/sqlite/schema.js';
import type { DrizzleStorageTransaction } from '../drivers/sqlite/sqlite-config.js';
import { DrizzleBucketStorageFactory } from './DrizzleBucketStorageFactory.js';
import { currentBuckets, currentLookups, DrizzlePersistedBatch } from './DrizzlePersistedBatch.js';
import { DrizzleStorageDialect } from './DrizzleStorageDialect.js';

export interface DrizzleBucketBatchOptions {
  factory: DrizzleBucketStorageFactory;
  dialect: DrizzleStorageDialect;
  logger: Logger;
  syncRules: sync_rules.HydratedSyncConfig;
  replicationStreamId: number;
  replicationStreamName: string;
  lastCheckpointLsn: string | null;
  keepaliveOp: bigint | null;
  resumeFromLsn: string | null;
  storeCurrentData: boolean;
  skipExistingRows: boolean;
  markRecordUnavailable: storage.BucketStorageMarkRecordUnavailable | undefined;
  hooks: storage.StorageHooks | undefined;
}

const MAX_OPERATION_BATCH_COUNT = 2_000;

export class DrizzleBucketBatch
  extends BaseObserver<storage.BucketBatchStorageListener>
  implements storage.BucketStorageBatch
{
  [DO_NOT_LOG] = true;

  public last_flushed_op: bigint | null = null;
  public resumeFromLsn: string | null;
  public readonly skipExistingRows: boolean;

  private lastCheckpointLsnValue: string | null;
  private persistedOp: bigint | null;
  private readonly pendingOperations: storage.SaveOptions[] = [];
  private readonly customWriteCheckpointBatch: storage.CustomWriteCheckpointOptions[] = [];
  private needsActivation = true;

  constructor(private readonly options: DrizzleBucketBatchOptions) {
    super();
    this.lastCheckpointLsnValue = options.lastCheckpointLsn;
    this.resumeFromLsn = options.resumeFromLsn;
    this.skipExistingRows = options.skipExistingRows;
    this.persistedOp = options.keepaliveOp;
  }

  get lastCheckpointLsn(): string | null {
    return this.lastCheckpointLsnValue;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    if (this.customWriteCheckpointBatch.length > 0) {
      this.options.logger.warn('Disposing writer with unflushed custom write checkpoints');
    }
    super.clearListeners();
  }

  async dispose(): Promise<void> {
    await this[Symbol.asyncDispose]();
  }

  async resolveTables(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult> {
    const syncRules = options.syncRules ?? this.options.syncRules;
    const { connection_id, source } = options;
    const { schema, name: table, objectId, replicaIdColumns, connectionTag, sendsCompleteRows } = source;
    const normalizedReplicaIdColumns = normalizeReplicaIdColumns(replicaIdColumns);
    const relationId = { object_id: objectId };
    const { tables } = this.options.dialect;

    return this.options.dialect.transaction((tx) => {
      const existingRows = tx
        .select()
        .from(tables.sourceTables)
        .where(
          and(
            eq(tables.sourceTables.groupId, this.options.replicationStreamId),
            eq(tables.sourceTables.connectionId, connection_id)
          )
        )
        .all();

      let sourceTableRow =
        existingRows.find((row) => {
          const matchesRelationId = objectId == null || relationObjectId(row.relationId) == objectId;
          return (
            row.schemaName == schema &&
            row.tableName == table &&
            matchesRelationId &&
            jsonEquals(row.replicaIdColumns, normalizedReplicaIdColumns)
          );
        }) ?? null;

      if (sourceTableRow == null) {
        sourceTableRow = tx
          .insert(tables.sourceTables)
          .values({
            id: options.idGenerator ? String(options.idGenerator()) : uuid.v4(),
            groupId: this.options.replicationStreamId,
            connectionId: connection_id,
            relationId,
            schemaName: schema,
            tableName: table,
            replicaIdColumns: normalizedReplicaIdColumns,
            snapshotDone: false,
            snapshotTotalEstimatedCount: null,
            snapshotReplicatedCount: null,
            snapshotLastKey: null
          })
          .returning()
          .get();
      }

      const sourceTable = sourceTableFromRow(sourceTableRow, connectionTag, syncRules);
      sourceTable.storeCurrentData = sendsCompleteRows !== true;

      const dropTables = existingRows
        .filter((row) => row.id != sourceTableRow.id)
        .filter((row) => {
          const matchesTableName = row.schemaName == schema && row.tableName == table;
          return objectId == null ? matchesTableName : relationObjectId(row.relationId) == objectId || matchesTableName;
        })
        .map((row) => sourceTableFromRow(row, connectionTag, syncRules));

      return {
        tables: [sourceTable],
        dropTables
      };
    });
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    const { db, tables } = this.options.dialect;
    const row = db
      .select()
      .from(tables.sourceTables)
      .where(
        and(
          eq(tables.sourceTables.groupId, this.options.replicationStreamId),
          eq(tables.sourceTables.id, String(table.id))
        )
      )
      .get();

    return row == null ? null : sourceTableFromRow(row, table.ref.connectionTag, this.options.syncRules);
  }

  async save(record: storage.SaveOptions): Promise<storage.FlushedResult | null> {
    const { after, before, sourceTable, tag } = record;
    const storeCurrentData = this.options.storeCurrentData && sourceTable.storeCurrentData;
    for (const event of this.getTableEvents(sourceTable)) {
      this.iterateListeners((cb) =>
        cb.replicationEvent?.({
          batch: this,
          table: sourceTable,
          data: {
            op: tag,
            after: after && utils.isCompleteRow(storeCurrentData, after) ? after : undefined,
            before: before && utils.isCompleteRow(storeCurrentData, before) ? before : undefined
          },
          event
        })
      );
    }

    if (!sourceTable.syncData && !sourceTable.syncParameters) {
      return null;
    }

    this.pendingOperations.push(record);
    if (this.pendingOperations.length >= MAX_OPERATION_BATCH_COUNT) {
      return this.flush();
    }
    return null;
  }

  async truncate(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    await this.flush();

    let nextOpId = 0n;
    let firstOpId = 0n;

    this.options.dialect.transaction((tx) => {
      nextOpId = this.getNextOpId(tx);
      firstOpId = nextOpId;
      const persistedBatch = this.createPersistedBatch(tx);
      for (const table of sourceTables) {
        if (!table.syncData && !table.syncParameters) {
          continue;
        }

        const rows = tx
          .select()
          .from(this.options.dialect.tables.currentData)
          .where(
            and(
              eq(this.options.dialect.tables.currentData.groupId, this.options.replicationStreamId),
              eq(this.options.dialect.tables.currentData.sourceTable, String(table.id))
            )
          )
          .all();

        for (const row of rows) {
          const sourceKey = storage.deserializeReplicaId(Buffer.from(row.sourceKey));
          if (table.syncData) {
            nextOpId = persistedBatch.persistBucketData({
              table,
              sourceKey,
              existingBuckets: currentBuckets(row),
              evaluated: [],
              nextOpId
            });
          }

          if (table.syncParameters) {
            nextOpId = persistedBatch.persistParameterData({
              table,
              sourceKey,
              existingLookups: currentLookups(row),
              evaluated: [],
              nextOpId
            });
          }
        }
        tx.delete(this.options.dialect.tables.currentData)
          .where(
            and(
              eq(this.options.dialect.tables.currentData.groupId, this.options.replicationStreamId),
              eq(this.options.dialect.tables.currentData.sourceTable, String(table.id))
            )
          )
          .run();
      }
      this.setNextOpId(tx, nextOpId);
    });

    if (nextOpId == firstOpId) {
      return null;
    }

    const lastOpId = nextOpId - 1n;
    this.persistedOp = lastOpId;
    this.last_flushed_op = lastOpId;
    this.logFlushedOperations({
      operationCount: sourceTables.length,
      firstOpId,
      nextOpId,
      operationLabel: 'truncate source table'
    });
    return { flushed_op: lastOpId };
  }

  async drop(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    const result = await this.truncate(sourceTables);
    this.options.dialect.transaction((tx) => {
      for (const table of sourceTables) {
        tx.delete(this.options.dialect.tables.sourceTables)
          .where(
            and(
              eq(this.options.dialect.tables.sourceTables.groupId, this.options.replicationStreamId),
              eq(this.options.dialect.tables.sourceTables.id, String(table.id))
            )
          )
          .run();
      }
    });
    return result;
  }

  async flush(_options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
    let result: storage.FlushedResult | null = null;
    if (this.pendingOperations.length > 0) {
      await this.options.hooks?.beforeBatchFlush?.(this);
      const operations = this.pendingOperations.splice(0);
      let nextOpId = 0n;
      let firstOpId: bigint | null = null;
      for (const batch of chunked(operations, MAX_OPERATION_BATCH_COUNT)) {
        this.options.dialect.transaction((tx) => {
          nextOpId = this.getNextOpId(tx);
          firstOpId ??= nextOpId;
          const persistedBatch = this.createPersistedBatch(tx);
          nextOpId = persistedBatch.persistOperations(batch, nextOpId);
          this.setNextOpId(tx, nextOpId);
        });
      }
      const lastOpId = nextOpId - 1n;
      this.persistedOp = lastOpId;
      this.last_flushed_op = lastOpId;
      result = { flushed_op: lastOpId };
      this.logFlushedOperations({
        operationCount: operations.length,
        firstOpId: firstOpId!,
        nextOpId
      });
      await this.options.hooks?.afterBatchFlush?.(this);
    }

    await this.flushCustomWriteCheckpoints();
    return result;
  }

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    await this.flush();
    const createEmptyCheckpoints = options?.createEmptyCheckpoints ?? true;
    const now = new Date();
    const { tables } = this.options.dialect;

    const result = this.options.dialect.transaction((tx) => {
      const syncRulesRow = tx
        .select()
        .from(tables.syncRules)
        .where(eq(tables.syncRules.id, this.options.replicationStreamId))
        .get();
      if (syncRulesRow == null) {
        throw new Error(`Missing replication stream ${this.options.replicationStreamId}`);
      }

      const canCheckpoint =
        syncRulesRow.snapshotDone === true &&
        (syncRulesRow.lastCheckpointLsn == null || syncRulesRow.lastCheckpointLsn <= lsn) &&
        (syncRulesRow.noCheckpointBefore == null || syncRulesRow.noCheckpointBefore <= lsn);

      let checkpointCreated = false;

      if (canCheckpoint) {
        const newLastCheckpoint = maxBigint(
          syncRulesRow.lastCheckpoint,
          this.persistedOp,
          syncRulesRow.keepaliveOp,
          0n
        );
        const changed = syncRulesRow.lastCheckpoint !== newLastCheckpoint || syncRulesRow.keepaliveOp != null;

        if (changed || createEmptyCheckpoints) {
          tx.update(tables.syncRules)
            .set({
              lastCheckpointLsn: lsn,
              lastCheckpointTs: now,
              lastKeepaliveTs: now,
              lastFatalError: null,
              keepaliveOp: null,
              lastCheckpoint: newLastCheckpoint,
              snapshotLsn: null
            })
            .where(eq(tables.syncRules.id, this.options.replicationStreamId))
            .run();
          checkpointCreated = true;
          tx.delete(tables.currentData)
            .where(
              and(
                eq(tables.currentData.groupId, this.options.replicationStreamId),
                lte(tables.currentData.pendingDelete, newLastCheckpoint)
              )
            )
            .run();
        } else {
          tx.update(tables.syncRules)
            .set({ lastKeepaliveTs: now })
            .where(eq(tables.syncRules.id, this.options.replicationStreamId))
            .run();
        }
      } else {
        tx.update(tables.syncRules)
          .set({
            keepaliveOp: maxBigint(syncRulesRow.keepaliveOp, this.persistedOp, 0n),
            lastKeepaliveTs: now
          })
          .where(eq(tables.syncRules.id, this.options.replicationStreamId))
          .run();
      }

      return {
        checkpointBlocked: !canCheckpoint,
        checkpointCreated
      };
    });

    if (!result.checkpointBlocked) {
      await this.autoActivate(lsn);
    }

    this.persistedOp = null;
    this.lastCheckpointLsnValue = lsn;
    this.options.factory.checkpointWatcher.notify();
    return result;
  }

  keepalive(lsn: string): Promise<storage.CheckpointResult> {
    return this.commit(lsn, { createEmptyCheckpoints: true });
  }

  async setResumeLsn(lsn: string): Promise<void> {
    const { db, tables } = this.options.dialect;
    const result = db
      .update(tables.syncRules)
      .set({ snapshotLsn: lsn })
      .where(eq(tables.syncRules.id, this.options.replicationStreamId))
      .run();
    if (result.changes == 0) {
      throw new Error(`Missing replication stream ${this.options.replicationStreamId}`);
    }
    this.resumeFromLsn = lsn;
  }

  async markAllSnapshotDone(noCheckpointBeforeLsn: string): Promise<void> {
    await this.markSnapshotDoneInternal(noCheckpointBeforeLsn);
  }

  async markSnapshotDone(noCheckpointBeforeLsn: string, options?: { throwOnConflict?: boolean }): Promise<void> {
    const { db, tables } = this.options.dialect;
    const remaining =
      db
        .select({ value: count() })
        .from(tables.sourceTables)
        .where(
          and(
            eq(tables.sourceTables.groupId, this.options.replicationStreamId),
            eq(tables.sourceTables.snapshotDone, false)
          )
        )
        .get()?.value ?? 0;

    if (remaining > 0) {
      if (options?.throwOnConflict ?? true) {
        throw new ReplicationAssertionError(
          `Cannot mark snapshot done while ${remaining} source table${remaining == 1 ? '' : 's'} still require snapshotting`
        );
      }
      return;
    }

    await this.markSnapshotDoneInternal(noCheckpointBeforeLsn);
  }

  async markTableSnapshotRequired(_table: storage.SourceTable): Promise<void> {
    const { db, tables } = this.options.dialect;
    const result = db
      .update(tables.syncRules)
      .set({ snapshotDone: false })
      .where(eq(tables.syncRules.id, this.options.replicationStreamId))
      .run();
    if (result.changes == 0) {
      throw new Error(`Missing replication stream ${this.options.replicationStreamId}`);
    }
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    noCheckpointBeforeLsn?: string
  ): Promise<storage.SourceTable[]> {
    const ids = tables.map((table) => String(table.id));
    this.options.dialect.transaction((tx) => {
      if (ids.length > 0) {
        tx.update(this.options.dialect.tables.sourceTables)
          .set({
            snapshotDone: true,
            snapshotTotalEstimatedCount: null,
            snapshotReplicatedCount: null,
            snapshotLastKey: null
          })
          .where(inArray(this.options.dialect.tables.sourceTables.id, ids))
          .run();
      }

      if (noCheckpointBeforeLsn != null) {
        this.assignNoCheckpointBefore(tx, noCheckpointBeforeLsn);
      }
    });

    return tables.map((table) => {
      const copy = table.clone();
      copy.snapshotComplete = true;
      copy.snapshotStatus = undefined;
      return copy;
    });
  }

  async updateTableProgress(
    table: storage.SourceTable,
    progress: Partial<storage.TableSnapshotStatus>
  ): Promise<storage.SourceTable> {
    const copy = table.clone();
    const snapshotStatus = {
      totalEstimatedCount: progress.totalEstimatedCount ?? copy.snapshotStatus?.totalEstimatedCount ?? 0,
      replicatedCount: progress.replicatedCount ?? copy.snapshotStatus?.replicatedCount ?? 0,
      lastKey: progress.lastKey ?? copy.snapshotStatus?.lastKey ?? null
    };
    copy.snapshotStatus = snapshotStatus;

    const { db, tables } = this.options.dialect;
    const result = db
      .update(tables.sourceTables)
      .set({
        snapshotTotalEstimatedCount: BigInt(snapshotStatus.totalEstimatedCount),
        snapshotReplicatedCount: BigInt(snapshotStatus.replicatedCount),
        snapshotLastKey: snapshotStatus.lastKey == null ? null : Buffer.from(snapshotStatus.lastKey)
      })
      .where(eq(tables.sourceTables.id, String(table.id)))
      .run();
    if (result.changes == 0) {
      throw new Error(`Missing source table ${table.id}`);
    }

    return copy;
  }

  addCustomWriteCheckpoint(checkpoint: storage.BatchedCustomWriteCheckpointOptions): void {
    this.customWriteCheckpointBatch.push({
      ...checkpoint,
      sync_rules_id: this.options.replicationStreamId
    });
  }

  private async flushCustomWriteCheckpoints(): Promise<void> {
    if (this.customWriteCheckpointBatch.length == 0) {
      return;
    }

    const batch = this.customWriteCheckpointBatch.splice(0);
    this.options.dialect.transaction((tx) => {
      for (const checkpoint of batch) {
        const table = this.options.dialect.tables.writeCheckpoints;
        const existing = tx
          .select()
          .from(table)
          .where(
            and(
              eq(table.userId, checkpoint.user_id),
              checkpoint.sync_rules_id == null
                ? isNull(table.syncRulesId)
                : eq(table.syncRulesId, checkpoint.sync_rules_id)
            )
          )
          .get();

        if (existing == null) {
          tx.insert(table)
            .values({
              id: uuid.v4(),
              syncRulesId: checkpoint.sync_rules_id,
              userId: checkpoint.user_id,
              checkpoint: checkpoint.checkpoint,
              heads: null,
              createdAt: new Date()
            })
            .run();
        } else {
          tx.update(table)
            .set({
              checkpoint: checkpoint.checkpoint,
              createdAt: new Date()
            })
            .where(eq(table.id, existing.id))
            .run();
        }
      }
    });
    this.options.factory.checkpointWatcher.notify();
  }

  private createPersistedBatch(tx: DrizzleStorageTransaction): DrizzlePersistedBatch {
    return new DrizzlePersistedBatch({
      tx,
      dialect: this.options.dialect,
      logger: this.options.logger,
      syncRules: this.options.syncRules,
      replicationStreamId: this.options.replicationStreamId,
      storeCurrentData: this.options.storeCurrentData,
      skipExistingRows: this.options.skipExistingRows,
      markRecordUnavailable: this.options.markRecordUnavailable
    });
  }

  private getNextOpId(tx: DrizzleStorageTransaction): bigint {
    const row = tx
      .select()
      .from(this.options.dialect.tables.opIdSequence)
      .where(eq(this.options.dialect.tables.opIdSequence.id, 1))
      .get();
    if (row == null) {
      throw new Error('Missing op ID sequence state');
    }
    return row.nextOpId;
  }

  private setNextOpId(tx: DrizzleStorageTransaction, nextOpId: bigint): void {
    tx.update(this.options.dialect.tables.opIdSequence)
      .set({ nextOpId })
      .where(eq(this.options.dialect.tables.opIdSequence.id, 1))
      .run();
  }

  private logFlushedOperations(options: {
    operationCount: number;
    firstOpId: bigint;
    nextOpId: bigint;
    operationLabel?: string;
  }): void {
    const storageOperationCount = options.nextOpId - options.firstOpId;
    const operationLabel = options.operationLabel ?? 'source operation';
    const pluralizedOperationLabel = options.operationCount == 1 ? operationLabel : `${operationLabel}s`;

    if (storageOperationCount == 0n) {
      this.options.logger.info(
        `[${this.options.replicationStreamName}] Flushed ${options.operationCount} ${pluralizedOperationLabel} to Drizzle storage DB with no new storage ops`
      );
      return;
    }

    this.options.logger.info(
      `[${this.options.replicationStreamName}] Flushed ${options.operationCount} ${pluralizedOperationLabel} to Drizzle storage DB as ${storageOperationCount.toString()} storage ops (${options.firstOpId.toString()}-${(options.nextOpId - 1n).toString()})`
    );
  }

  private async markSnapshotDoneInternal(noCheckpointBeforeLsn: string): Promise<void> {
    this.options.dialect.transaction((tx) => {
      this.assignNoCheckpointBefore(tx, noCheckpointBeforeLsn);
    });
    this.options.factory.checkpointWatcher.notify();
  }

  private assignNoCheckpointBefore(tx: DrizzleStorageTransaction, noCheckpointBeforeLsn: string): void {
    const table = this.options.dialect.tables.syncRules;
    const row = tx.select().from(table).where(eq(table.id, this.options.replicationStreamId)).get();
    if (row == null) {
      throw new Error(`Missing replication stream ${this.options.replicationStreamId}`);
    }
    tx.update(table)
      .set({
        snapshotDone: true,
        lastKeepaliveTs: new Date(),
        noCheckpointBefore:
          row.noCheckpointBefore == null || row.noCheckpointBefore < noCheckpointBeforeLsn
            ? noCheckpointBeforeLsn
            : row.noCheckpointBefore
      })
      .where(eq(table.id, this.options.replicationStreamId))
      .run();
  }

  private async autoActivate(lsn: string): Promise<void> {
    if (!this.needsActivation) {
      return;
    }

    let didActivate = false;
    this.options.dialect.transaction((tx) => {
      const table = this.options.dialect.tables.syncRules;
      const syncRulesRow = tx.select().from(table).where(eq(table.id, this.options.replicationStreamId)).get();

      if (syncRulesRow?.state == storage.SyncRuleState.PROCESSING && syncRulesRow.snapshotDone) {
        tx.update(table)
          .set({ state: storage.SyncRuleState.ACTIVE })
          .where(eq(table.id, this.options.replicationStreamId))
          .run();
        tx.update(table)
          .set({ state: storage.SyncRuleState.STOP })
          .where(
            and(
              ne(table.id, this.options.replicationStreamId),
              inArray(table.state, [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED])
            )
          )
          .run();
        didActivate = true;
        this.needsActivation = false;
      } else if (syncRulesRow?.state != storage.SyncRuleState.PROCESSING) {
        this.needsActivation = false;
      }
    });

    if (didActivate) {
      this.options.logger.info(`Activated new replication stream at ${lsn}`);
    }
  }

  private getTableEvents(table: storage.SourceTable): sync_rules.SqlEventDescriptor[] {
    return this.options.syncRules.eventDescriptors.filter((event) =>
      [...event.getSourceTables()].some((sourceTable) => sourceTable.matches(table.ref))
    );
  }
}

function sourceTableFromRow(
  row: SourceTableRow,
  connectionTag: string,
  syncRules: sync_rules.HydratedSyncConfig
): storage.SourceTable {
  const ref = { connectionTag, schema: row.schemaName, name: row.tableName };
  const sourceTable = new storage.SourceTable({
    id: row.id,
    ref,
    objectId: relationObjectId(row.relationId),
    replicaIdColumns: replicaIdColumns(row.replicaIdColumns),
    snapshotComplete: row.snapshotDone ?? true,
    ...syncRules.getMatchingSources(ref)
  });

  if (!sourceTable.snapshotComplete) {
    sourceTable.snapshotStatus = {
      totalEstimatedCount: Number(row.snapshotTotalEstimatedCount ?? -1n),
      replicatedCount: Number(row.snapshotReplicatedCount ?? 0n),
      lastKey: row.snapshotLastKey ?? null
    };
  }

  sourceTable.syncEvent = syncRules.tableTriggersEvent(ref);
  sourceTable.syncData = sourceTable.bucketDataSources.length > 0;
  sourceTable.syncParameters = sourceTable.parameterLookupSources.length > 0;
  return sourceTable;
}

function normalizeReplicaIdColumns(replicaIdColumns: ColumnDescriptor[]): ColumnDescriptor[] {
  return replicaIdColumns.map((column) => ({
    name: column.name,
    type: column.type,
    typeId: typeof column.typeId === 'undefined' ? column.typeId : Number(column.typeId)
  }));
}

function replicaIdColumns(value: unknown): ColumnDescriptor[] {
  return Array.isArray(value) ? (value as ColumnDescriptor[]) : [];
}

function relationObjectId(value: unknown): string | number | undefined {
  if (value == null || typeof value != 'object' || Array.isArray(value)) {
    return undefined;
  }
  const objectId = (value as Record<string, unknown>).object_id;
  return typeof objectId == 'string' || typeof objectId == 'number' ? objectId : undefined;
}

function jsonEquals(left: unknown, right: unknown): boolean {
  return JSON.stringify(left ?? null) == JSON.stringify(right ?? null);
}

function maxBigint(...values: (bigint | null | undefined)[]): bigint {
  return values.reduce<bigint>((max, value) => (value != null && value > max ? value : max), 0n);
}

function* chunked<T>(items: T[], size: number): Generator<T[]> {
  for (let index = 0; index < items.length; index += size) {
    yield items.slice(index, index + size);
  }
}
