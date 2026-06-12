import { EntityManager, MikroORM } from '@mikro-orm/core';
import { BaseObserver, DO_NOT_LOG, Logger, ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, storage, utils } from '@powersync/service-core';
import * as sync_rules from '@powersync/service-sync-rules';
import * as uuid from 'uuid';
import { SourceTable as SourceTableEntity } from '../entities/entities-index.js';
import { MikroOrmBucketStorageFactory } from './MikroOrmBucketStorageFactory.js';
import { currentBuckets, currentLookups, MikroOrmPersistedBatch } from './MikroOrmPersistedBatch.js';
import { MikroOrmStorageDialect } from './MikroOrmStorageDialect.js';

export interface MikroOrmBucketBatchOptions {
  factory: MikroOrmBucketStorageFactory;
  orm: MikroORM;
  dialect: MikroOrmStorageDialect;
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

export class MikroOrmBucketBatch
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

  constructor(private readonly options: MikroOrmBucketBatchOptions) {
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
    const em = this.options.orm.em.fork();

    return em.transactional(async (transactionalEntityManager) => {
      const existingRows = await transactionalEntityManager.find(this.options.dialect.sourceTableEntity, {
        groupId: this.options.replicationStreamId,
        connectionId: connection_id,
        schemaName: schema,
        tableName: table
      });

      let sourceTableRow =
        existingRows.find((row) => {
          const matchesRelationId = objectId == null || relationObjectId(row.relationId) == objectId;
          return matchesRelationId && jsonEquals(row.replicaIdColumns, normalizedReplicaIdColumns);
        }) ?? null;

      if (sourceTableRow == null) {
        sourceTableRow = transactionalEntityManager.create(this.options.dialect.sourceTableEntity, {
          id: options.idGenerator ? String(options.idGenerator()) : uuid.v4(),
          groupId: this.options.replicationStreamId,
          connectionId: connection_id,
          relationId,
          schemaName: schema,
          tableName: table,
          replicaIdColumns: normalizedReplicaIdColumns,
          snapshotDone: true,
          snapshotTotalEstimatedCount: null,
          snapshotReplicatedCount: null,
          snapshotLastKey: null
        });
        transactionalEntityManager.persist(sourceTableRow);
        await transactionalEntityManager.flush();
      }

      const sourceTable = sourceTableFromRow(sourceTableRow, connectionTag, syncRules);
      sourceTable.storeCurrentData = sendsCompleteRows !== true;

      const dropTables = existingRows
        .filter((row) => row.id != sourceTableRow.id)
        .filter((row) => objectId == null || relationObjectId(row.relationId) == objectId)
        .map((row) => sourceTableFromRow(row, connectionTag, syncRules));

      return {
        tables: [sourceTable],
        dropTables
      };
    });
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    const row = await this.options.orm.em.fork().findOne(this.options.dialect.sourceTableEntity, {
      groupId: this.options.replicationStreamId,
      id: String(table.id)
    });

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

    const em = this.options.orm.em.fork();
    let nextOpId = await this.getNextOpId(em);
    const firstOpId = nextOpId;

    await em.transactional(async (transactionalEntityManager) => {
      const persistedBatch = this.createPersistedBatch(transactionalEntityManager);
      for (const table of sourceTables) {
        if (!table.syncData && !table.syncParameters) {
          continue;
        }

        const rows = await transactionalEntityManager.find(this.options.dialect.currentDataEntity, {
          groupId: this.options.replicationStreamId,
          sourceTable: String(table.id)
        });

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

          transactionalEntityManager.remove(row);
        }
      }
      await transactionalEntityManager.flush();
    });

    if (nextOpId == firstOpId) {
      return null;
    }

    const lastOpId = nextOpId - 1n;
    this.persistedOp = lastOpId;
    this.last_flushed_op = lastOpId;
    return { flushed_op: lastOpId };
  }

  async drop(sourceTables: storage.SourceTable[]): Promise<storage.FlushedResult | null> {
    const result = await this.truncate(sourceTables);
    const em = this.options.orm.em.fork();
    await em.transactional(async (transactionalEntityManager) => {
      for (const table of sourceTables) {
        await transactionalEntityManager.nativeDelete(this.options.dialect.sourceTableEntity, {
          groupId: this.options.replicationStreamId,
          id: String(table.id)
        });
      }
    });
    return result;
  }

  async flush(_options?: storage.BatchBucketFlushOptions): Promise<storage.FlushedResult | null> {
    let result: storage.FlushedResult | null = null;
    if (this.pendingOperations.length > 0) {
      await this.options.hooks?.beforeBatchFlush?.(this);
      const operations = this.pendingOperations.splice(0);
      const em = this.options.orm.em.fork();
      let nextOpId = await this.getNextOpId(em);
      for (const batch of chunked(operations, MAX_OPERATION_BATCH_COUNT)) {
        await this.options.orm.em.fork().transactional(async (transactionalEntityManager) => {
          const persistedBatch = this.createPersistedBatch(transactionalEntityManager);
          nextOpId = await persistedBatch.persistOperations(batch, nextOpId);
          await transactionalEntityManager.flush();
        });
      }
      const lastOpId = nextOpId - 1n;
      this.persistedOp = lastOpId;
      this.last_flushed_op = lastOpId;
      result = { flushed_op: lastOpId };
      await this.options.hooks?.afterBatchFlush?.(this);
    }

    await this.flushCustomWriteCheckpoints();
    return result;
  }

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    await this.flush();
    const createEmptyCheckpoints = options?.createEmptyCheckpoints ?? true;
    const now = new Date();
    const em = this.options.orm.em.fork();

    const result = await em.transactional(async (transactionalEntityManager) => {
      const syncRulesRow = await transactionalEntityManager.findOneOrFail(this.options.dialect.syncRulesEntity, {
        id: this.options.replicationStreamId
      });

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
          transactionalEntityManager.assign(syncRulesRow, {
            lastCheckpointLsn: lsn,
            lastCheckpointTs: now,
            lastKeepaliveTs: now,
            lastFatalError: null,
            keepaliveOp: null,
            lastCheckpoint: newLastCheckpoint,
            snapshotLsn: null
          });
          checkpointCreated = true;
          if (newLastCheckpoint != null) {
            await transactionalEntityManager.nativeDelete(this.options.dialect.currentDataEntity, {
              groupId: this.options.replicationStreamId,
              pendingDelete: { $lte: newLastCheckpoint }
            });
          }
        } else {
          transactionalEntityManager.assign(syncRulesRow, {
            lastKeepaliveTs: now
          });
        }
      } else {
        transactionalEntityManager.assign(syncRulesRow, {
          keepaliveOp: maxBigint(syncRulesRow.keepaliveOp, this.persistedOp, 0n),
          lastKeepaliveTs: now
        });
      }

      await transactionalEntityManager.flush();
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
    const em = this.options.orm.em.fork();
    const row = await em.findOneOrFail(this.options.dialect.syncRulesEntity, {
      id: this.options.replicationStreamId
    });
    em.assign(row, { snapshotLsn: lsn });
    await em.flush();
    this.resumeFromLsn = lsn;
  }

  async markAllSnapshotDone(noCheckpointBeforeLsn: string): Promise<void> {
    await this.markSnapshotDoneInternal(noCheckpointBeforeLsn);
  }

  async markSnapshotDone(noCheckpointBeforeLsn: string, options?: { throwOnConflict?: boolean }): Promise<void> {
    const remaining = await this.options.orm.em.fork().count(this.options.dialect.sourceTableEntity, {
      groupId: this.options.replicationStreamId,
      snapshotDone: false
    });

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
    const em = this.options.orm.em.fork();
    const row = await em.findOneOrFail(this.options.dialect.syncRulesEntity, {
      id: this.options.replicationStreamId
    });
    em.assign(row, { snapshotDone: false });
    await em.flush();
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    noCheckpointBeforeLsn?: string
  ): Promise<storage.SourceTable[]> {
    const ids = tables.map((table) => String(table.id));
    const em = this.options.orm.em.fork();
    await em.transactional(async (transactionalEntityManager) => {
      const rows = await transactionalEntityManager.find(this.options.dialect.sourceTableEntity, {
        id: { $in: ids }
      });
      for (const row of rows) {
        transactionalEntityManager.assign(row, {
          snapshotDone: true,
          snapshotTotalEstimatedCount: null,
          snapshotReplicatedCount: null,
          snapshotLastKey: null
        });
      }

      if (noCheckpointBeforeLsn != null) {
        await this.assignNoCheckpointBefore(transactionalEntityManager, noCheckpointBeforeLsn);
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

    const em = this.options.orm.em.fork();
    const row = await em.findOneOrFail(this.options.dialect.sourceTableEntity, {
      id: String(table.id)
    });
    em.assign(row, {
      snapshotTotalEstimatedCount: BigInt(snapshotStatus.totalEstimatedCount),
      snapshotReplicatedCount: BigInt(snapshotStatus.replicatedCount),
      snapshotLastKey: snapshotStatus.lastKey
    });
    await em.flush();

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
    const em = this.options.orm.em.fork();
    await em.transactional(async (transactionalEntityManager) => {
      for (const checkpoint of batch) {
        const existing = await transactionalEntityManager.findOne(this.options.dialect.writeCheckpointEntity, {
          userId: checkpoint.user_id,
          syncRulesId: checkpoint.sync_rules_id
        });

        if (existing == null) {
          const row = transactionalEntityManager.create(this.options.dialect.writeCheckpointEntity, {
            id: uuid.v4(),
            syncRulesId: checkpoint.sync_rules_id,
            userId: checkpoint.user_id,
            checkpoint: checkpoint.checkpoint,
            heads: null,
            createdAt: new Date()
          });
          transactionalEntityManager.persist(row);
        } else {
          transactionalEntityManager.assign(existing, {
            checkpoint: checkpoint.checkpoint,
            createdAt: new Date()
          });
        }
      }
      await transactionalEntityManager.flush();
    });
    this.options.factory.checkpointWatcher.notify();
  }

  private createPersistedBatch(transactionalEntityManager: EntityManager): MikroOrmPersistedBatch {
    return new MikroOrmPersistedBatch({
      transactionalEntityManager,
      dialect: this.options.dialect,
      logger: this.options.logger,
      syncRules: this.options.syncRules,
      replicationStreamId: this.options.replicationStreamId,
      storeCurrentData: this.options.storeCurrentData,
      skipExistingRows: this.options.skipExistingRows,
      markRecordUnavailable: this.options.markRecordUnavailable
    });
  }

  private async getNextOpId(em: EntityManager): Promise<bigint> {
    const [bucketData] = await em.find(
      this.options.dialect.bucketDataEntity,
      {},
      { orderBy: { opId: 'DESC' }, limit: 1 }
    );
    const [bucketParameters] = await em.find(
      this.options.dialect.bucketParametersEntity,
      {},
      { orderBy: { id: 'DESC' }, limit: 1 }
    );
    const [pendingDelete] = await em.find(
      this.options.dialect.currentDataEntity,
      { pendingDelete: { $ne: null } },
      { orderBy: { pendingDelete: 'DESC' }, limit: 1 }
    );

    return maxBigint(bucketData?.opId, bucketParameters?.id, pendingDelete?.pendingDelete, 0n) + 1n;
  }

  private async markSnapshotDoneInternal(noCheckpointBeforeLsn: string): Promise<void> {
    const em = this.options.orm.em.fork();
    await em.transactional(async (transactionalEntityManager) => {
      await this.assignNoCheckpointBefore(transactionalEntityManager, noCheckpointBeforeLsn);
    });
    this.options.factory.checkpointWatcher.notify();
  }

  private async assignNoCheckpointBefore(
    transactionalEntityManager: EntityManager,
    noCheckpointBeforeLsn: string
  ): Promise<void> {
    const row = await transactionalEntityManager.findOneOrFail(this.options.dialect.syncRulesEntity, {
      id: this.options.replicationStreamId
    });
    transactionalEntityManager.assign(row, {
      snapshotDone: true,
      lastKeepaliveTs: new Date(),
      noCheckpointBefore:
        row.noCheckpointBefore == null || row.noCheckpointBefore < noCheckpointBeforeLsn
          ? noCheckpointBeforeLsn
          : row.noCheckpointBefore
    });
  }

  private async autoActivate(lsn: string): Promise<void> {
    if (!this.needsActivation) {
      return;
    }

    const em = this.options.orm.em.fork();
    let didActivate = false;
    await em.transactional(async (transactionalEntityManager) => {
      const syncRulesRow = await transactionalEntityManager.findOne(this.options.dialect.syncRulesEntity, {
        id: this.options.replicationStreamId
      });

      if (syncRulesRow?.state == storage.SyncRuleState.PROCESSING && syncRulesRow.snapshotDone) {
        transactionalEntityManager.assign(syncRulesRow, { state: storage.SyncRuleState.ACTIVE });
        const oldRows = await transactionalEntityManager.find(this.options.dialect.syncRulesEntity, {
          id: { $ne: this.options.replicationStreamId },
          state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
        });
        for (const oldRow of oldRows) {
          transactionalEntityManager.assign(oldRow, { state: storage.SyncRuleState.STOP });
        }
        await transactionalEntityManager.flush();
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
  row: SourceTableEntity,
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
    typeId: column.typeId
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
