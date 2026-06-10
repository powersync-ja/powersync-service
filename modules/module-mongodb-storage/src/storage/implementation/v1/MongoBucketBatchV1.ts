import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, InternalOpId, SourceTable, storage } from '@powersync/service-core';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { calculateCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV1 } from './PersistedBatchV1.js';
import { SourceRecordStoreV1 } from './SourceRecordStoreV1.js';
import { VersionedPowerSyncMongoV1 } from './VersionedPowerSyncMongoV1.js';
import { SourceTableDocumentV1, SyncRuleDocumentV1 } from './models.js';

export class MongoBucketBatchV1 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV1;

  private readonly store: SourceRecordStore;
  private needsActivation = true;
  private lastWaitingLogThrottled = 0;

  /**
   * The last persisted op that is not yet covered by a checkpoint, if any.
   *
   * Seeded from the persisted keepalive_op, so that ops persisted before a restart can still be
   * included in the next checkpoint.
   */
  private persisted_op: InternalOpId | null = null;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    this.persisted_op = options.keepaliveOp ?? null;
    this.store = new SourceRecordStoreV1(this.db, this.replicationStreamId);
  }

  protected override recordPersistedOp(lastOp: InternalOpId): void {
    this.persisted_op = lastOp;
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV1(this.db, this.replicationStreamId, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected async cleanupDroppedSourceTables(_tables: SourceTable[]) {
    // No-op for V1: source records live in a shared collection.
  }

  async resolveTables(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult> {
    const syncRules = options.parsedSyncConfig?.hydratedSyncConfig ?? this.sync_rules;
    const { connection_id, source } = options;
    const { schema, name, objectId, replicaIdColumns, connectionTag, sendsCompleteRows } = source;

    const normalizedReplicaIdColumns = replicaIdColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));

    let result: storage.ResolveTablesResult | null = null;
    await this.db.client.withSession(async (session) => {
      const col = this.db.commonSourceTables(this.replicationStreamId);
      const filter: any = {
        group_id: this.replicationStreamId,
        connection_id,
        schema_name: schema,
        table_name: name,
        replica_id_columns2: normalizedReplicaIdColumns
      };
      if (objectId != null) {
        filter.relation_id = objectId;
      }

      let doc = await col.findOne(filter, { session });
      if (doc == null) {
        doc = {
          _id: options.idGenerator ? (options.idGenerator() as bson.ObjectId) : new bson.ObjectId(),
          group_id: this.replicationStreamId,
          connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: name,
          replica_id_columns: null,
          replica_id_columns2: normalizedReplicaIdColumns,
          snapshot_done: false,
          snapshot_status: undefined
        };
        await col.insertOne(doc, { session });
      }

      const sourceTable = new storage.SourceTable({
        id: doc._id,
        ref: source,
        objectId,
        replicaIdColumns,
        snapshotComplete: doc.snapshot_done ?? true,
        ...syncRules.getMatchingSources(source)
      });
      sourceTable.syncEvent = syncRules.tableTriggersEvent(source);
      sourceTable.syncData = sourceTable.bucketDataSources.length > 0;
      sourceTable.syncParameters = sourceTable.parameterLookupSources.length > 0;
      sourceTable.storeCurrentData = sendsCompleteRows !== true;
      sourceTable.snapshotStatus =
        doc.snapshot_status == null
          ? undefined
          : {
              lastKey: doc.snapshot_status.last_key?.buffer ?? null,
              totalEstimatedCount: doc.snapshot_status.total_estimated_count,
              replicatedCount: doc.snapshot_status.replicated_count
            };

      const truncateFilter = [{ schema_name: schema, table_name: name }] as any[];
      if (objectId != null) {
        truncateFilter.push({ relation_id: objectId });
      }
      const truncate = await col
        .find(
          {
            group_id: this.replicationStreamId,
            connection_id,
            _id: { $ne: doc._id },
            $or: truncateFilter
          },
          { session }
        )
        .toArray();
      const dropTables = truncate.map((dropDoc) => {
        const ref = {
          connectionTag,
          schema: dropDoc.schema_name,
          name: dropDoc.table_name
        };
        const table = new storage.SourceTable({
          id: dropDoc._id,
          ref,
          objectId: dropDoc.relation_id,
          replicaIdColumns:
            dropDoc.replica_id_columns2?.map(
              (c) => ({ name: c.name, typeId: c.type_oid, type: c.type }) satisfies ColumnDescriptor
            ) ?? [],
          snapshotComplete: dropDoc.snapshot_done ?? true,
          ...syncRules.getMatchingSources(ref)
        });
        table.syncEvent = syncRules.tableTriggersEvent(ref);
        table.syncData = table.bucketDataSources.length > 0;
        table.syncParameters = table.parameterLookupSources.length > 0;
        return table;
      });

      result = { tables: [sourceTable], dropTables };
    });

    return result!;
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    const doc = (await this.db.commonSourceTables(this.replicationStreamId).findOne(
      {
        group_id: this.replicationStreamId,
        _id: mongoTableId(table.id)
      },
      { session: this.session }
    )) as SourceTableDocumentV1 | null;
    if (doc == null) {
      return null;
    }

    const ref = {
      connectionTag: table.ref.connectionTag,
      schema: doc.schema_name,
      name: doc.table_name
    };
    const sourceTable = new storage.SourceTable({
      id: doc._id,
      ref,
      objectId: doc.relation_id,
      replicaIdColumns:
        doc.replica_id_columns2?.map(
          (c) => ({ name: c.name, typeId: c.type_oid, type: c.type }) satisfies ColumnDescriptor
        ) ?? [],
      snapshotComplete: doc.snapshot_done ?? true,
      ...this.sync_rules.getMatchingSources(ref)
    });
    sourceTable.syncEvent = this.sync_rules.tableTriggersEvent(ref);
    sourceTable.syncData = sourceTable.bucketDataSources.length > 0;
    sourceTable.syncParameters = sourceTable.parameterLookupSources.length > 0;
    sourceTable.snapshotStatus =
      doc.snapshot_status == null
        ? undefined
        : {
            lastKey: doc.snapshot_status.last_key?.buffer ?? null,
            totalEstimatedCount: doc.snapshot_status.total_estimated_count,
            replicatedCount: doc.snapshot_status.replicated_count
          };
    return sourceTable;
  }

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    const { createEmptyCheckpoints } = { ...storage.DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS, ...options };

    await this.flush(options);

    const now = new Date();

    await this.db.write_checkpoints.updateMany(
      {
        processed_at_lsn: null,
        'lsns.1': { $lte: lsn }
      },
      {
        $set: {
          processed_at_lsn: lsn
        }
      },
      {
        session: this.session
      }
    );

    const can_checkpoint = {
      $and: [
        { $eq: ['$snapshot_done', true] },
        {
          $or: [{ $eq: ['$last_checkpoint_lsn', null] }, { $lte: ['$last_checkpoint_lsn', { $literal: lsn }] }]
        },
        {
          $or: [{ $eq: ['$no_checkpoint_before', null] }, { $lte: ['$no_checkpoint_before', { $literal: lsn }] }]
        }
      ]
    };

    const new_keepalive_op = {
      $cond: [
        can_checkpoint,
        { $literal: null },
        {
          $toString: {
            $max: [{ $toLong: '$keepalive_op' }, { $literal: this.persisted_op }, 0n]
          }
        }
      ]
    };

    const new_last_checkpoint = {
      $cond: [
        can_checkpoint,
        {
          $max: ['$last_checkpoint', { $literal: this.persisted_op }, { $toLong: '$keepalive_op' }, 0n]
        },
        '$last_checkpoint'
      ]
    };

    const preUpdateDocument = (await this.db.sync_rules.findOneAndUpdate(
      { _id: this.replicationStreamId },
      [
        {
          $set: {
            _can_checkpoint: can_checkpoint,
            _not_empty: createEmptyCheckpoints
              ? true
              : {
                  $or: [
                    { $literal: createEmptyCheckpoints },
                    { $ne: ['$keepalive_op', new_keepalive_op] },
                    { $ne: ['$last_checkpoint', new_last_checkpoint] }
                  ]
                }
          }
        },
        {
          $set: {
            last_checkpoint_lsn: {
              $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: lsn }, '$last_checkpoint_lsn']
            },
            last_checkpoint_ts: {
              $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: now }, '$last_checkpoint_ts']
            },
            last_keepalive_ts: { $literal: now },
            last_fatal_error: { $literal: null },
            last_fatal_error_ts: { $literal: null },
            keepalive_op: new_keepalive_op,
            last_checkpoint: new_last_checkpoint,
            snapshot_lsn: {
              $cond: [{ $and: ['$_can_checkpoint', '$_not_empty'] }, { $literal: null }, '$snapshot_lsn']
            }
          }
        },
        {
          $unset: ['_can_checkpoint', '_not_empty']
        }
      ],
      {
        session: this.session,
        returnDocument: 'before',
        projection: {
          snapshot_done: 1,
          last_checkpoint_lsn: 1,
          no_checkpoint_before: 1,
          keepalive_op: 1,
          last_checkpoint: 1
        }
      }
    )) as SyncRuleDocumentV1;

    if (preUpdateDocument == null) {
      throw new ReplicationAssertionError(
        'Failed to update checkpoint - no matching sync_rules document for _id: ' + this.replicationStreamId
      );
    }

    const checkpointState = calculateCheckpointState({
      lsn,
      snapshotDone: preUpdateDocument.snapshot_done === true,
      lastCheckpointLsn: preUpdateDocument.last_checkpoint_lsn,
      noCheckpointBefore: preUpdateDocument.no_checkpoint_before,
      keepaliveOp: preUpdateDocument.keepalive_op == null ? null : BigInt(preUpdateDocument.keepalive_op),
      lastCheckpoint: preUpdateDocument.last_checkpoint,
      persistedOp: this.persisted_op,
      createEmptyCheckpoints
    });
    if (checkpointState.checkpointBlocked) {
      if (Date.now() - this.lastWaitingLogThrottled > 5_000) {
        this.logger.info(
          `Waiting before creating checkpoint, currently at ${lsn} / ${checkpointState.newKeepaliveOp}. Current state: ${JSON.stringify(
            {
              snapshot_done: preUpdateDocument.snapshot_done,
              last_checkpoint_lsn: preUpdateDocument.last_checkpoint_lsn,
              no_checkpoint_before: preUpdateDocument.no_checkpoint_before
            }
          )}`
        );
        this.lastWaitingLogThrottled = Date.now();
      }
    } else {
      if (checkpointState.checkpointCreated) {
        this.logger.debug(`Created checkpoint at ${lsn} / ${checkpointState.newLastCheckpoint}`);
      }
      await this.autoActivate(lsn);
      await this.db.notifyCheckpoint();
      this.persisted_op = null;
      if (checkpointState.newLastCheckpoint != null) {
        await this.sourceRecordStore.postCommitCleanup(checkpointState.newLastCheckpoint, this.logger);
      }
    }

    return {
      checkpointBlocked: checkpointState.checkpointBlocked,
      checkpointCreated: checkpointState.checkpointCreated
    };
  }

  async keepalive(lsn: string): Promise<storage.CheckpointResult> {
    return await this.commit(lsn, { createEmptyCheckpoints: true });
  }

  async setResumeLsn(lsn: string): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          snapshot_lsn: lsn
        }
      },
      { session: this.session }
    );
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          snapshot_done: true,
          last_keepalive_ts: new Date()
        },
        $max: {
          no_checkpoint_before: no_checkpoint_before_lsn
        }
      },
      { session: this.session }
    );
  }

  async markSnapshotDone(no_checkpoint_before_lsn: string, options?: { throwOnConflict?: boolean }): Promise<void> {
    await this.withTransaction(async () => {
      // Protect against race conditions
      const count = await this.db.commonSourceTables(this.replicationStreamId).countDocuments(
        {
          group_id: this.replicationStreamId,
          snapshot_done: false
        },
        { session: this.session }
      );
      if (count > 0) {
        if (options?.throwOnConflict ?? true) {
          throw new ReplicationAssertionError(
            `Cannot mark snapshot done while ${count} source table${count == 1 ? '' : 's'} still require snapshotting`
          );
        } else {
          return;
        }
      }

      await this.markAllSnapshotDone(no_checkpoint_before_lsn);
    });
  }

  async markTableSnapshotRequired(_table: storage.SourceTable): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          snapshot_done: false
        }
      },
      { session: this.session }
    );
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    const session = this.session;
    const ids = tables.map((table) => mongoTableId(table.id));

    await this.withTransaction(async () => {
      await this.db.commonSourceTables(this.replicationStreamId).updateMany(
        { _id: { $in: ids } },
        {
          $set: {
            snapshot_done: true
          },
          $unset: {
            snapshot_status: 1
          }
        },
        { session }
      );

      if (no_checkpoint_before_lsn != null) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId
          },
          {
            $set: {
              last_keepalive_ts: new Date()
            },
            $max: {
              no_checkpoint_before: no_checkpoint_before_lsn
            }
          },
          { session: this.session }
        );
      }
    });
    return tables.map((table) => {
      const copy = table.clone();
      copy.snapshotComplete = true;
      return copy;
    });
  }

  private async autoActivate(lsn: string): Promise<void> {
    if (!this.needsActivation) {
      return;
    }

    const session = this.session;
    let activated = false;
    await session.withTransaction(async () => {
      const doc = (await this.db.sync_rules.findOne(
        { _id: this.replicationStreamId },
        { session }
      )) as SyncRuleDocumentV1;
      if (doc && doc.state == storage.SyncRuleState.PROCESSING && doc.snapshot_done && doc.last_checkpoint != null) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId
          },
          {
            $set: {
              state: storage.SyncRuleState.ACTIVE
            }
          },
          { session }
        );

        await this.db.sync_rules.updateMany(
          {
            _id: { $ne: this.replicationStreamId },
            state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
          },
          {
            $set: {
              state: storage.SyncRuleState.STOP
            }
          },
          { session }
        );
        activated = true;
      } else if (doc?.state != storage.SyncRuleState.PROCESSING) {
        this.needsActivation = false;
      }
    });
    if (activated) {
      this.logger.info(`Activated new replication stream at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivation = false;
    }
  }
}
