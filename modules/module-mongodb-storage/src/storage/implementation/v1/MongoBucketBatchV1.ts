import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, InternalOpId, SourceTable, storage } from '@powersync/service-core';
import { HydratedSyncConfig } from '@powersync/service-sync-rules';
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
  declare readonly db: VersionedPowerSyncMongoV1;

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
    const reconcile = options.reconcileSourceTables ?? storage.defaultSourceTableReconciler;
    const { connection_id, source } = options;
    const { schema, name, objectId, replicaIdColumns, connectionTag, sendsCompleteRows } = source;

    const normalizedReplicaIdColumns = replicaIdColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));

    let result: storage.ResolveTablesResult | null = null;
    await this.db.client.withSession(async (session) => {
      const col = this.db.sourceTablesV1(this.replicationStreamId);

      // Single overlap query: any candidate matching (schema + name) OR relation_id. Replaces the
      // previous exact-match lookup plus separate conflict lookup.
      const orClauses: Record<string, unknown>[] = [{ schema_name: schema, table_name: name }];
      if (objectId != null) {
        orClauses.push({ relation_id: objectId });
      }
      const candidateDocs = await col
        .find({ group_id: this.replicationStreamId, connection_id, $or: orClauses }, { session })
        .toArray();

      // Hydrate candidates and let the source reconciler classify compatibility in JS.
      const candidateTables = candidateDocs.map((doc) =>
        this.hydrateSourceTable(doc, connectionTag, syncRules, sendsCompleteRows)
      );
      const resolution = await reconcile({ source, candidates: candidateTables });
      const sourceCompatibleCandidateIds = Array.from(resolution.sourceCompatibleCandidateIds.values());
      const compatibleTables = candidateTables.filter(
        (table) => sourceCompatibleCandidateIds.find((id) => storage.sourceTableIdEquals(id, table.id)) != null
      );

      // V1 resolves to a single record. When multiple candidates are compatible (unexpected -
      // duplicate or corrupt state), deterministically prefer a snapshot-complete record so we
      // preserve already-snapshotted data, and treat the rest as conflicts to drop.
      const reuse = compatibleTables.find((table) => table.snapshotComplete) ?? compatibleTables[0] ?? null;

      let sourceTable: storage.SourceTable;
      if (reuse != null) {
        sourceTable = reuse;
      } else {
        const doc: SourceTableDocumentV1 = {
          _id: options.idGenerator ? (options.idGenerator() as bson.ObjectId) : new bson.ObjectId(),
          group_id: this.replicationStreamId,
          connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: name,
          replica_id_columns: null,
          replica_id_columns2: normalizedReplicaIdColumns,
          snapshot_done: false,
          snapshot_status: undefined,
          source_metadata: resolution.sourceMetadata
        };
        await col.insertOne(doc, { session });
        sourceTable = this.hydrateSourceTable(doc, connectionTag, syncRules, sendsCompleteRows);
      }

      // All remaining overlapping candidates are drops (renames, relation-id / replica-id changes,
      // superseded source-metadata generations, or duplicate compatible records).
      const dropTables = candidateTables.filter((table) => !storage.sourceTableIdEquals(table.id, sourceTable.id));

      result = { tables: [sourceTable], dropTables };
    });

    return result!;
  }

  /**
   * Hydrate a persisted v1 source-table doc into a `SourceTable`, including sync flags and
   * snapshot state. Used both as reconciler input and to build resolved/dropped tables.
   */
  private hydrateSourceTable(
    doc: SourceTableDocumentV1,
    connectionTag: string,
    syncRules: HydratedSyncConfig,
    sendsCompleteRows: boolean | undefined
  ): storage.SourceTable {
    const ref = { connectionTag, schema: doc.schema_name, name: doc.table_name };
    const table = new storage.SourceTable({
      id: doc._id,
      ref,
      objectId: doc.relation_id,
      replicaIdColumns:
        doc.replica_id_columns2?.map(
          (c) => ({ name: c.name, typeId: c.type_oid, type: c.type }) satisfies ColumnDescriptor
        ) ?? [],
      snapshotComplete: doc.snapshot_done ?? true,
      sourceMetadata: doc.source_metadata,
      ...syncRules.getMatchingSources(ref)
    });
    table.syncEvent = syncRules.tableTriggersEvent(ref);
    table.syncData = table.bucketDataSources.length > 0;
    table.syncParameters = table.parameterLookupSources.length > 0;
    table.storeCurrentData = sendsCompleteRows !== true;
    table.snapshotStatus =
      doc.snapshot_status == null
        ? undefined
        : {
            lastKey: doc.snapshot_status.last_key?.buffer ?? null,
            totalEstimatedCount: doc.snapshot_status.total_estimated_count,
            replicatedCount: doc.snapshot_status.replicated_count
          };
    return table;
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    const doc = await this.db.sourceTablesV1(this.replicationStreamId).findOne(
      {
        group_id: this.replicationStreamId,
        _id: mongoTableId(table.id)
      },
      { session: this.session }
    );
    if (doc == null) {
      return null;
    }

    // storeCurrentData is resolved externally per-batch; getSourceTableStatus preserves the
    // conservative default (true) by passing sendsCompleteRows undefined.
    return this.hydrateSourceTable(doc, table.ref.connectionTag, this.sync_rules, undefined);
  }

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    const { createEmptyCheckpoints } = { ...storage.DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS, ...options };

    await this.flush(options);

    using _ = this.tracer.span('storage', 'commit');

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
    using _ = this.tracer.span('storage', 'set_resume_lsn');
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          snapshot_lsn: lsn
        }
      },
      {
        session: this.session,
        // Losing occasional resume LSN is fine. That may mean reprocessing
        // some source changes in some edge cases, which is not an issue since
        // changes are processed in an idempotent way.
        writeConcern: { w: 1 }
      }
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
      const blockingTables = await this.db
        .sourceTablesV1(this.replicationStreamId)
        .find(
          {
            group_id: this.replicationStreamId,
            snapshot_done: false
          },
          {
            session: this.session,
            projection: { schema_name: 1, table_name: 1 }
          }
        )
        .toArray();

      if (blockingTables.length > 0) {
        if (options?.throwOnConflict ?? true) {
          throw new ReplicationAssertionError(
            `Cannot mark snapshot done while source tables still require snapshotting. Tables: ${blockingTables.map((t) => `${t.schema_name}.${t.table_name}`).join(', ')}`
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
      await this.db.sourceTablesV1(this.replicationStreamId).updateMany(
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
    let needsFutureActivationCheck = true;
    await session.withTransaction(async () => {
      // Reset on transaction retries.
      activated = false;
      needsFutureActivationCheck = true;

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
        needsFutureActivationCheck = false;
      }
    });
    if (activated) {
      this.logger.info(`Activated new replication stream at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivation = false;
    } else if (!needsFutureActivationCheck) {
      this.needsActivation = false;
    }
  }
}
