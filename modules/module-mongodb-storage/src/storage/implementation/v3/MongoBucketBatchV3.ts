import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, storage } from '@powersync/service-core';
import { HydratedSyncConfig, MatchingSources } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { canCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { syncRuleStateUpdatePipeline } from '../SyncRuleStateUpdate.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { ReplicationStreamDocumentV3, SourceTableDocumentV3 } from './models.js';

function sameStringArray(left: string[], right: string[]) {
  return left.length == right.length && left.every((value, index) => value == right[index]);
}

export class MongoBucketBatchV3 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV3;

  private readonly store: SourceRecordStore;
  private readonly syncConfigIds: bson.ObjectId[];
  private needsActivationV3 = true;
  private lastWaitingLogThrottledV3 = 0;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    const syncConfigIds = options.syncConfigIds ?? [];
    if (syncConfigIds.length == 0) {
      throw new ReplicationAssertionError('Missing sync config id for v3 batch');
    }
    this.syncConfigIds = syncConfigIds;
    this.store = new SourceRecordStoreV3(this.db, this.replicationStreamId, this.mapping);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.replicationStreamId, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected override async onReplicationTransactionFlush(
    session: lib_mongo.mongo.ClientSession,
    lastOp: bigint
  ): Promise<void> {
    // Durably advance the stream-level head of persisted ops within the flush transaction.
    // This ensures a checkpoint created later (even by an empty commit, or by a freshly-appended
    // config that replicates nothing) covers all ops persisted before a potential crash.
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $max: {
          last_persisted_op: lastOp
        }
      },
      { session }
    );
  }

  private selectedSyncConfigObjectIds(syncConfigIds: string[]): bson.ObjectId[] {
    const selectedIds = new Set(this.syncConfigIds.map((id) => id.toHexString()));
    return syncConfigIds
      .filter((id) => selectedIds.has(id) && bson.ObjectId.isValid(id))
      .map((id) => new bson.ObjectId(id));
  }

  private relevantSyncConfigIds(table: storage.SourceTable): bson.ObjectId[] {
    const bucketDataSourceIds = table.bucketDataSources.map((source) => this.mapping.bucketSourceId(source));
    const parameterLookupSourceIds = table.parameterLookupSources.map((source) =>
      this.mapping.parameterLookupId(source)
    );
    return this.selectedSyncConfigObjectIds(
      this.mapping.syncConfigIdsForSourceTable(
        this.syncConfigIds.map((id) => id.toHexString()),
        table.ref,
        bucketDataSourceIds,
        parameterLookupSourceIds
      )
    );
  }

  private relevantSyncConfigIdsForTables(tables: storage.SourceTable[]): bson.ObjectId[] {
    const ids = new Map<string, bson.ObjectId>();
    for (const table of tables) {
      for (const id of this.relevantSyncConfigIds(table)) {
        ids.set(id.toHexString(), id);
      }
    }
    return [...ids.values()];
  }

  private snapshotBlockingSourceTablesFilter(): Record<string, unknown> {
    const clauses = this.syncConfigIds.flatMap((syncConfigId) => {
      const filter = this.mapping.snapshotBlockingSourceTablesFilter(syncConfigId.toHexString()) as {
        $or?: Record<string, unknown>[];
      };
      return filter.$or ?? [];
    });

    if (clauses.length == 0) {
      return {
        snapshot_done: false,
        _id: { $exists: false }
      };
    }

    return {
      snapshot_done: false,
      $or: clauses
    };
  }

  protected async cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]) {
    for (const table of sourceTables) {
      await this.db
        .sourceRecordsV3(this.replicationStreamId, mongoTableId(table.id))
        .drop()
        .catch((error) => {
          if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
            return;
          }
          throw error;
        });
    }
  }

  async resolveTables(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult> {
    const ref = options.source;
    const syncRules = options.syncRules ?? this.sync_rules;
    const matchingSources = syncRules.getMatchingSources(ref);

    const { connection_id, source } = options;
    const { schema, name, objectId, replicaIdColumns, connectionTag, sendsCompleteRows } = source;
    const normalizedReplicaIdColumns = replicaIdColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));

    let result: storage.ResolveTablesResult | null = null;
    const initializeSourceRecordsFor: bson.ObjectId[] = [];

    await this.db.client.withSession(async (session) => {
      const col = this.db.commonSourceTables(this.replicationStreamId);
      const exactFilter: Record<string, unknown> = {
        connection_id,
        schema_name: schema,
        table_name: name,
        replica_id_columns2: normalizedReplicaIdColumns
      };
      if (objectId != null) {
        exactFilter.relation_id = objectId;
      }

      const exactDocs = (await col.find(exactFilter, { session }).toArray()) as SourceTableDocumentV3[];
      const bucketSourceById = new Map(
        matchingSources.bucketDataSources.map((source) => [this.mapping.bucketSourceId(source), source] as const)
      );
      const parameterLookupSourceById = new Map(
        matchingSources.parameterLookupSources.map(
          (source) => [this.mapping.parameterLookupId(source), source] as const
        )
      );
      const desiredBucketIds = new Set(bucketSourceById.keys());
      const desiredLookupIds = new Set(parameterLookupSourceById.keys());
      const desiredHasMembership = desiredBucketIds.size > 0 || desiredLookupIds.size > 0;
      const triggersEvent = syncRules.tableTriggersEvent(ref);

      const coveredBucketIds = new Set<string>();
      const coveredLookupIds = new Set<string>();
      const retainedDocIds: bson.ObjectId[] = [];
      const tables: storage.SourceTable[] = [];
      let retainedEventOnlyTable = false;

      for (const doc of exactDocs) {
        const bucketDataSourceIds = doc.bucket_data_source_ids.filter((id) => desiredBucketIds.has(id));
        const parameterLookupSourceIds = doc.parameter_lookup_source_ids.filter((id) => desiredLookupIds.has(id));
        const coversDesiredMembership = bucketDataSourceIds.length > 0 || parameterLookupSourceIds.length > 0;
        const coversEventOnlyTable = !desiredHasMembership && triggersEvent && !retainedEventOnlyTable;

        for (const id of bucketDataSourceIds) {
          coveredBucketIds.add(id);
        }
        for (const id of parameterLookupSourceIds) {
          coveredLookupIds.add(id);
        }

        const updates: Partial<SourceTableDocumentV3> = {};
        if (
          !sameStringArray(doc.bucket_data_source_ids, bucketDataSourceIds) ||
          !sameStringArray(doc.parameter_lookup_source_ids, parameterLookupSourceIds)
        ) {
          updates.bucket_data_source_ids = bucketDataSourceIds;
          updates.parameter_lookup_source_ids = parameterLookupSourceIds;
        }
        if (Object.keys(updates).length > 0) {
          await col.updateOne({ _id: doc._id }, { $set: updates }, { session });
        }

        if (coversDesiredMembership || coversEventOnlyTable) {
          if (coversEventOnlyTable) {
            retainedEventOnlyTable = true;
          }
          retainedDocIds.push(doc._id);
          const table = this.sourceTableFromDocument(
            {
              ...doc,
              bucket_data_source_ids: bucketDataSourceIds,
              parameter_lookup_source_ids: parameterLookupSourceIds
            },
            connectionTag,
            syncRules,
            {
              bucketDataSources: bucketDataSourceIds.map((id) => bucketSourceById.get(id)!),
              parameterLookupSources: parameterLookupSourceIds.map((id) => parameterLookupSourceById.get(id)!)
            }
          );
          table.storeCurrentData = sendsCompleteRows !== true;
          tables.push(table);
        }
      }

      const uncoveredBucketIds = [...desiredBucketIds].filter((id) => !coveredBucketIds.has(id));
      const uncoveredLookupIds = [...desiredLookupIds].filter((id) => !coveredLookupIds.has(id));

      if (uncoveredBucketIds.length > 0 || uncoveredLookupIds.length > 0 || (triggersEvent && tables.length == 0)) {
        const id = options.idGenerator ? (options.idGenerator() as bson.ObjectId) : new bson.ObjectId();
        const sourceTable = new storage.SourceTable({
          id,
          ref,
          objectId,
          replicaIdColumns,
          snapshotComplete: false,
          bucketDataSources: uncoveredBucketIds.map((id) => bucketSourceById.get(id)!),
          parameterLookupSources: uncoveredLookupIds.map((id) => parameterLookupSourceById.get(id)!)
        });
        sourceTable.syncData = uncoveredBucketIds.length > 0;
        sourceTable.syncParameters = uncoveredLookupIds.length > 0;
        sourceTable.syncEvent = triggersEvent;
        sourceTable.storeCurrentData = sendsCompleteRows !== true;

        const createDoc: SourceTableDocumentV3 = {
          _id: id,
          connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: name,
          replica_id_columns: null,
          replica_id_columns2: normalizedReplicaIdColumns,
          snapshot_done: false,
          snapshot_status: undefined,
          bucket_data_source_ids: uncoveredBucketIds,
          parameter_lookup_source_ids: uncoveredLookupIds
        };

        await col.insertOne(createDoc, { session });
        initializeSourceRecordsFor.push(createDoc._id);
        retainedDocIds.push(createDoc._id);
        tables.push(sourceTable);
      }

      const conflictFilter = [{ schema_name: schema, table_name: name }] as Record<string, unknown>[];
      if (objectId != null) {
        conflictFilter.push({ relation_id: objectId });
      }
      const dropTables = await col
        .find(
          {
            connection_id,
            _id: { $nin: retainedDocIds },
            $or: conflictFilter
          },
          { session }
        )
        .toArray();

      result = {
        tables,
        dropTables: dropTables.map((doc) =>
          this.sourceTableFromDocument(doc as SourceTableDocumentV3, connectionTag, syncRules)
        )
      };
    });

    for (const sourceTableId of initializeSourceRecordsFor) {
      await this.db.initializeSourceRecordsCollection(this.replicationStreamId, sourceTableId);
    }

    return result!;
  }

  private sourceTableFromDocument(
    doc: SourceTableDocumentV3,
    connectionTag: string,
    syncRules: HydratedSyncConfig,
    memberships?: MatchingSources
  ): storage.SourceTable {
    const resolvedMemberships = memberships ?? this.sourceTableMembershipsFromDocument(doc, syncRules);
    const table = new storage.SourceTable({
      id: doc._id,
      ref: {
        connectionTag,
        schema: doc.schema_name,
        name: doc.table_name
      },
      objectId: doc.relation_id,
      replicaIdColumns: doc.replica_id_columns2!.map(
        (c) => ({ name: c.name, typeId: c.type_oid, type: c.type }) satisfies ColumnDescriptor
      ),
      snapshotComplete: doc.snapshot_done ?? true,
      bucketDataSources: resolvedMemberships.bucketDataSources,
      parameterLookupSources: resolvedMemberships.parameterLookupSources
    });
    table.syncData = table.bucketDataSources.length > 0;
    table.syncParameters = table.parameterLookupSources.length > 0;
    table.syncEvent = syncRules.tableTriggersEvent(table.ref);
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

  private sourceTableMembershipsFromDocument(
    doc: SourceTableDocumentV3,
    syncRules: HydratedSyncConfig
  ): MatchingSources {
    const bucketDataSourceIds = new Set(doc.bucket_data_source_ids);
    const parameterLookupSourceIds = new Set(doc.parameter_lookup_source_ids);

    return {
      bucketDataSources: syncRules.bucketDataSources.filter((source) =>
        bucketDataSourceIds.has(this.mapping.bucketSourceId(source))
      ),
      parameterLookupSources: syncRules.bucketParameterLookupSources.filter((source) =>
        parameterLookupSourceIds.has(this.mapping.parameterLookupId(source))
      )
    };
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    const doc = (await this.db
      .commonSourceTables(this.replicationStreamId)
      .findOne({ _id: mongoTableId(table.id) }, { session: this.session })) as SourceTableDocumentV3 | null;
    if (doc == null) {
      return null;
    }

    return this.sourceTableFromDocument(doc, table.ref.connectionTag, this.sync_rules);
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

    const preUpdateDocument = (await this.db.sync_rules.findOne(
      {
        _id: this.replicationStreamId,
        'sync_configs._id': { $in: this.syncConfigIds }
      },
      {
        session: this.session,
        projection: {
          snapshot_lsn: 1,
          sync_configs: 1,
          last_persisted_op: 1
        }
      }
    )) as ReplicationStreamDocumentV3 | null;

    const states =
      preUpdateDocument?.sync_configs?.filter((config) => this.syncConfigIds.some((id) => id.equals(config._id))) ?? [];
    if (states.length == 0) {
      throw new ReplicationAssertionError(
        `Failed to update checkpoint - no matching sync_config for _id: ${this.replicationStreamId}/${this.syncConfigIds
          .map((id) => id.toHexString())
          .join(',')}`
      );
    }
    // The replication job / MongoBucketBatch must be constructed with all replicating (PROCESSING / ACTIVE)
    // sync configs in the stream, otherwise we'll get inconsistencies. Configs in other states (e.g. STOP)
    // remain embedded in the document and are ignored here.
    const missingSyncConfig = preUpdateDocument!.sync_configs.find(
      (config) =>
        [storage.SyncRuleState.PROCESSING, storage.SyncRuleState.ACTIVE].includes(config.state) &&
        !this.syncConfigIds.some((id) => id.equals(config._id))
    );
    if (missingSyncConfig != null) {
      throw new ReplicationAssertionError(`Replication job not configured for sync config ${missingSyncConfig._id}`);
    }

    // Effective head of the stream's op sequence.
    // last_persisted_op is $max-advanced durably in the same transaction as every flush, and this
    // read uses the same session, so it covers all ops persisted by this batch.
    const newCheckpoint =
      preUpdateDocument?.last_persisted_op == null ? 0n : BigInt(preUpdateDocument.last_persisted_op);

    let checkpointBlocked = false;
    let checkpointCreated = false;
    let checkpointLogState: unknown = null;
    const unblockedConfigIds: bson.ObjectId[] = [];

    for (const state of states) {
      if (state.last_checkpoint != null && state.last_checkpoint > newCheckpoint) {
        // last_persisted_op is $max-advanced durably in the same transaction as every flush, and
        // checkpoints are only ever created at that head, so a checkpoint past the head means the
        // op sequence or the stored state is corrupt.
        throw new ReplicationAssertionError(
          `Invariant violation: sync config ${state._id} has last_checkpoint ${state.last_checkpoint} > stream head ${newCheckpoint}`
        );
      }

      const canCheckpoint = canCheckpointState(lsn, {
        snapshotDone: state.snapshot_done === true,
        lastCheckpointLsn: state.last_checkpoint_lsn,
        noCheckpointBefore: state.no_checkpoint_before
      });

      if (!canCheckpoint) {
        checkpointBlocked = true;
        // Log the first blocked config's state.
        checkpointLogState ??= {
          snapshot_done: state.snapshot_done,
          last_checkpoint_lsn: state.last_checkpoint_lsn,
          no_checkpoint_before: state.no_checkpoint_before
        };
        continue;
      }

      checkpointCreated ||= createEmptyCheckpoints || state.last_checkpoint !== newCheckpoint;
      unblockedConfigIds.push(state._id);
    }

    if (unblockedConfigIds.length > 0) {
      // All unblocked configs get the SAME new value, so we apply it with a single updateOne
      // (single-document atomicity).
      const updateSet: Record<string, any> = {
        last_keepalive_ts: now,
        last_fatal_error: null,
        last_fatal_error_ts: null
      };
      // Only advance checkpoint fields when an actual (non-empty) checkpoint is created, matching
      // the previous per-config / v1 behaviour.
      if (checkpointCreated) {
        updateSet['sync_configs.$[config].last_checkpoint'] = newCheckpoint;
        updateSet['sync_configs.$[config].last_checkpoint_lsn'] = lsn;
        updateSet['snapshot_lsn'] = null;
        updateSet['last_checkpoint_ts'] = now;
      }

      await this.db.sync_rules.updateOne(
        {
          _id: this.replicationStreamId,
          'sync_configs._id': { $in: unblockedConfigIds }
        },
        { $set: updateSet },
        {
          session: this.session,
          arrayFilters: checkpointCreated ? [{ 'config._id': { $in: unblockedConfigIds } }] : undefined
        }
      );
    } else {
      // All selected configs are blocked - only update keepalive/error tracking.
      await this.db.sync_rules.updateOne(
        {
          _id: this.replicationStreamId
        },
        {
          $set: {
            last_keepalive_ts: now,
            last_fatal_error: null,
            last_fatal_error_ts: null
          }
        },
        { session: this.session }
      );
    }

    if (checkpointBlocked) {
      if (Date.now() - this.lastWaitingLogThrottledV3 > 5_000) {
        this.logger.info(
          `Waiting before creating checkpoint, currently at ${lsn}. Current state: ${JSON.stringify(checkpointLogState)}`
        );
        this.lastWaitingLogThrottledV3 = Date.now();
      }
    } else {
      if (checkpointCreated) {
        this.logger.debug(`Created checkpoint at ${lsn}`);
      }
      await this.autoActivateV3(lsn);
      await this.db.notifyCheckpoint();
      this.last_checkpoint_lsn = lsn;
      // All configs are now checkpointed at newLastCheckpoint (the stream head).
      await this.sourceRecordStore.postCommitCleanup(newCheckpoint, this.logger);
    }
    return {
      checkpointBlocked,
      checkpointCreated
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

  private async autoActivateV3(lsn: string): Promise<void> {
    if (!this.needsActivationV3) {
      return;
    }

    const session = this.session;
    let activated = false;
    await session.withTransaction(async () => {
      const doc = await this.db.sync_rules.findOne(
        {
          _id: this.replicationStreamId,
          'sync_configs._id': { $in: this.syncConfigIds }
        },
        {
          session,
          projection: {
            state: 1,
            sync_configs: 1
          }
        }
      );
      const states =
        (doc as ReplicationStreamDocumentV3)?.sync_configs?.filter((config) =>
          this.syncConfigIds.some((id) => id.equals(config._id))
        ) ?? [];
      if (doc == null || states.length == 0) {
        return;
      }

      const processingStates = states.filter((state) => state.state == storage.SyncRuleState.PROCESSING);
      if (
        doc.state == storage.SyncRuleState.PROCESSING &&
        processingStates.length == states.length &&
        states.every((state) => state.snapshot_done && state.last_checkpoint != null)
      ) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: this.syncConfigIds }
          },
          {
            $set: {
              state: storage.SyncRuleState.ACTIVE,
              'sync_configs.$[config].state': storage.SyncRuleState.ACTIVE
            }
          },
          {
            session,
            arrayFilters: [{ 'config._id': { $in: this.syncConfigIds } }]
          }
        );

        await this.db.sync_rules.updateMany(
          {
            _id: { $ne: this.replicationStreamId },
            state: { $in: [storage.SyncRuleState.ACTIVE, storage.SyncRuleState.ERRORED] }
          },
          syncRuleStateUpdatePipeline(storage.SyncRuleState.STOP),
          { session }
        );
        activated = true;
      } else if (
        doc.state == storage.SyncRuleState.ACTIVE &&
        processingStates.length > 0 &&
        processingStates.every((state) => state.snapshot_done && state.last_checkpoint != null)
      ) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: processingStates.map((state) => state._id) }
          },
          {
            $set: {
              'sync_configs.$[activeConfig].state': storage.SyncRuleState.STOP,
              'sync_configs.$[processingConfig].state': storage.SyncRuleState.ACTIVE
            }
          },
          {
            session,
            arrayFilters: [
              { 'activeConfig.state': storage.SyncRuleState.ACTIVE },
              { 'processingConfig._id': { $in: processingStates.map((state) => state._id) } }
            ]
          }
        );
        activated = true;
      } else if (doc.state != storage.SyncRuleState.PROCESSING && doc.state != storage.SyncRuleState.ACTIVE) {
        this.needsActivationV3 = false;
      }
    });
    if (activated) {
      this.logger.info(`Activated new replication stream at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivationV3 = false;
    }
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId,
        'sync_configs._id': { $in: this.syncConfigIds }
      },
      {
        $set: {
          'sync_configs.$[config].snapshot_done': true,
          last_keepalive_ts: new Date()
        },
        $max: {
          'sync_configs.$[config].no_checkpoint_before': no_checkpoint_before_lsn
        }
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': { $in: this.syncConfigIds } }]
      }
    );
  }

  async markSnapshotDone(no_checkpoint_before_lsn: string, options?: { throwOnConflict?: boolean }): Promise<void> {
    await this.withTransaction(async () => {
      // Protect against race conditions
      const count = await this.db
        .sourceTablesV3(this.replicationStreamId)
        .countDocuments(this.snapshotBlockingSourceTablesFilter(), { session: this.session });
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

  async markTableSnapshotRequired(table: storage.SourceTable): Promise<void> {
    const syncConfigIds = this.relevantSyncConfigIds(table);
    if (syncConfigIds.length == 0) {
      return;
    }

    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId,
        'sync_configs._id': { $in: syncConfigIds }
      },
      {
        $set: {
          'sync_configs.$[config].snapshot_done': false
        }
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': { $in: syncConfigIds } }]
      }
    );
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    const session = this.session;
    const ids = tables.map((table) => mongoTableId(table.id));
    const syncConfigIds = this.relevantSyncConfigIdsForTables(tables);

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

      if (no_checkpoint_before_lsn != null && syncConfigIds.length > 0) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: syncConfigIds }
          },
          {
            $set: {
              last_keepalive_ts: new Date()
            },
            $max: {
              'sync_configs.$[config].no_checkpoint_before': no_checkpoint_before_lsn
            }
          },
          {
            session: this.session,
            // Only set for sync configs that use this table
            arrayFilters: [{ 'config._id': { $in: syncConfigIds } }]
          }
        );
      }
    });
    return tables.map((table) => {
      const copy = table.clone();
      copy.snapshotComplete = true;
      return copy;
    });
  }
}
