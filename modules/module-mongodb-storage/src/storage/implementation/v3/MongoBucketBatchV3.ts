import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError, ServiceAssertionError } from '@powersync/lib-services-framework';
import { InternalOpId, storage } from '@powersync/service-core';
import { EventDefinitionId } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { canCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoParsedSyncConfigSet } from '../MongoParsedSyncConfigSet.js';
import { MongoWriteBatch } from '../MongoWriteBatch.js';
import { stopReplicationStreamPipeline } from '../SyncRuleStateUpdate.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { SyncRuleDocumentBase } from '../models.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { CustomCheckpointRequestDocumentV3, ReplicationStreamDocumentV3, SourceTableDocumentV3 } from './models.js';
import {
  createNewSourceTable,
  overlappingSourceTableFilter,
  planSourceTableReconciliation,
  sourceTableDesiredResolution,
  sourceTableFromDocument,
  SourceTableReconciliationContext
} from './source-table-utils.js';

export class MongoBucketBatchV3 extends MongoBucketBatch {
  declare readonly db: VersionedPowerSyncMongoV3;

  private readonly store: SourceRecordStore;
  /** Avoid repeating idempotent createIndexes calls for every checkpoint flush. */
  private readonly initializedCustomCheckpointEventIds = new Set<EventDefinitionId>();
  // Used only to validate supplied checkpoint ids; it does not initialize collections.
  private readonly knownEventIds: ReadonlySet<EventDefinitionId>;
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
    this.knownEventIds = new Set(this.mapping.allEventDefinitionIds());
    this.store = new SourceRecordStoreV3(this.db, this.replicationStreamId, this.mapping);
  }

  protected override get usePipeline(): boolean {
    return true;
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.replicationStreamId, this.mapping, writtenSize, {
      logger: this.logger,
      objectStorage: this.options.objectStorage,
      inlineThresholdBytes: this.options.inlineThresholdBytes,
      signal: this.uploadSignal,
      objectStorageUsageWriterId: this.objectStorageUsageWriterId
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected override persistedOpHead(stream: SyncRuleDocumentBase): InternalOpId {
    return (stream as ReplicationStreamDocumentV3).last_persisted_op ?? 0n;
  }

  protected override onReplicationTransactionFlush(writes: MongoWriteBatch, lastOp: InternalOpId): void {
    // Durably advance the stream-level head of persisted ops within the flush transaction.
    // This ensures a checkpoint created later (even by an empty commit, or by a freshly-appended
    // config that replicates nothing) covers all ops persisted before a potential crash.
    writes.updateOne(
      this.db.sync_rules,
      {
        _id: this.replicationStreamId
      },
      {
        $max: {
          last_persisted_op: lastOp
        }
      }
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
        parameterLookupSourceIds,
        [...table.eventDefinitionIds!]
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
        .sourceRecords(this.replicationStreamId, mongoTableId(table.id))
        .drop()
        .catch((error) => {
          if (lib_mongo.isMongoNamespaceNotFoundError(error)) {
            return;
          }
          throw error;
        });
    }
  }

  async resolveTables(options: storage.ResolveTablesOptions): Promise<storage.ResolveTablesResult> {
    await this.flush();
    // The test-only override is a whole parsed set, so the sync rules and the mapping
    // used below always come from the same parse.
    const parsedOverride = options.parsedSyncConfig as MongoParsedSyncConfigSet | undefined;
    const syncConfig = parsedOverride?.hydratedSyncConfig ?? this.sync_rules;
    const mapping = parsedOverride?.mapping ?? this.mapping;
    const eventById = (parsedOverride ?? this.options.parsedSyncConfig).eventById;

    const { connection_id, source } = options;
    const reconcile = options.reconcileSourceTables ?? storage.defaultSourceTableReconciler;
    const identity = {
      schema: source.schema,
      name: source.name,
      objectId: source.objectId,
      replicaIdColumns: source.replicaIdColumns.map((column) => ({
        name: column.name,
        type: column.type,
        type_oid: column.typeId
      }))
    };

    let result: storage.ResolveTablesResult | null = null;
    const session = this.db.client.startSession();
    await using _ = { [Symbol.asyncDispose]: () => session.endSession() };

    await this.withWriterAccess(() =>
      session.withTransaction(async () => {
        await this.fence(session);
        const col = this.db.sourceTables(this.replicationStreamId);

        // Find records that overlap by name or relation id.
        const candidateDocs = await col
          .find(overlappingSourceTableFilter(connection_id, identity), { session })
          .toArray();

        const candidateTables = candidateDocs.map((doc) =>
          sourceTableFromDocument(doc, source.connectionTag, syncConfig, mapping, eventById)
        );
        const candidates = candidateTables.map((table) => table.clone());
        const resolution = await reconcile({ source, candidates });
        storage.validateSourceTableCandidateResolution(candidates, resolution);

        // Persist metadata from the reconciler without mutating the queried documents.
        for (const { id, sourceMetadata } of storage.diffSourceTableUpdates(candidateTables, resolution)) {
          await col.updateOne({ _id: mongoTableId(id) }, { $set: { source_metadata: sourceMetadata } }, { session });
        }

        const context: SourceTableReconciliationContext = {
          connectionId: connection_id,
          connectionTag: source.connectionTag,
          identity,
          storeCurrentData: source.sendsCompleteRows !== true,
          syncConfig,
          mapping,
          desired: sourceTableDesiredResolution(syncConfig, source, mapping, eventById),
          sourceCompatibleTables: resolution.compatibleTables,
          newTableSourceMetadata: resolution.newTableValues.sourceMetadata
        };

        // Plan record reuse, membership changes, creation, and removal.
        const plan = planSourceTableReconciliation(candidateDocs, context);

        // Persist narrowing for incomplete snapshots only. Snapshot-complete docs keep stale
        // coverage ids so compatible future configs can reuse already-snapshotted data.
        // Narrowing occurs after removing a sync config, meaning we don't process those
        // definitions anymore.
        for (const update of plan.narrowingUpdates) {
          await col.updateOne(
            { _id: update.id },
            {
              $set: {
                bucket_data_source_ids: update.memberships.bucketDataSourceIds,
                parameter_lookup_source_ids: update.memberships.parameterLookupSourceIds,
                event_definition_ids: update.memberships.eventDefinitionIds
              }
            },
            { session }
          );
        }

        // Any desired membership not covered by an existing doc gets a new source table.
        // That table snapshots only the uncovered memberships.
        if (plan.newTableMemberships != null) {
          const id = options.idGenerator ? (options.idGenerator() as bson.ObjectId) : new bson.ObjectId();
          const { doc, table } = createNewSourceTable(id, plan.newTableMemberships, context);

          await col.insertOne(doc, { session });
          await this.db.initializeSourceRecordsCollection(this.replicationStreamId, doc._id, session);
          plan.tables.push(table);
        }

        result = {
          tables: plan.tables,
          dropTables: plan.dropDocs.map((doc) =>
            sourceTableFromDocument(doc, context.connectionTag, syncConfig, mapping, eventById)
          )
        };
      })
    );

    return result!;
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    await this.flush();
    const doc = (await this.db
      .sourceTables(this.replicationStreamId)
      .findOne({ _id: mongoTableId(table.id) }, { session: this.session })) as SourceTableDocumentV3 | null;
    if (doc == null) {
      return null;
    }

    return sourceTableFromDocument(
      doc,
      table.ref.connectionTag,
      this.sync_rules,
      this.mapping,
      this.options.parsedSyncConfig.eventById
    );
  }

  async commit(lsn: string, options?: storage.BucketBatchCommitOptions): Promise<storage.CheckpointResult> {
    const { createEmptyCheckpoints } = { ...storage.DEFAULT_BUCKET_BATCH_COMMIT_OPTIONS, ...options };

    using _ = this.tracer.span('storage', 'commit');

    const checkpoint = async (stream: SyncRuleDocumentBase, lastOp?: InternalOpId) => {
      const now = new Date();
      const preUpdateDocument = stream as ReplicationStreamDocumentV3;
      const writes = this.db.createWriteBatch(this.session, { ordered: false });
      writes.updateMany(
        this.db.write_checkpoints,
        { processed_at_lsn: null, 'lsns.1': { $lte: lsn } },
        { $set: { processed_at_lsn: lsn } }
      );

      const states =
        preUpdateDocument?.sync_configs?.filter((config) => this.syncConfigIds.some((id) => id.equals(config._id))) ??
        [];
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
      // A combined flush supplies its new head; an empty commit uses the fenced persisted head.
      const newCheckpoint = lastOp ?? this.persistedOpHead(preUpdateDocument);

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

      // Every commit advances the stream's resume position: commit() flushes first, so all
      // source changes up to this lsn have been persisted, even when checkpoints are blocked.
      // In the future we could also advance this on flush, when the connector provides the
      // current position (see setResumeLsn, which connectors may already call after flushing).
      const resumeLsnUpdate = {
        resume_lsn: lsn,
        ...(lastOp == null ? {} : { last_persisted_op: lastOp })
      };

      if (unblockedConfigIds.length > 0) {
        // All unblocked configs get the SAME new value, so we apply it with a single updateOne
        // (single-document atomicity).
        const updateSet: Record<string, any> = {
          last_fatal_error: null,
          last_fatal_error_ts: null
        };
        // Only advance checkpoint fields when an actual (non-empty) checkpoint is created, matching
        // the previous per-config / v1 behaviour.
        if (checkpointCreated) {
          updateSet['sync_configs.$[config].last_checkpoint'] = newCheckpoint;
          updateSet['sync_configs.$[config].last_checkpoint_lsn'] = lsn;
          updateSet['last_checkpoint_ts'] = now;
        }

        writes.updateOne(
          this.db.sync_rules,
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: unblockedConfigIds }
          },
          { $set: updateSet, $max: resumeLsnUpdate },
          {
            arrayFilters: checkpointCreated ? [{ 'config._id': { $in: unblockedConfigIds } }] : undefined
          }
        );
      } else {
        // All selected configs are blocked - only update keepalive/error tracking and the
        // resume position.
        writes.updateOne(
          this.db.sync_rules,
          {
            _id: this.replicationStreamId
          },
          {
            $set: {
              last_fatal_error: null,
              last_fatal_error_ts: null
            },
            $max: resumeLsnUpdate
          }
        );
      }

      await writes.execute();
      return { checkpointBlocked, checkpointCreated, checkpointLogState, newCheckpoint };
    };
    const projection = { sync_configs: 1, last_persisted_op: 1 };
    const { checkpointBlocked, checkpointCreated, checkpointLogState, newCheckpoint } = await this.flushAndCommit(
      checkpoint,
      () => this.withFencedTransaction(() => this.fence(this.session, projection), checkpoint),
      options,
      projection
    );
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
      // All configs are now checkpointed at newCheckpoint (the stream head).
      await this.sourceRecordStore.postCommitCleanup(newCheckpoint, this.logger);
    }
    if (checkpointCreated) {
      await this.db.notifyCheckpoint();
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
    using _ = this.tracer.span('storage', 'set_resume_lsn');
    // Losing occasional resume LSN would only reprocess source changes.
    // Keep the lease check atomic, but do not wait for majority replication of this resume hint.
    await this.updateStreamMetadata({ resume_lsn: { $literal: lsn } }, {}, { w: 1 });
  }

  private async autoActivateV3(lsn: string): Promise<void> {
    if (!this.needsActivationV3) {
      return;
    }

    const session = this.session;
    let activated = false;
    let needsFutureActivationCheck = true;
    await this.withWriterAccess(() =>
      session.withTransaction(async () => {
        await this.fence(session);
        // Reset on transaction retries.
        needsFutureActivationCheck = true;
        activated = false;

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
            stopReplicationStreamPipeline(),
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
          needsFutureActivationCheck = false;
        } else if (doc.state == storage.SyncRuleState.ACTIVE && processingStates.length == 0) {
          needsFutureActivationCheck = false;
        }
      })
    );
    if (activated) {
      this.logger.info(`Activated new replication stream at ${lsn}`);
      await this.db.notifyCheckpoint();
      this.needsActivationV3 = false;
    } else if (!needsFutureActivationCheck) {
      this.needsActivationV3 = false;
    }
  }

  private async updateSyncConfigMetadata(syncConfigIds: bson.ObjectId[], set: lib_mongo.mongo.Document): Promise<void> {
    await this.updateStreamMetadata(
      {
        sync_configs: {
          $map: {
            input: '$sync_configs',
            as: 'config',
            in: {
              $cond: [
                { $in: ['$$config._id', { $literal: syncConfigIds }] },
                { $mergeObjects: ['$$config', set] },
                '$$config'
              ]
            }
          }
        }
      },
      { 'sync_configs._id': { $in: syncConfigIds } }
    );
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    await this.updateSyncConfigMetadata(this.syncConfigIds, {
      snapshot_done: true,
      no_checkpoint_before: { $max: ['$$config.no_checkpoint_before', { $literal: no_checkpoint_before_lsn }] }
    });
  }

  async markSnapshotDone(no_checkpoint_before_lsn: string, options?: { throwOnConflict?: boolean }): Promise<void> {
    await this.withTransaction(async () => {
      // Protect against race conditions
      const blockingTables = await this.db
        .sourceTables(this.replicationStreamId)
        .find(this.snapshotBlockingSourceTablesFilter(), {
          session: this.session,
          projection: { schema_name: 1, table_name: 1 }
        })
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

  async markTableSnapshotRequired(table: storage.SourceTable): Promise<void> {
    const syncConfigIds = this.relevantSyncConfigIds(table);
    if (syncConfigIds.length == 0) {
      return;
    }
    await this.updateSyncConfigMetadata(syncConfigIds, { snapshot_done: false });
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    const session = this.session;
    const ids = tables.map((table) => mongoTableId(table.id));
    const syncConfigIds = this.relevantSyncConfigIdsForTables(tables);

    await this.withTransaction(async () => {
      const writes = this.db.createWriteBatch(session, { ordered: false });
      writes.updateMany(
        this.db.sourceTables(this.replicationStreamId),
        { _id: { $in: ids } },
        {
          $set: {
            snapshot_done: true
          },
          $unset: {
            snapshot_status: 1
          }
        }
      );

      if (no_checkpoint_before_lsn != null && syncConfigIds.length > 0) {
        writes.updateOne(
          this.db.sync_rules,
          {
            _id: this.replicationStreamId,
            'sync_configs._id': { $in: syncConfigIds }
          },
          {
            $max: {
              'sync_configs.$[config].no_checkpoint_before': no_checkpoint_before_lsn
            }
          },
          {
            // Only set for sync configs that use this table
            arrayFilters: [{ 'config._id': { $in: syncConfigIds } }]
          }
        );
      }
      await writes.execute();
    });
    return tables.map((table) => {
      const copy = table.clone();
      copy.snapshotComplete = true;
      return copy;
    });
  }

  protected override async batchCreateCustomWriteCheckpoints(
    session: lib_mongo.mongo.ClientSession,
    opId: InternalOpId
  ): Promise<void> {
    if (this.write_checkpoint_batch.length == 0) {
      return;
    }

    const checkpointsByEvent = Map.groupBy(this.write_checkpoint_batch, (checkpoint) =>
      this.validateCustomCheckpointEventId(checkpoint.event_id)
    );

    const writes = this.db.createWriteBatch(session, { ordered: false });
    for (const [eventId, checkpoints] of checkpointsByEvent) {
      this.batchCreateEventCustomWriteCheckpoints(writes, opId, eventId, checkpoints);
    }
    await writes.execute();
  }

  protected override async prepareCustomWriteCheckpoints(): Promise<void> {
    // Most events never produce custom checkpoints, so create collections lazily
    // only for the event ids present in this batch. This hook runs before the
    // replication transaction because MongoDB cannot create indexes within it.
    const eventIds = new Set(
      this.write_checkpoint_batch.map((checkpoint) => this.validateCustomCheckpointEventId(checkpoint.event_id))
    );
    for (const eventId of eventIds) {
      if (this.initializedCustomCheckpointEventIds.has(eventId)) {
        continue;
      }
      await this.db.initializeCustomCheckpointRequestsCollection({
        replicationStreamId: this.replicationStreamId,
        eventId
      });
      this.initializedCustomCheckpointEventIds.add(eventId);
    }
  }

  private validateCustomCheckpointEventId(eventId: EventDefinitionId | undefined): EventDefinitionId {
    if (eventId == null) {
      throw new ServiceAssertionError('V3 custom checkpoints require an event definition id');
    }

    if (!this.knownEventIds.has(eventId)) {
      throw new ServiceAssertionError(`Unknown custom checkpoint event definition ${eventId}`);
    }
    return eventId;
  }

  private batchCreateEventCustomWriteCheckpoints(
    writes: MongoWriteBatch,
    opId: InternalOpId,
    eventId: EventDefinitionId,
    checkpoints: storage.CustomWriteCheckpointOptions[]
  ): void {
    // A repeated user within an event replaces the complete checkpoint state.
    const uniqueCheckpoints = new Map(checkpoints.map((checkpoint) => [checkpoint.user_id, checkpoint]));
    writes.bulkWriteUnordered(
      this.db.customCheckpointRequests({
        eventId,
        replicationStreamId: this.replicationStreamId
      }),
      [...uniqueCheckpoints.values()].map((checkpointOptions) => {
        const set: Partial<CustomCheckpointRequestDocumentV3> = {
          user_id: checkpointOptions.user_id,
          checkpoint: checkpointOptions.checkpoint,
          op_id: opId
        };
        if (checkpointOptions.checkpoint_requested_at != null) {
          set.checkpoint_requested_at = checkpointOptions.checkpoint_requested_at;
        }

        return {
          updateOne: {
            filter: {
              user_id: checkpointOptions.user_id
            },
            update: {
              $set: set,
              ...(checkpointOptions.checkpoint_requested_at == null
                ? {
                    $unset: {
                      checkpoint_requested_at: 1
                    }
                  }
                : {})
            },
            upsert: true
          }
        };
      })
    );
  }
}
