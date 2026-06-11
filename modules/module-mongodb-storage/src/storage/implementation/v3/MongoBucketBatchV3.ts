import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { canCheckpointState } from '../CheckpointState.js';
import { MongoBucketBatch, MongoBucketBatchOptions } from '../MongoBucketBatch.js';
import { MongoParsedSyncConfigSet } from '../MongoParsedSyncConfigSet.js';
import { syncRuleStateUpdatePipeline } from '../SyncRuleStateUpdate.js';
import { PersistedBatch } from '../common/PersistedBatch.js';
import { SourceRecordStore } from '../common/SourceRecordStore.js';
import { PersistedBatchV3 } from './PersistedBatchV3.js';
import { SourceRecordStoreV3 } from './SourceRecordStoreV3.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { ReplicationStreamDocumentV3, SourceTableDocumentV3 } from './models.js';
import {
  SourceTableRetentionPlanner,
  hasSourceTableMembershipIds,
  matchingSourceTableIdentity,
  overlappingSourceTableFilter,
  sourceTableFromDocument,
  uncoveredSourceTableMembershipIds
} from './source-table-utils.js';

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
    // The test-only override is a whole parsed set, so the sync rules and the mapping
    // used below always come from the same parse.
    const parsedOverride = options.parsedSyncConfig as MongoParsedSyncConfigSet | undefined;
    const syncRules = parsedOverride?.hydratedSyncConfig ?? this.sync_rules;
    const mapping = parsedOverride?.mapping ?? this.mapping;
    const matchingSources = syncRules.getMatchingSources(ref);

    const { connection_id, source } = options;
    const { schema, name, objectId, replicaIdColumns, connectionTag, sendsCompleteRows } = source;
    const normalizedReplicaIdColumns = replicaIdColumns.map((column) => ({
      name: column.name,
      type: column.type,
      type_oid: column.typeId
    }));

    let result: storage.ResolveTablesResult | null = null;
    let ensureSourceRecordIndexesFor: bson.ObjectId[] = [];
    const session = this.db.client.startSession();
    await using _ = { [Symbol.asyncDispose]: () => session.endSession() };

    await session.withTransaction(async () => {
      const col = this.db.sourceTablesV3(this.replicationStreamId);
      const currentIdentity = { schema, name, objectId, replicaIdColumns: normalizedReplicaIdColumns };

      const candidateDocs = await col
        .find(overlappingSourceTableFilter(connection_id, currentIdentity), { session })
        .toArray();
      const exactDocs = candidateDocs.filter((doc) => matchingSourceTableIdentity(doc, currentIdentity));
      const bucketSourceById = new Map(
        matchingSources.bucketDataSources.map((source) => [mapping.bucketSourceId(source), source] as const)
      );
      const parameterLookupSourceById = new Map(
        matchingSources.parameterLookupSources.map((source) => [mapping.parameterLookupId(source), source] as const)
      );
      const desiredBucketIds = new Set(bucketSourceById.keys());
      const desiredLookupIds = new Set(parameterLookupSourceById.keys());
      const desiredMembershipIds = {
        bucketDataSourceIds: desiredBucketIds,
        parameterLookupSourceIds: desiredLookupIds
      };
      const triggersEvent = syncRules.tableTriggersEvent(ref);

      const { coveredMembershipIds, retainedDocIds, tables, narrowingUpdates } = new SourceTableRetentionPlanner({
        source: {
          schema,
          name,
          connectionTag,
          storeCurrentData: sendsCompleteRows !== true
        },
        config: {
          forceNarrowing: parsedOverride != null,
          syncRules,
          mapping
        },
        desired: {
          membershipIds: desiredMembershipIds,
          triggersEvent,
          bucketSourceById,
          parameterLookupSourceById
        }
      }).plan(exactDocs);

      for (const update of narrowingUpdates) {
        await col.updateOne(
          { _id: update.id },
          {
            $set: {
              bucket_data_source_ids: update.memberships.bucketDataSourceIds,
              parameter_lookup_source_ids: update.memberships.parameterLookupSourceIds
            }
          },
          { session }
        );
      }

      const uncoveredMembershipIds = uncoveredSourceTableMembershipIds(desiredMembershipIds, coveredMembershipIds);

      if (hasSourceTableMembershipIds(uncoveredMembershipIds) || (triggersEvent && tables.length == 0)) {
        const id = options.idGenerator ? (options.idGenerator() as bson.ObjectId) : new bson.ObjectId();
        const createDoc: SourceTableDocumentV3 = {
          _id: id,
          connection_id,
          relation_id: objectId,
          schema_name: schema,
          table_name: name,
          replica_id_columns: normalizedReplicaIdColumns,
          snapshot_done: false,
          snapshot_status: undefined,
          bucket_data_source_ids: uncoveredMembershipIds.bucketDataSourceIds,
          parameter_lookup_source_ids: uncoveredMembershipIds.parameterLookupSourceIds
        };
        const sourceTable = sourceTableFromDocument(createDoc, connectionTag, syncRules, mapping, {
          bucketDataSources: uncoveredMembershipIds.bucketDataSourceIds.map((id) => bucketSourceById.get(id)!),
          parameterLookupSources: uncoveredMembershipIds.parameterLookupSourceIds.map(
            (id) => parameterLookupSourceById.get(id)!
          )
        });
        sourceTable.storeCurrentData = sendsCompleteRows !== true;

        await col.insertOne(createDoc, { session });
        retainedDocIds.push(createDoc._id);
        tables.push(sourceTable);
      }

      if (triggersEvent) {
        // A single source row change must fire each event only once, even when memberships
        // are split over multiple source tables (connectors save each row change once per
        // table, and save() fires events per call). Designate one table per ref as the
        // event carrier. Prefer a snapshot-complete table, so that snapshotting a new
        // table for added definitions does not replay events for existing rows.
        const eventCarrier = tables.find((table) => table.snapshotComplete) ?? tables[0];
        for (const table of tables) {
          table.syncEvent = table === eventCarrier;
        }
      }
      ensureSourceRecordIndexesFor = retainedDocIds;

      const retainedDocIdStrings = new Set(retainedDocIds.map((id) => id.toHexString()));
      const conflictingTables = candidateDocs.filter(
        (doc) =>
          !retainedDocIdStrings.has(doc._id.toHexString()) &&
          (parsedOverride != null || !matchingSourceTableIdentity(doc, currentIdentity))
      );

      result = {
        tables,
        dropTables: conflictingTables.map((doc) => sourceTableFromDocument(doc, connectionTag, syncRules, mapping))
      };
    });

    // Index creation is idempotent. Running it for all retained tables - not only newly
    // created ones - heals the index if an earlier resolveTables crashed between inserting
    // the document and reaching this point (this runs outside the session on purpose, since
    // index creation cannot be transactional).
    for (const sourceTableId of ensureSourceRecordIndexesFor) {
      await this.db.initializeSourceRecordsCollection(this.replicationStreamId, sourceTableId);
    }

    return result!;
  }

  async getSourceTableStatus(table: storage.SourceTable): Promise<storage.SourceTable | null> {
    const doc = (await this.db
      .sourceTablesV3(this.replicationStreamId)
      .findOne({ _id: mongoTableId(table.id) }, { session: this.session })) as SourceTableDocumentV3 | null;
    if (doc == null) {
      return null;
    }

    const refreshed = sourceTableFromDocument(doc, table.ref.connectionTag, this.sync_rules, this.mapping);
    // The event-carrier designation is decided per resolveTables result and not persisted -
    // preserve the caller's designation instead of recomputing it from the ref, so that
    // refreshing a non-carrier table does not make it fire events.
    refreshed.syncEvent = table.syncEvent;
    return refreshed;
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

    // Every commit advances the stream's resume position: commit() flushes first, so all
    // source changes up to this lsn have been persisted, even when checkpoints are blocked.
    // In the future we could also advance this on flush, when the connector provides the
    // current position (see setResumeLsn, which connectors may already call after flushing).
    const resumeLsnUpdate = { resume_lsn: lsn };

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
        updateSet['last_checkpoint_ts'] = now;
      }

      await this.db.sync_rules.updateOne(
        {
          _id: this.replicationStreamId,
          'sync_configs._id': { $in: unblockedConfigIds }
        },
        { $set: updateSet, $max: resumeLsnUpdate },
        {
          session: this.session,
          arrayFilters: checkpointCreated ? [{ 'config._id': { $in: unblockedConfigIds } }] : undefined
        }
      );
    } else {
      // All selected configs are blocked - only update keepalive/error tracking and the
      // resume position.
      await this.db.sync_rules.updateOne(
        {
          _id: this.replicationStreamId
        },
        {
          $set: {
            last_keepalive_ts: now,
            last_fatal_error: null,
            last_fatal_error_ts: null
          },
          $max: resumeLsnUpdate
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
    await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId
      },
      {
        $set: {
          resume_lsn: lsn
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
      await this.db.sourceTablesV3(this.replicationStreamId).updateMany(
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
