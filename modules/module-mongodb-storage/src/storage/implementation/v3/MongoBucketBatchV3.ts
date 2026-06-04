import * as lib_mongo from '@powersync/lib-service-mongodb';
import { mongo } from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, storage } from '@powersync/service-core';
import { HydratedSyncConfig, MatchingSources } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { mongoTableId } from '../../../utils/util.js';
import { calculateCheckpointState } from '../CheckpointState.js';
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

function mergeStableIds(existing: string[], added: Iterable<string>): string[] {
  return [...new Set([...existing, ...added])].sort();
}

function hasSelectedOwner(owners: readonly string[] | undefined, selectedSyncConfigIds: Set<string>): boolean {
  return owners?.some((id) => selectedSyncConfigIds.has(id)) ?? false;
}

function updateMembershipOwners<T extends string>(
  ids: T[],
  existingOwners: Partial<Record<T, string[]>> | undefined,
  desiredOwners: Map<T, string[]>,
  selectedSyncConfigIds: Set<string>,
  options?: { addMissingDesiredIds?: boolean }
): Partial<Record<T, string[]>> {
  const updated: Partial<Record<T, string[]>> = {};
  for (const id of ids) {
    const remainingOwners = (existingOwners?.[id] ?? [...selectedSyncConfigIds]).filter(
      (owner) => !selectedSyncConfigIds.has(owner)
    );
    const ownersToAdd = desiredOwners.get(id);
    if (ownersToAdd != null) {
      updated[id] = mergeStableIds(remainingOwners, ownersToAdd);
    } else if (remainingOwners.length > 0) {
      updated[id] = remainingOwners;
    }
  }

  if (options?.addMissingDesiredIds) {
    for (const [id, owners] of desiredOwners) {
      if (updated[id] == null) {
        updated[id] = owners;
      }
    }
  }
  return updated;
}

function addDesiredOwner<T extends string>(
  owners: Map<T, string[]>,
  id: T,
  syncConfigId: string | undefined,
  fallbackSyncConfigIds: string[]
) {
  owners.set(id, mergeStableIds(owners.get(id) ?? [], syncConfigId == null ? fallbackSyncConfigIds : [syncConfigId]));
}

function removeOwners<T extends string>(
  ids: T[],
  owners: Partial<Record<T, string[]>> | undefined,
  removedSyncConfigIds: Set<string>
): { ids: T[]; owners: Partial<Record<T, string[]>> } {
  const retainedOwners: Partial<Record<T, string[]>> = {};
  const retainedIds: T[] = [];
  for (const id of ids) {
    const retained = (owners?.[id] ?? []).filter((owner) => !removedSyncConfigIds.has(owner));
    if (retained.length > 0) {
      retainedIds.push(id);
      retainedOwners[id] = retained;
    }
  }
  return { ids: retainedIds, owners: retainedOwners };
}

export class MongoBucketBatchV3 extends MongoBucketBatch {
  declare public readonly db: VersionedPowerSyncMongoV3;

  private readonly store: SourceRecordStore;
  private readonly syncConfigIds: bson.ObjectId[];
  private readonly syncConfigIdStrings: string[];
  private needsActivationV3 = true;
  private lastWaitingLogThrottledV3 = 0;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    const syncConfigIds = options.syncConfigIds ?? [];
    if (syncConfigIds.length == 0) {
      throw new ReplicationAssertionError('Missing sync config id for v3 batch');
    }
    this.syncConfigIds = syncConfigIds;
    this.syncConfigIdStrings = syncConfigIds.map((id) => id.toHexString()).sort();
    this.store = new SourceRecordStoreV3(this.db, this.group_id, this.mapping);
  }

  protected createPersistedBatch(writtenSize: number): PersistedBatch {
    return new PersistedBatchV3(this.db, this.group_id, this.mapping, writtenSize, {
      logger: this.logger
    });
  }

  protected get sourceRecordStore(): SourceRecordStore {
    return this.store;
  }

  protected async cleanupDroppedSourceTables(sourceTables: storage.SourceTable[]) {
    for (const table of sourceTables) {
      await this.db
        .sourceRecordsV3(this.group_id, mongoTableId(table.id))
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
      const col = this.db.commonSourceTables(this.group_id);
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
      const desiredBucketOwners = new Map<string, string[]>();
      for (const source of matchingSources.bucketDataSources) {
        addDesiredOwner(
          desiredBucketOwners,
          this.mapping.bucketSourceId(source),
          this.mapping.bucketSourceSyncConfigId(source),
          this.syncConfigIdStrings
        );
      }
      const desiredLookupOwners = new Map<string, string[]>();
      for (const source of matchingSources.parameterLookupSources) {
        addDesiredOwner(
          desiredLookupOwners,
          this.mapping.parameterLookupId(source),
          this.mapping.parameterLookupSyncConfigId(source),
          this.syncConfigIdStrings
        );
      }
      const desiredHasMembership = desiredBucketIds.size > 0 || desiredLookupIds.size > 0;
      const triggersEvent = syncRules.tableTriggersEvent(ref);
      const selectedSyncConfigIds = new Set(this.syncConfigIdStrings);

      const coveredBucketIds = new Set<string>();
      const coveredLookupIds = new Set<string>();
      const retainedDocIds: bson.ObjectId[] = [];
      const tables: storage.SourceTable[] = [];
      let retainedEventOnlyTable = false;

      for (const doc of exactDocs) {
        const bucketOwners = updateMembershipOwners(
          doc.bucket_data_source_ids,
          doc.bucket_data_source_sync_config_ids,
          desiredBucketOwners,
          selectedSyncConfigIds
        );
        const parameterOwners = updateMembershipOwners(
          doc.parameter_lookup_source_ids,
          doc.parameter_lookup_source_sync_config_ids,
          desiredLookupOwners,
          selectedSyncConfigIds
        );
        const bucketDataSourceIds = Object.keys(bucketOwners);
        const parameterLookupSourceIds = Object.keys(parameterOwners);
        const selectedBucketDataSourceIds = bucketDataSourceIds.filter((id) =>
          hasSelectedOwner(bucketOwners[id], selectedSyncConfigIds)
        );
        const selectedParameterLookupSourceIds = parameterLookupSourceIds.filter((id) =>
          hasSelectedOwner(parameterOwners[id], selectedSyncConfigIds)
        );
        const coversDesiredMembership =
          selectedBucketDataSourceIds.length > 0 || selectedParameterLookupSourceIds.length > 0;
        const eventOwners = mergeStableIds(
          (doc.event_sync_config_ids ?? []).filter((id) => !selectedSyncConfigIds.has(id)),
          triggersEvent ? selectedSyncConfigIds : []
        );
        const coversEventOnlyTable =
          !desiredHasMembership &&
          triggersEvent &&
          hasSelectedOwner(eventOwners, selectedSyncConfigIds) &&
          !retainedEventOnlyTable;

        for (const id of selectedBucketDataSourceIds) {
          coveredBucketIds.add(id);
        }
        for (const id of selectedParameterLookupSourceIds) {
          coveredLookupIds.add(id);
        }

        const updates: Partial<SourceTableDocumentV3> = {};
        if (
          !sameStringArray(doc.bucket_data_source_ids, bucketDataSourceIds) ||
          !sameStringArray(doc.parameter_lookup_source_ids, parameterLookupSourceIds) ||
          JSON.stringify(doc.bucket_data_source_sync_config_ids ?? {}) != JSON.stringify(bucketOwners) ||
          JSON.stringify(doc.parameter_lookup_source_sync_config_ids ?? {}) != JSON.stringify(parameterOwners) ||
          !sameStringArray(doc.event_sync_config_ids ?? [], eventOwners)
        ) {
          updates.bucket_data_source_ids = bucketDataSourceIds;
          updates.parameter_lookup_source_ids = parameterLookupSourceIds;
          updates.bucket_data_source_sync_config_ids = bucketOwners;
          updates.parameter_lookup_source_sync_config_ids = parameterOwners;
          updates.event_sync_config_ids = eventOwners;
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
              parameter_lookup_source_ids: parameterLookupSourceIds,
              bucket_data_source_sync_config_ids: bucketOwners,
              parameter_lookup_source_sync_config_ids: parameterOwners,
              event_sync_config_ids: eventOwners
            },
            connectionTag,
            syncRules,
            {
              bucketDataSources: selectedBucketDataSourceIds.map((id) => bucketSourceById.get(id)!),
              parameterLookupSources: selectedParameterLookupSourceIds.map((id) => parameterLookupSourceById.get(id)!)
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
          parameter_lookup_source_ids: uncoveredLookupIds,
          bucket_data_source_sync_config_ids: updateMembershipOwners(
            [],
            undefined,
            new Map(uncoveredBucketIds.map((id) => [id, desiredBucketOwners.get(id) ?? this.syncConfigIdStrings])),
            selectedSyncConfigIds,
            { addMissingDesiredIds: true }
          ),
          parameter_lookup_source_sync_config_ids: updateMembershipOwners(
            [],
            undefined,
            new Map(uncoveredLookupIds.map((id) => [id, desiredLookupOwners.get(id) ?? this.syncConfigIdStrings])),
            selectedSyncConfigIds,
            { addMissingDesiredIds: true }
          ),
          event_sync_config_ids: triggersEvent ? this.syncConfigIdStrings : []
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
      await this.db.initializeSourceRecordsCollection(this.group_id, sourceTableId);
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
    const selectedSyncConfigIds = new Set(this.syncConfigIdStrings);
    const bucketDataSourceIds = new Set(
      doc.bucket_data_source_ids.filter((id) =>
        hasSelectedOwner(doc.bucket_data_source_sync_config_ids?.[id], selectedSyncConfigIds)
      )
    );
    const parameterLookupSourceIds = new Set(
      doc.parameter_lookup_source_ids.filter((id) =>
        hasSelectedOwner(doc.parameter_lookup_source_sync_config_ids?.[id], selectedSyncConfigIds)
      )
    );

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
      .commonSourceTables(this.group_id)
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

    const preUpdateDocument = await this.db.sync_rules.findOne(
      {
        _id: this.group_id,
        'sync_configs._id': { $in: this.syncConfigIds }
      },
      {
        session: this.session,
        projection: {
          snapshot_lsn: 1,
          sync_configs: 1
        }
      }
    );

    const states =
      (preUpdateDocument as ReplicationStreamDocumentV3)?.sync_configs?.filter((config) =>
        this.syncConfigIds.some((id) => id.equals(config._id))
      ) ?? [];
    if (states.length == 0) {
      throw new ReplicationAssertionError(
        `Failed to update checkpoint - no matching sync_config for _id: ${this.group_id}/${this.syncConfigIds
          .map((id) => id.toHexString())
          .join(',')}`
      );
    }

    let checkpointBlocked = false;
    let checkpointCreated = false;
    let checkpointLogState: unknown = null;
    const cleanupCheckpoints: bigint[] = [];

    for (const state of states) {
      const checkpointState = calculateCheckpointState({
        lsn,
        snapshotDone: state.snapshot_done === true,
        lastCheckpointLsn: state.last_checkpoint_lsn,
        noCheckpointBefore: state.no_checkpoint_before,
        keepaliveOp: state.keepalive_op == null ? null : BigInt(state.keepalive_op),
        lastCheckpoint: state.last_checkpoint,
        persistedOp: this.persisted_op,
        createEmptyCheckpoints
      });

      checkpointBlocked ||= checkpointState.checkpointBlocked;
      checkpointCreated ||= checkpointState.checkpointCreated;
      if (checkpointState.newLastCheckpoint != null) {
        cleanupCheckpoints.push(checkpointState.newLastCheckpoint);
      }
      checkpointLogState ??= {
        snapshot_done: state.snapshot_done,
        last_checkpoint_lsn: state.last_checkpoint_lsn,
        no_checkpoint_before: state.no_checkpoint_before
      };

      const updateSet: Record<string, any> = {
        last_keepalive_ts: now,
        last_fatal_error: null,
        last_fatal_error_ts: null,
        'sync_configs.$[config].keepalive_op': checkpointState.newKeepaliveOp,
        'sync_configs.$[config].last_checkpoint': checkpointState.newLastCheckpoint
      };
      if (checkpointState.checkpointCreated) {
        updateSet['sync_configs.$[config].last_checkpoint_lsn'] = lsn;
        updateSet['snapshot_lsn'] = null;
        updateSet['last_checkpoint_ts'] = now;
      }

      await this.db.sync_rules.updateOne(
        {
          _id: this.group_id,
          'sync_configs._id': state._id
        },
        {
          $set: updateSet
        },
        {
          session: this.session,
          arrayFilters: [{ 'config._id': state._id }]
        }
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
      this.persisted_op = null;
      this.last_checkpoint_lsn = lsn;
      const cleanupCheckpoint = cleanupCheckpoints.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0))[0];
      if (cleanupCheckpoint != null) {
        await this.sourceRecordStore.postCommitCleanup(cleanupCheckpoint, this.logger);
      }
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
        _id: this.group_id,
        'sync_configs._id': { $in: this.syncConfigIds }
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
          _id: this.group_id,
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
            _id: this.group_id,
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
            _id: { $ne: this.group_id },
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
        const stoppedSyncConfigIds = states
          .filter((state) => state.state == storage.SyncRuleState.ACTIVE)
          .map((state) => state._id.toHexString());
        await this.db.sync_rules.updateOne(
          {
            _id: this.group_id,
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
        await this.removeSourceTableOwners(stoppedSyncConfigIds, session);
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

  private async removeSourceTableOwners(removedSyncConfigIdList: string[], session: mongo.ClientSession) {
    if (removedSyncConfigIdList.length == 0) {
      return;
    }

    const removedSyncConfigIds = new Set(removedSyncConfigIdList);
    const updates: mongo.AnyBulkWriteOperation<SourceTableDocumentV3>[] = [];
    const sourceTables = await this.db.sourceTablesV3(this.group_id).find({}, { session }).toArray();
    for (const sourceTable of sourceTables) {
      const bucket = removeOwners(
        sourceTable.bucket_data_source_ids,
        sourceTable.bucket_data_source_sync_config_ids,
        removedSyncConfigIds
      );
      const parameters = removeOwners(
        sourceTable.parameter_lookup_source_ids,
        sourceTable.parameter_lookup_source_sync_config_ids,
        removedSyncConfigIds
      );
      const eventSyncConfigIds = (sourceTable.event_sync_config_ids ?? []).filter(
        (id) => !removedSyncConfigIds.has(id)
      );

      if (
        sameStringArray(sourceTable.bucket_data_source_ids, bucket.ids) &&
        sameStringArray(sourceTable.parameter_lookup_source_ids, parameters.ids) &&
        JSON.stringify(sourceTable.bucket_data_source_sync_config_ids ?? {}) == JSON.stringify(bucket.owners) &&
        JSON.stringify(sourceTable.parameter_lookup_source_sync_config_ids ?? {}) ==
          JSON.stringify(parameters.owners) &&
        sameStringArray(sourceTable.event_sync_config_ids ?? [], eventSyncConfigIds)
      ) {
        continue;
      }

      updates.push({
        updateOne: {
          filter: { _id: sourceTable._id },
          update: {
            $set: {
              bucket_data_source_ids: bucket.ids,
              parameter_lookup_source_ids: parameters.ids,
              bucket_data_source_sync_config_ids: bucket.owners,
              parameter_lookup_source_sync_config_ids: parameters.owners,
              event_sync_config_ids: eventSyncConfigIds
            }
          }
        }
      });
    }

    if (updates.length > 0) {
      await this.db.sourceTablesV3(this.group_id).bulkWrite(updates, { session, ordered: false });
    }
  }

  async markAllSnapshotDone(no_checkpoint_before_lsn: string): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id,
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
      const count = await this.db.sourceTablesV3(this.group_id).countDocuments(
        {
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
        _id: this.group_id,
        'sync_configs._id': { $in: this.syncConfigIds }
      },
      {
        $set: {
          'sync_configs.$[config].snapshot_done': false
        }
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': { $in: this.syncConfigIds } }]
      }
    );
  }

  async markTableSnapshotDone(
    tables: storage.SourceTable[],
    no_checkpoint_before_lsn?: string
  ): Promise<storage.SourceTable[]> {
    const session = this.session;
    const ids = tables.map((table) => mongoTableId(table.id));

    await this.withTransaction(async () => {
      await this.db.commonSourceTables(this.group_id).updateMany(
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
            _id: this.group_id,
            'sync_configs._id': { $in: this.syncConfigIds }
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
            arrayFilters: [{ 'config._id': { $in: this.syncConfigIds } }]
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
