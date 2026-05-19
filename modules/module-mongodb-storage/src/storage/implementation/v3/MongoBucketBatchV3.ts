import * as lib_mongo from '@powersync/lib-service-mongodb';
import { ReplicationAssertionError } from '@powersync/lib-services-framework';
import { ColumnDescriptor, storage } from '@powersync/service-core';
import { HydratedSyncRules, MatchingSources } from '@powersync/service-sync-rules';
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

export class MongoBucketBatchV3 extends MongoBucketBatch {
  get db(): VersionedPowerSyncMongoV3 {
    return this._db as VersionedPowerSyncMongoV3;
  }

  private readonly store: SourceRecordStore;
  private readonly syncConfigId: bson.ObjectId;
  private needsActivationV3 = true;
  private lastWaitingLogThrottledV3 = 0;

  constructor(options: MongoBucketBatchOptions) {
    super(options);
    if (options.syncConfigId == null) {
      throw new ReplicationAssertionError('Missing sync config id for v3 batch');
    }
    this.syncConfigId = options.syncConfigId;
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
    const { schema, name, objectId, replicaIdColumns, connectionTag } = source;
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

        if (
          !sameStringArray(doc.bucket_data_source_ids, bucketDataSourceIds) ||
          !sameStringArray(doc.parameter_lookup_source_ids, parameterLookupSourceIds)
        ) {
          await col.updateOne(
            { _id: doc._id },
            {
              $set: {
                bucket_data_source_ids: bucketDataSourceIds,
                parameter_lookup_source_ids: parameterLookupSourceIds
              }
            },
            { session }
          );
        }

        if (coversDesiredMembership || coversEventOnlyTable) {
          if (coversEventOnlyTable) {
            retainedEventOnlyTable = true;
          }
          retainedDocIds.push(doc._id);
          tables.push(
            this.sourceTableFromDocument(
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
            )
          );
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
      await this.db.initializeSourceRecordsCollection(this.group_id, sourceTableId);
    }

    return result!;
  }

  private sourceTableFromDocument(
    doc: SourceTableDocumentV3,
    connectionTag: string,
    syncRules: HydratedSyncRules,
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
    syncRules: HydratedSyncRules
  ): MatchingSources {
    const bucketDataSourceIds = new Set(doc.bucket_data_source_ids);
    const parameterLookupSourceIds = new Set(doc.parameter_lookup_source_ids);

    return {
      bucketDataSources: syncRules.definition.bucketDataSources.filter((source) =>
        bucketDataSourceIds.has(this.mapping.bucketSourceId(source))
      ),
      parameterLookupSources: syncRules.definition.bucketParameterLookupSources.filter((source) =>
        parameterLookupSourceIds.has(this.mapping.parameterLookupId(source))
      )
    };
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
        'sync_configs._id': this.syncConfigId
      },
      {
        session: this.session,
        projection: {
          snapshot_lsn: 1,
          sync_configs: {
            $elemMatch: {
              _id: this.syncConfigId
            }
          }
        }
      }
    );

    const state = (preUpdateDocument as ReplicationStreamDocumentV3)?.sync_configs?.[0];
    if (state == null) {
      throw new ReplicationAssertionError(
        `Failed to update checkpoint - no matching sync_config for _id: ${this.group_id}/${this.syncConfigId.toHexString()}`
      );
    }

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
        'sync_configs._id': this.syncConfigId
      },
      {
        $set: updateSet
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': this.syncConfigId }]
      }
    );

    if (checkpointState.checkpointBlocked) {
      if (Date.now() - this.lastWaitingLogThrottledV3 > 5_000) {
        this.logger.info(
          `Waiting before creating checkpoint, currently at ${lsn} / ${checkpointState.newKeepaliveOp}. Current state: ${JSON.stringify(
            {
              snapshot_done: state.snapshot_done,
              last_checkpoint_lsn: state.last_checkpoint_lsn,
              no_checkpoint_before: state.no_checkpoint_before
            }
          )}`
        );
        this.lastWaitingLogThrottledV3 = Date.now();
      }
    } else {
      if (checkpointState.checkpointCreated) {
        this.logger.debug(`Created checkpoint at ${lsn} / ${checkpointState.newLastCheckpoint}`);
      }
      await this.autoActivateV3(lsn);
      await this.db.notifyCheckpoint();
      this.persisted_op = null;
      this.last_checkpoint_lsn = lsn;
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
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
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
          'sync_configs._id': this.syncConfigId
        },
        {
          session,
          projection: {
            state: 1,
            sync_configs: {
              $elemMatch: {
                _id: this.syncConfigId
              }
            }
          }
        }
      );
      const state = (doc as ReplicationStreamDocumentV3)?.sync_configs?.[0];
      if (
        doc &&
        doc.state == storage.SyncRuleState.PROCESSING &&
        state?.state == storage.SyncRuleState.PROCESSING &&
        state.snapshot_done &&
        state.last_checkpoint != null
      ) {
        await this.db.sync_rules.updateOne(
          {
            _id: this.group_id,
            'sync_configs._id': this.syncConfigId
          },
          {
            $set: {
              state: storage.SyncRuleState.ACTIVE,
              'sync_configs.$[config].state': storage.SyncRuleState.ACTIVE
            }
          },
          {
            session,
            arrayFilters: [{ 'config._id': this.syncConfigId }]
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
      } else if (doc?.state != storage.SyncRuleState.PROCESSING) {
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
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
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
        arrayFilters: [{ 'config._id': this.syncConfigId }]
      }
    );
  }

  async markTableSnapshotRequired(_table: storage.SourceTable): Promise<void> {
    await this.db.sync_rules.updateOne(
      {
        _id: this.group_id,
        'sync_configs._id': this.syncConfigId
      },
      {
        $set: {
          'sync_configs.$[config].snapshot_done': false
        }
      },
      {
        session: this.session,
        arrayFilters: [{ 'config._id': this.syncConfigId }]
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
            'sync_configs._id': this.syncConfigId
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
            arrayFilters: [{ 'config._id': this.syncConfigId }]
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
