import * as lib_mongo from '@powersync/lib-service-mongodb';
import { Logger, ReplicationAbortedError } from '@powersync/lib-services-framework';
import { storage } from '@powersync/service-core';
import { BucketDefinitionId, ParameterIndexId, SyncConfig } from '@powersync/service-sync-rules';
import * as bson from 'bson';
import { SingleSyncConfigBucketDefinitionMapping } from '../BucketDefinitionMapping.js';
import { VersionedPowerSyncMongoV3 } from './VersionedPowerSyncMongoV3.js';
import { ReplicationStreamDocumentV3, SourceTableDocumentV3, SyncConfigDefinition } from './models.js';

type SyncConfigState = ReplicationStreamDocumentV3['sync_configs'][number];

const LIVE_STATES = new Set<storage.SyncRuleState>([
  storage.SyncRuleState.ACTIVE,
  storage.SyncRuleState.PROCESSING,
  storage.SyncRuleState.ERRORED
]);

const EMPTY_RESULT: storage.CleanupStoppedSyncConfigsResult = {
  stoppedSyncConfigsRemoved: 0,
  bucketDataCollectionsDropped: 0,
  parameterIndexCollectionsDropped: 0,
  sourceRecordCollectionsDropped: 0,
  sourceTablesUpdated: 0,
  sourceTablesDeleted: 0
};

export interface MongoStoppedSyncConfigCleanupOptions extends storage.CleanupStoppedSyncConfigsOptions {
  replicationStreamId: number;
  db: VersionedPowerSyncMongoV3;
  logger: Logger;
}

export class MongoStoppedSyncConfigCleanup {
  private readonly db: VersionedPowerSyncMongoV3;
  private readonly replicationStreamId: number;
  private readonly signal: AbortSignal | undefined;
  private readonly logger: Logger;
  private readonly defaultSchema: string;
  private readonly sourceConnectionTag: string;

  constructor(options: MongoStoppedSyncConfigCleanupOptions) {
    this.db = options.db;
    this.replicationStreamId = options.replicationStreamId;
    this.signal = options.signal;
    this.logger = options.logger;
    this.defaultSchema = options.defaultSchema;
    this.sourceConnectionTag = options.sourceConnectionTag;
  }

  async run(): Promise<storage.CleanupStoppedSyncConfigsResult> {
    this.throwIfAborted();

    const doc = await this.db.sync_rules.findOne<ReplicationStreamDocumentV3>(
      { _id: this.replicationStreamId },
      { projection: { sync_configs: 1 } }
    );
    if (doc == null || doc.sync_configs.length == 0) {
      return { ...EMPTY_RESULT };
    }

    const stoppedStates = doc.sync_configs.filter((config) => config.state == storage.SyncRuleState.STOP);
    if (stoppedStates.length == 0) {
      return { ...EMPTY_RESULT };
    }

    const liveStates = doc.sync_configs.filter((config) => LIVE_STATES.has(config.state));
    const configDocs = await this.loadSyncConfigDefinitions([...stoppedStates, ...liveStates]);
    const stoppedIds = new Set(stoppedStates.map((state) => state._id.toHexString()));
    const liveIds = new Set(liveStates.map((state) => state._id.toHexString()));
    const stoppedConfigDocs = configDocs.filter((config) => stoppedIds.has(config._id.toHexString()));
    const liveConfigDocs = configDocs.filter((config) => liveIds.has(config._id.toHexString()));

    const stoppedStorageIds = this.storageIdsFor(stoppedConfigDocs);
    const liveStorageIds = this.storageIdsFor(liveConfigDocs);
    const unusedBucketDefinitionIds = difference(
      stoppedStorageIds.bucketDefinitionIds,
      liveStorageIds.bucketDefinitionIds
    );
    const unusedParameterIndexIds = difference(stoppedStorageIds.parameterIndexIds, liveStorageIds.parameterIndexIds);

    const result: storage.CleanupStoppedSyncConfigsResult = {
      ...EMPTY_RESULT
    };

    if (unusedBucketDefinitionIds.length > 0 || unusedParameterIndexIds.length > 0) {
      await this.cleanupSourceTableMemberships(
        unusedBucketDefinitionIds,
        unusedParameterIndexIds,
        liveConfigDocs,
        result
      );
      result.bucketDataCollectionsDropped = await this.dropBucketDataCollections(unusedBucketDefinitionIds);
      result.parameterIndexCollectionsDropped = await this.dropParameterIndexCollections(unusedParameterIndexIds);
    }

    result.stoppedSyncConfigsRemoved = await this.pruneStoppedSyncConfigStates(stoppedStates);

    if (result.stoppedSyncConfigsRemoved > 0) {
      this.logger.info(
        `Cleaned up ${result.stoppedSyncConfigsRemoved} stopped sync config${result.stoppedSyncConfigsRemoved == 1 ? '' : 's'}`,
        result
      );
    }
    return result;
  }

  private async loadSyncConfigDefinitions(states: SyncConfigState[]): Promise<SyncConfigDefinition[]> {
    const ids = [...new Map(states.map((state) => [state._id.toHexString(), state._id])).values()];
    if (ids.length == 0) {
      return [];
    }
    return this.db.syncConfigDefinitions.find({ _id: { $in: ids } }).toArray();
  }

  private storageIdsFor(configs: Pick<SyncConfigDefinition, 'rule_mapping'>[]) {
    const mappings = configs.map((config) => SingleSyncConfigBucketDefinitionMapping.fromSyncConfig(config));
    return {
      bucketDefinitionIds: [...new Set(mappings.flatMap((mapping) => mapping.allBucketDefinitionIds()))],
      parameterIndexIds: [...new Set(mappings.flatMap((mapping) => mapping.allParameterIndexIds()))]
    };
  }

  private async cleanupSourceTableMemberships(
    unusedBucketDefinitionIds: BucketDefinitionId[],
    unusedParameterIndexIds: ParameterIndexId[],
    liveConfigDocs: SyncConfigDefinition[],
    result: storage.CleanupStoppedSyncConfigsResult
  ) {
    const update: Record<string, unknown> = {};
    if (unusedBucketDefinitionIds.length > 0) {
      update.bucket_data_source_ids = { $in: unusedBucketDefinitionIds };
    }
    if (unusedParameterIndexIds.length > 0) {
      update.parameter_lookup_source_ids = { $in: unusedParameterIndexIds };
    }

    // Keep obsolete membership ids as the durable cleanup marker until each source table is
    // either deleted or retained. If interrupted after dropping a source_records collection,
    // the next run can still rediscover the source table from these obsolete ids and retry.
    const filter = this.sourceTableMembershipFilter(unusedBucketDefinitionIds, unusedParameterIndexIds);
    const candidateSourceTables = await this.db
      .sourceTables(this.replicationStreamId)
      .find(filter, {
        projection: {
          _id: 1,
          bucket_data_source_ids: 1,
          parameter_lookup_source_ids: 1,
          schema_name: 1,
          table_name: 1
        }
      })
      .toArray();
    if (candidateSourceTables.length == 0) {
      return;
    }
    const liveSyncConfigs = this.parseSyncConfigs(liveConfigDocs);

    const deletableSourceTables = candidateSourceTables.filter(
      (sourceTable) =>
        this.membershipsBecomeEmpty(sourceTable, unusedBucketDefinitionIds, unusedParameterIndexIds) &&
        !this.triggersLiveEvent(sourceTable, liveSyncConfigs)
    );
    const retainedSourceTableIds = candidateSourceTables
      .filter((sourceTable) => !deletableSourceTables.some((deletable) => deletable._id.equals(sourceTable._id)))
      .map((sourceTable) => sourceTable._id);

    // Re-fetch rows before deleting, so we don't delete a source table that became live
    // through another writer after the planning read above.
    const deletableSourceTableIds = deletableSourceTables.map((table) => table._id);
    const sourceTablesToDelete =
      deletableSourceTableIds.length == 0
        ? []
        : await this.db
            .sourceTables(this.replicationStreamId)
            .find(
              this.deletableSourceTableFilter(
                deletableSourceTableIds,
                unusedBucketDefinitionIds,
                unusedParameterIndexIds
              ),
              {
                projection: { _id: 1 }
              }
            )
            .toArray();

    // Drop sourceRecords collections before the related sourceTables entries, so that we
    // can recover after interruptions.
    for (const sourceTable of sourceTablesToDelete) {
      this.throwIfAborted();
      await this.dropCollection(this.db.sourceRecords(this.replicationStreamId, sourceTable._id));
      result.sourceRecordCollectionsDropped += 1;
    }

    if (sourceTablesToDelete.length > 0) {
      this.throwIfAborted();
      const deleteResult = await this.db.sourceTables(this.replicationStreamId).deleteMany(
        this.deletableSourceTableFilter(
          sourceTablesToDelete.map((table) => table._id),
          unusedBucketDefinitionIds,
          unusedParameterIndexIds
        ),
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      );
      result.sourceTablesDeleted += deleteResult.deletedCount;
    }

    if (retainedSourceTableIds.length > 0) {
      this.throwIfAborted();
      const updateResult = await this.db.sourceTables(this.replicationStreamId).updateMany(
        { _id: { $in: retainedSourceTableIds } },
        {
          $pull: update
        },
        { maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }
      );
      result.sourceTablesUpdated += updateResult.modifiedCount;
    }
  }

  private membershipsBecomeEmpty(
    sourceTable: Pick<SourceTableDocumentV3, 'bucket_data_source_ids' | 'parameter_lookup_source_ids'>,
    unusedBucketDefinitionIds: BucketDefinitionId[],
    unusedParameterIndexIds: ParameterIndexId[]
  ): boolean {
    const unusedBucketDefinitionIdSet = new Set(unusedBucketDefinitionIds);
    const unusedParameterIndexIdSet = new Set(unusedParameterIndexIds);
    return (
      sourceTable.bucket_data_source_ids.every((id) => unusedBucketDefinitionIdSet.has(id)) &&
      sourceTable.parameter_lookup_source_ids.every((id) => unusedParameterIndexIdSet.has(id))
    );
  }

  private deletableSourceTableFilter(
    ids: bson.ObjectId[],
    unusedBucketDefinitionIds: BucketDefinitionId[],
    unusedParameterIndexIds: ParameterIndexId[]
  ): Record<string, unknown> {
    return {
      _id: { $in: ids },
      bucket_data_source_ids: { $not: { $elemMatch: { $nin: unusedBucketDefinitionIds } } },
      parameter_lookup_source_ids: { $not: { $elemMatch: { $nin: unusedParameterIndexIds } } }
    };
  }

  private parseSyncConfigs(configDocs: SyncConfigDefinition[]): SyncConfig[] {
    // This is ugly - we should not need to re-parse to achieve this.
    // Revisit persistence for this later.
    return configDocs.map((config) => {
      return storage.parsePersistedSyncConfigContent({
        content: config.content,
        compiledPlan: config.serialized_plan ?? null,
        storageVersion: config.storage_version,
        parseOptions: {
          defaultSchema: this.defaultSchema
        }
      }).config;
    });
  }

  private triggersLiveEvent(sourceTable: SourceTableDocumentV3, liveSyncConfigs: SyncConfig[]): boolean {
    return liveSyncConfigs.some((syncConfig) =>
      syncConfig.tableTriggersEvent({
        connectionTag: this.sourceConnectionTag,
        schema: sourceTable.schema_name,
        name: sourceTable.table_name
      })
    );
  }

  private sourceTableMembershipFilter(
    bucketDefinitionIds: BucketDefinitionId[],
    parameterIndexIds: ParameterIndexId[]
  ): Partial<SourceTableDocumentV3> | Record<string, unknown> {
    const clauses: Record<string, unknown>[] = [];
    if (bucketDefinitionIds.length > 0) {
      clauses.push({ bucket_data_source_ids: { $in: bucketDefinitionIds } });
    }
    if (parameterIndexIds.length > 0) {
      clauses.push({ parameter_lookup_source_ids: { $in: parameterIndexIds } });
    }
    if (clauses.length == 0) {
      return { _id: { $exists: false } };
    }
    return { $or: clauses };
  }

  private async dropBucketDataCollections(definitionIds: BucketDefinitionId[]): Promise<number> {
    for (const definitionId of definitionIds) {
      this.throwIfAborted();
      await this.dropCollection(this.db.bucketData(this.replicationStreamId, definitionId));
    }
    return definitionIds.length;
  }

  private async dropParameterIndexCollections(indexIds: ParameterIndexId[]): Promise<number> {
    for (const indexId of indexIds) {
      this.throwIfAborted();
      await this.dropCollection(this.db.parameterIndex(this.replicationStreamId, indexId));
    }
    return indexIds.length;
  }

  private async dropCollection<T extends lib_mongo.mongo.Document>(
    collection: lib_mongo.mongo.Collection<T>
  ): Promise<void> {
    await collection.drop({ maxTimeMS: lib_mongo.db.MONGO_CLEAR_OPERATION_TIMEOUT_MS }).catch((error) => {
      if (lib_mongo.isMongoServerError(error) && error.codeName === 'NamespaceNotFound') {
        return;
      }
      throw error;
    });
  }

  private async pruneStoppedSyncConfigStates(stoppedStates: SyncConfigState[]): Promise<number> {
    this.throwIfAborted();
    const stoppedIds = stoppedStates.map((state) => state._id);
    const result = await this.db.sync_rules.updateOne(
      {
        _id: this.replicationStreamId,
        sync_configs: {
          $not: {
            $elemMatch: {
              _id: { $in: stoppedIds },
              state: { $ne: storage.SyncRuleState.STOP }
            }
          }
        }
      },
      {
        $pull: {
          sync_configs: {
            _id: { $in: stoppedIds },
            state: storage.SyncRuleState.STOP
          }
        }
      } as bson.Document
    );
    if (result.modifiedCount == 0 && stoppedIds.length > 0) {
      this.logger.warn(`Skipped pruning stopped sync configs`);
      return 0;
    }
    return stoppedStates.length;
  }

  private throwIfAborted() {
    if (this.signal?.aborted) {
      throw new ReplicationAbortedError('Aborted stopped sync config cleanup', this.signal.reason);
    }
  }
}

function difference<T>(left: T[], right: T[]): T[] {
  const rightSet = new Set(right);
  return left.filter((value) => !rightSet.has(value));
}
