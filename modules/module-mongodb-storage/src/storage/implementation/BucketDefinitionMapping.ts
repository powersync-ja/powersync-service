import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  BucketDataSource,
  BucketDefinitionId,
  HashMap,
  ParameterIndexId,
  ParameterIndexLookupCreator,
  ParameterLookupDefinitionId,
  SerializedBucketDataSourceWithDataSources,
  SerializedParameterIndexLookupCreator,
  serializedStreamBucketDataSourceEquality,
  serializedStreamParameterIndexLookupCreatorEquality,
  SerializedSyncPlanV1,
  SourceTableRef,
  SyncConfigWithErrors
} from '@powersync/service-sync-rules';
import { SyncConfigDefinition } from '../storage-index.js';

export interface SerializedSyncConfigWithMapping {
  plan: SerializedSyncPlanV1;
  mapping: SingleSyncConfigBucketDefinitionMapping;
}

export interface SyncConfigWithMapping {
  syncConfigId?: string;
  syncConfig: SyncConfigWithErrors;
  mapping: SingleSyncConfigBucketDefinitionMapping | null;
}

export interface SyncConfigWithRequiredMapping {
  syncConfigId: string;
  syncConfig: SyncConfigWithErrors;
  mapping: SingleSyncConfigBucketDefinitionMapping;
}

/**
 * Represents a mapping from bucket data sources and parameter lookup sources to stable IDs used for bucket definition and parameter index persistence.
 *
 * Usage is deliberately restricted: writer/batch paths resolve through their own parsed
 * sync config set's mapping, and read paths resolve through the persisted
 * {@link SingleSyncConfigBucketDefinitionMapping} of the single sync config they serve.
 * Do not construct ad-hoc resolution paths from parsed sources.
 */
export interface BucketDefinitionMapping {
  /**
   * Given a BucketDataSource within the associated SyncConfig(s), return the BucketDefinitionId, or throw if not found.
   */
  bucketSourceId(source: BucketDataSource): BucketDefinitionId;

  parameterLookupId(source: ParameterIndexLookupCreator): ParameterIndexId;

  allBucketDefinitionIds(): BucketDefinitionId[];

  allParameterIndexIds(): ParameterIndexId[];

  syncConfigIdsForSourceTable(
    selectedSyncConfigIds: string[],
    table: SourceTableRef,
    bucketDataSourceIds: BucketDefinitionId[],
    parameterLookupSourceIds: ParameterIndexId[]
  ): string[];

  snapshotBlockingSourceTablesFilter(syncConfigId: string): Record<string, unknown>;
}

/**
 * A BucketDefinitionMapping associated with a single SyncConfig.
 *
 * Within a single SyncConfig, unique names are the persistence key for bucket definitions,
 * so lookups here are name-based by design. MongoHydrationState and
 * MultiSyncConfigBucketDefinitionMapping handle the mapping across multiple SyncConfigs in
 * the same replication stream, where names are not unique.
 */
export class SingleSyncConfigBucketDefinitionMapping implements BucketDefinitionMapping {
  static fromSyncConfig(doc: Pick<SyncConfigDefinition, 'rule_mapping'>): SingleSyncConfigBucketDefinitionMapping {
    return new SingleSyncConfigBucketDefinitionMapping(
      doc.rule_mapping?.definitions ?? {},
      doc.rule_mapping?.parameter_indexes ?? {}
    );
  }

  static fromParsedSyncConfig(syncConfig: SyncConfigWithErrors): SingleSyncConfigBucketDefinitionMapping {
    const definitionNames = syncConfig.config.bucketDataSources.map((source) => source.uniqueName).sort();
    const parameterKeys = syncConfig.config.bucketParameterLookupSources
      .map((source) => `${source.sourceId.lookupName}#${source.sourceId.queryId}`)
      .sort();

    const definitions: Record<string, BucketDefinitionId> = {};
    const parameterLookups: Record<string, ParameterIndexId> = {};

    for (const [index, uniqueName] of definitionNames.entries()) {
      definitions[uniqueName] = (index + 1).toString(16);
    }
    for (const [index, key] of parameterKeys.entries()) {
      parameterLookups[key] = (index + 1).toString(16);
    }

    return new SingleSyncConfigBucketDefinitionMapping(definitions, parameterLookups);
  }

  /**
   *
   * @param compatibleConfigs SyncConfigs with definitions that may be re-used
   * @param newPlan
   * @param reservedMappings _all_ mappings used currently and historically for the replication stream; used to construct new ids
   * @returns
   */
  static constructIncrementalMappingFromSerializedPlans(
    compatibleConfigs: SerializedSyncConfigWithMapping[],
    newPlan: SerializedSyncPlanV1,
    reservedMappings: SingleSyncConfigBucketDefinitionMapping[]
  ): SingleSyncConfigBucketDefinitionMapping {
    let nextBucketDefinitionId =
      reservedMappings
        .map((mapping) => mapping.allBucketDefinitionIds())
        .flat()
        .reduce((maxId, id) => Math.max(maxId, parseInt(id, 16)), 0) + 1;
    function generateNewBucketDefinitionId(): BucketDefinitionId {
      const id = nextBucketDefinitionId.toString(16);
      nextBucketDefinitionId++;
      return id;
    }
    let nextParameterIndexId =
      reservedMappings
        .map((mapping) => mapping.allParameterIndexIds())
        .flat()
        .reduce((maxId, id) => Math.max(maxId, parseInt(id, 16)), 0) + 1;
    function generateNewParameterIndexId(): ParameterIndexId {
      const id = nextParameterIndexId.toString(16);
      nextParameterIndexId++;
      return id;
    }

    const definitions: Record<string, BucketDefinitionId> = {};
    const parameterLookups: Record<string, ParameterIndexId> = {};
    const compatibleBuckets = new HashMap<SerializedBucketDataSourceWithDataSources, BucketDefinitionId>(
      serializedStreamBucketDataSourceEquality
    );
    const compatibleParameterLookups = new HashMap<SerializedParameterIndexLookupCreator, ParameterIndexId>(
      serializedStreamParameterIndexLookupCreatorEquality
    );

    for (const config of compatibleConfigs) {
      for (const bucket of config.plan.buckets) {
        compatibleBuckets.putIfAbsent({ bucket, dataSources: config.plan.dataSources }, () =>
          config.mapping.bucketSourceIdByName(bucket.uniqueName)
        );
      }

      for (const parameterLookup of config.plan.parameterIndexes) {
        compatibleParameterLookups.putIfAbsent(parameterLookup, () =>
          config.mapping.parameterLookupIdByKey(parameterLookupKey(parameterLookup.lookupScope))
        );
      }
    }

    for (const bucket of newPlan.buckets) {
      const compatibleId = compatibleBuckets.get({ bucket, dataSources: newPlan.dataSources });
      const id = compatibleId ?? generateNewBucketDefinitionId();
      definitions[bucket.uniqueName] = id;
    }

    for (const parameterLookup of newPlan.parameterIndexes) {
      const compatibleId = compatibleParameterLookups.get(parameterLookup);
      const id = compatibleId ?? generateNewParameterIndexId();
      parameterLookups[parameterLookupKey(parameterLookup.lookupScope)] = id;
    }

    return new SingleSyncConfigBucketDefinitionMapping(definitions, parameterLookups);
  }

  constructor(
    private definitions: Record<string, BucketDefinitionId> = {},
    private parameterLookupMapping: Record<string, ParameterIndexId> = {}
  ) {}

  bucketSourceId(source: BucketDataSource): BucketDefinitionId {
    return this.bucketSourceIdByName(source.uniqueName);
  }

  bucketSourceIdByName(uniqueName: string): BucketDefinitionId {
    const defId = this.definitions[uniqueName];
    if (defId == null) {
      throw new ServiceAssertionError(`No mapping found for bucket source ${uniqueName}`);
    }
    return defId;
  }

  allBucketDefinitionIds(): BucketDefinitionId[] {
    return Object.values(this.definitions);
  }

  allParameterIndexIds(): ParameterIndexId[] {
    return Object.values(this.parameterLookupMapping);
  }

  parameterLookupId(source: ParameterIndexLookupCreator): ParameterIndexId {
    return this.parameterLookupIdByKey(parameterLookupKey(source.sourceId));
  }

  parameterLookupIdByKey(key: string): ParameterIndexId {
    const defId = this.parameterLookupMapping[key];
    if (defId == null) {
      throw new ServiceAssertionError(`No mapping found for parameter lookup source ${key}`);
    }
    return defId;
  }

  serialize(): SyncConfigDefinition['rule_mapping'] {
    return {
      definitions: { ...this.definitions },
      parameter_indexes: { ...this.parameterLookupMapping }
    };
  }

  syncConfigIdsForSourceTable(
    selectedSyncConfigIds: string[],
    _table: SourceTableRef,
    bucketDataSourceIds: BucketDefinitionId[],
    parameterLookupSourceIds: ParameterIndexId[]
  ): string[] {
    return bucketDataSourceIds.length > 0 || parameterLookupSourceIds.length > 0 ? selectedSyncConfigIds : [];
  }

  snapshotBlockingSourceTablesFilter(_syncConfigId: string): Record<string, unknown> {
    const clauses: Record<string, unknown>[] = [];
    const bucketDataSourceIds = this.allBucketDefinitionIds();
    if (bucketDataSourceIds.length > 0) {
      clauses.push({ bucket_data_source_ids: { $in: bucketDataSourceIds } });
    }
    const parameterLookupSourceIds = this.allParameterIndexIds();
    if (parameterLookupSourceIds.length > 0) {
      clauses.push({ parameter_lookup_source_ids: { $in: parameterLookupSourceIds } });
    }
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
}

/**
 * A BucketDefinitionMapping across all SyncConfigs of one parse of a replication stream.
 *
 * Source lookups are strictly identity-based: a source that did not come from the parse
 * this mapping was constructed with is an error. Unique names are not unique across sync
 * configs in a replication stream, so there is deliberately no name-based fallback.
 */
export class MultiSyncConfigBucketDefinitionMapping implements BucketDefinitionMapping {
  private bucketDataSourceMappings = new WeakMap<BucketDataSource, SingleSyncConfigBucketDefinitionMapping>();
  private bucketDataSourceSyncConfigIdsById = new Map<BucketDefinitionId, Set<string>>();
  private parameterLookupMappings = new WeakMap<ParameterIndexLookupCreator, SingleSyncConfigBucketDefinitionMapping>();
  private parameterLookupSyncConfigIdsById = new Map<ParameterIndexId, Set<string>>();
  private syncConfigsById = new Map<string, SyncConfigWithRequiredMapping>();
  private mappings: SingleSyncConfigBucketDefinitionMapping[];

  constructor(syncConfigs: SyncConfigWithRequiredMapping[]) {
    this.mappings = syncConfigs.map((config) => config.mapping);

    for (const config of syncConfigs) {
      this.syncConfigsById.set(config.syncConfigId, config);
      for (const source of config.syncConfig.config.bucketDataSources) {
        this.bucketDataSourceMappings.set(source, config.mapping);
        addSetEntry(this.bucketDataSourceSyncConfigIdsById, config.mapping.bucketSourceId(source), config.syncConfigId);
      }
      for (const source of config.syncConfig.config.bucketParameterLookupSources) {
        this.parameterLookupMappings.set(source, config.mapping);
        addSetEntry(
          this.parameterLookupSyncConfigIdsById,
          config.mapping.parameterLookupId(source),
          config.syncConfigId
        );
      }
    }
  }

  bucketSourceId(source: BucketDataSource): BucketDefinitionId {
    const mapping = this.bucketDataSourceMappings.get(source);
    if (mapping == null) {
      throw new ServiceAssertionError(
        `Bucket source ${source.uniqueName} is not associated with this parse of the sync configs`
      );
    }
    return mapping.bucketSourceId(source);
  }

  allBucketDefinitionIds(): BucketDefinitionId[] {
    return [...new Set(this.mappings.flatMap((mapping) => mapping.allBucketDefinitionIds()))];
  }

  parameterLookupId(source: ParameterIndexLookupCreator): ParameterIndexId {
    const mapping = this.parameterLookupMappings.get(source);
    if (mapping == null) {
      throw new ServiceAssertionError(
        `Parameter lookup source ${source.sourceId.lookupName}#${source.sourceId.queryId} is not associated with this parse of the sync configs`
      );
    }
    return mapping.parameterLookupId(source);
  }

  allParameterIndexIds(): ParameterIndexId[] {
    return [...new Set(this.mappings.flatMap((mapping) => mapping.allParameterIndexIds()))];
  }

  syncConfigIdsForSourceTable(
    _selectedSyncConfigIds: string[],
    table: SourceTableRef,
    bucketDataSourceIds: BucketDefinitionId[],
    parameterLookupSourceIds: ParameterIndexId[]
  ): string[] {
    const ids = new Set<string>();
    for (const sourceId of bucketDataSourceIds) {
      addAll(ids, this.bucketDataSourceSyncConfigIdsById.get(sourceId));
    }
    for (const sourceId of parameterLookupSourceIds) {
      addAll(ids, this.parameterLookupSyncConfigIdsById.get(sourceId));
    }
    for (const [syncConfigId, config] of this.syncConfigsById) {
      if (config.syncConfig.config.tableTriggersEvent(table)) {
        ids.add(syncConfigId);
      }
    }
    return [...ids];
  }

  snapshotBlockingSourceTablesFilter(syncConfigId: string): Record<string, unknown> {
    const config = this.syncConfigsById.get(syncConfigId);
    if (config == null) {
      throw new ServiceAssertionError(`No mapping found for sync config ${syncConfigId}`);
    }

    return config.mapping.snapshotBlockingSourceTablesFilter(syncConfigId);
  }
}

export function parameterLookupKey(id: ParameterLookupDefinitionId) {
  return `${id.lookupName}#${id.queryId}`;
}

function addSetEntry<K, V>(map: Map<K, Set<V>>, key: K, value: V) {
  const existing = map.get(key) ?? new Set<V>();
  existing.add(value);
  map.set(key, existing);
}

function addAll<T>(target: Set<T>, values: Iterable<T> | undefined) {
  for (const value of values ?? []) {
    target.add(value);
  }
}
