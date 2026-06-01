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
  SyncConfigWithErrors
} from '@powersync/service-sync-rules';
import { SyncConfigDefinition } from '../storage-index.js';

export interface SyncConfigWithMapping {
  syncConfig: SyncConfigWithErrors;
  mapping: BucketDefinitionMapping | null;
}

export interface SyncConfigWithRequiredMapping {
  syncConfig: SyncConfigWithErrors;
  mapping: BucketDefinitionMapping;
}

export interface SerializedSyncConfigWithMapping {
  plan: SerializedSyncPlanV1;
  mapping: BucketDefinitionMapping;
}

/**
 * Represents a mapping from bucket data sources and parameter lookup sources to stable IDs used for bucket definition and parameter index persistence.
 *
 * An instance of BucketDefinitionMapping is associated with a specific SyncConfig.
 * MongoHydrationState handles the mapping across multiple SyncConfigs in the same replication stream.
 */
export class BucketDefinitionMapping {
  static fromSyncConfig(doc: Pick<SyncConfigDefinition, 'rule_mapping'>): BucketDefinitionMapping {
    return new BucketDefinitionMapping(doc.rule_mapping?.definitions ?? {}, doc.rule_mapping?.parameter_indexes ?? {});
  }

  static fromParsedSyncConfig(syncConfig: SyncConfigWithErrors): BucketDefinitionMapping {
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

    return new BucketDefinitionMapping(definitions, parameterLookups);
  }

  static constructIncrementalMappingFromSerializedPlans(
    existing: SerializedSyncConfigWithMapping[],
    newPlan: SerializedSyncPlanV1
  ): BucketDefinitionMapping {
    // FIXME: These ids may conflict with existing mappings if sync configs are de-activated.
    let nextBucketDefinitionId =
      existing
        .map((c) => c.mapping.allBucketDefinitionIds())
        .flat()
        .reduce((maxId, id) => Math.max(maxId, parseInt(id, 16)), 0) + 1;
    function generateNewBucketDefinitionId(): BucketDefinitionId {
      const id = nextBucketDefinitionId.toString(16);
      nextBucketDefinitionId++;
      return id;
    }
    let nextParameterIndexId =
      existing
        .map((c) => c.mapping.allParameterIndexIds())
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

    for (const config of existing) {
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

    return new BucketDefinitionMapping(definitions, parameterLookups);
  }

  constructor(
    private definitions: Record<string, BucketDefinitionId> = {},
    private parameterLookupMapping: Record<string, ParameterIndexId> = {}
  ) {}

  /**
   * Given a BucketDataSource within this SyncConfig, return the BucketDefinitionId, or throw if not found.
   *
   * The behavior is undefined if the source is associated with a different SyncConfig.
   */
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
}

export function parameterLookupKey(id: ParameterLookupDefinitionId) {
  return `${id.lookupName}#${id.queryId}`;
}
