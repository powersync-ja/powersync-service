import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  BucketDataSource,
  BucketDefinitionId,
  ParameterIndexId,
  ParameterIndexLookupCreator,
  ParameterLookupDefinitionId,
  SyncConfigWithErrors
} from '@powersync/service-sync-rules';
import { SyncConfigDefinition } from '../storage-index.js';

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

  constructor(
    private definitions: Record<string, BucketDefinitionId> = {},
    private parameterLookupMapping: Record<string, ParameterIndexId> = {}
  ) {}

  mergeSyncConfig(syncConfig: SyncConfigWithErrors): BucketDefinitionMapping {
    // FIXME: Proper implementation here
    const newMapping = BucketDefinitionMapping.fromParsedSyncConfig(syncConfig);
    const mergedDefinitions = { ...this.definitions };
    const mergedParameterLookups = { ...this.parameterLookupMapping };

    for (const [uniqueName, defId] of Object.entries(newMapping.definitions)) {
      if (mergedDefinitions[uniqueName] && mergedDefinitions[uniqueName] !== defId) {
        throw new ServiceAssertionError(
          `Conflict for bucket source ${uniqueName}: ${mergedDefinitions[uniqueName]} vs ${defId}`
        );
      }
      mergedDefinitions[uniqueName] = defId;
    }

    for (const [key, indexId] of Object.entries(newMapping.parameterLookupMapping)) {
      if (mergedParameterLookups[key] && mergedParameterLookups[key] !== indexId) {
        throw new ServiceAssertionError(
          `Conflict for parameter lookup source ${key}: ${mergedParameterLookups[key]} vs ${indexId}`
        );
      }
      mergedParameterLookups[key] = indexId;
    }

    return new BucketDefinitionMapping(mergedDefinitions, mergedParameterLookups);
  }

  bucketSourceId(source: BucketDataSource): BucketDefinitionId {
    const defId = this.definitions[source.uniqueName];
    if (defId == null) {
      throw new ServiceAssertionError(`No mapping found for bucket source ${source.uniqueName}`);
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
    const key = this.parameterLookupKey(source.sourceId);
    const defId = this.parameterLookupMapping[key];
    if (defId == null) {
      throw new ServiceAssertionError(`No mapping found for parameter lookup source ${key}`);
    }
    return defId;
  }

  private parameterLookupKey(id: ParameterLookupDefinitionId) {
    return `${id.lookupName}#${id.queryId}`;
  }

  serialize(): SyncConfigDefinition['rule_mapping'] {
    return {
      definitions: { ...this.definitions },
      parameter_indexes: { ...this.parameterLookupMapping }
    };
  }
}
