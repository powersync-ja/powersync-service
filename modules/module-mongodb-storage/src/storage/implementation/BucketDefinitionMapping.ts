import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDataSource, ParameterIndexLookupCreator, SyncConfigWithErrors } from '@powersync/service-sync-rules';
import { SyncConfigDefinition } from '../storage-index.js';

export type BucketDefinitionId = string;
export type ParameterIndexId = string;

export class BucketDefinitionMapping {
  static fromSyncConfig(doc: Pick<SyncConfigDefinition, 'rule_mapping'>): BucketDefinitionMapping {
    return new BucketDefinitionMapping(doc.rule_mapping?.definitions ?? {}, doc.rule_mapping?.parameter_indexes ?? {});
  }

  static fromParsedSyncRules(syncRules: SyncConfigWithErrors): BucketDefinitionMapping {
    const definitionNames = syncRules.config.bucketDataSources.map((source) => source.uniqueName).sort();
    const parameterKeys = syncRules.config.bucketParameterLookupSources
      .map((source) => `${source.defaultLookupScope.lookupName}#${source.defaultLookupScope.queryId}`)
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
    const key = this.parameterLookupKey(source.defaultLookupScope.lookupName, source.defaultLookupScope.queryId);
    const defId = this.parameterLookupMapping[key];
    if (defId == null) {
      throw new ServiceAssertionError(`No mapping found for parameter lookup source ${key}`);
    }
    return defId;
  }

  private parameterLookupKey(lookupName: string, queryId: string) {
    return `${lookupName}#${queryId}`;
  }

  serialize(): SyncConfigDefinition['rule_mapping'] {
    return {
      definitions: { ...this.definitions },
      parameter_indexes: { ...this.parameterLookupMapping }
    };
  }
}
