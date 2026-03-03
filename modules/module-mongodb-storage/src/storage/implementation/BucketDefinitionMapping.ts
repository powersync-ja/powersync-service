import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDataSource, ParameterIndexLookupCreator, SyncConfigWithErrors } from '@powersync/service-sync-rules';
import { SyncRuleDocument } from './models.js';

export class BucketDefinitionMapping {
  static fromSyncRules(doc: Pick<SyncRuleDocument, 'rule_mapping'>): BucketDefinitionMapping {
    return new BucketDefinitionMapping(doc.rule_mapping?.definitions ?? {}, doc.rule_mapping?.parameter_lookups ?? {});
  }

  static fromParsedSyncRules(syncRules: SyncConfigWithErrors): BucketDefinitionMapping {
    const definitionNames = syncRules.config.bucketDataSources.map((source) => source.uniqueName).sort();
    const parameterKeys = syncRules.config.bucketParameterLookupSources
      .map((source) => `${source.defaultLookupScope.lookupName}#${source.defaultLookupScope.queryId}`)
      .sort();

    const definitions: Record<string, number> = {};
    const parameterLookups: Record<string, number> = {};

    for (const [index, uniqueName] of definitionNames.entries()) {
      definitions[uniqueName] = index + 1;
    }
    for (const [index, key] of parameterKeys.entries()) {
      parameterLookups[key] = index + 1;
    }

    return new BucketDefinitionMapping(definitions, parameterLookups);
  }

  constructor(
    private definitions: Record<string, number> = {},
    private parameterLookupMapping: Record<string, number> = {}
  ) {}

  bucketSourceId(source: BucketDataSource): number {
    const defId = this.definitions[source.uniqueName];
    if (defId == null) {
      throw new ServiceAssertionError(`No mapping found for bucket source ${source.uniqueName}`);
    }
    return defId;
  }

  parameterLookupId(source: ParameterIndexLookupCreator): number {
    const key = `${source.defaultLookupScope.lookupName}#${source.defaultLookupScope.queryId}`;
    const defId = this.parameterLookupMapping[key];
    if (defId == null) {
      throw new ServiceAssertionError(`No mapping found for parameter lookup source ${key}`);
    }
    return defId;
  }

  serialize(): NonNullable<SyncRuleDocument['rule_mapping']> {
    return {
      definitions: { ...this.definitions },
      parameter_lookups: { ...this.parameterLookupMapping }
    };
  }
}
