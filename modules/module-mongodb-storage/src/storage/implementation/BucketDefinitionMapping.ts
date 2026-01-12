import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDataSource, ParameterIndexLookupCreator } from '@powersync/service-sync-rules';
import { SyncRuleDocument } from './models.js';

export class BucketDefinitionMapping {
  static fromSyncRules(doc: Pick<SyncRuleDocument, 'rule_mapping'>): BucketDefinitionMapping {
    return new BucketDefinitionMapping(doc.rule_mapping.definitions, doc.rule_mapping.parameter_lookups);
  }

  constructor(
    private definitions: Record<string, number>,
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
}
