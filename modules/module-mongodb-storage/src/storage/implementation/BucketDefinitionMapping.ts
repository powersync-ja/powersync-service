import { ServiceAssertionError } from '@powersync/lib-services-framework';
import { BucketDataSource, ParameterIndexLookupCreator } from '@powersync/service-sync-rules';
import { SyncRuleDocument } from './models.js';

export class BucketDefinitionMapping {
  static fromSyncRules(doc: Pick<SyncRuleDocument, 'rule_mapping'>): BucketDefinitionMapping {
    return new BucketDefinitionMapping(doc.rule_mapping.definitions, doc.rule_mapping.parameter_lookups);
  }

  static merged(mappings: BucketDefinitionMapping[]): BucketDefinitionMapping {
    return mappings.reduce((acc, curr) => acc.mergeWith(curr), new BucketDefinitionMapping());
  }

  constructor(
    private definitions: Record<string, number> = {},
    private parameterLookupMapping: Record<string, number> = {}
  ) {}

  hasBucketSourceId(id: number) {
    return Object.values(this.definitions).includes(id);
  }

  hasParameterLookupId(id: number) {
    return Object.values(this.parameterLookupMapping).includes(id);
  }

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

  equivalentBucketSourceId(source: BucketDataSource): number | null {
    // FIXME: Do an actual comparison, instead of just using the unique name
    return this.definitions[source.uniqueName] ?? null;
  }

  equivalentParameterLookupId(source: ParameterIndexLookupCreator): number | null {
    // FIXME: Do an actual comparison, instead of just using the scope
    const key = `${source.defaultLookupScope.lookupName}#${source.defaultLookupScope.queryId}`;
    return this.parameterLookupMapping[key] ?? null;
  }

  mergeWith(other: BucketDefinitionMapping): BucketDefinitionMapping {
    return new BucketDefinitionMapping(
      { ...this.definitions, ...other.definitions },
      { ...this.parameterLookupMapping, ...other.parameterLookupMapping }
    );
  }
}
