import { ServiceAssertionError } from '@powersync/lib-services-framework';
import {
  BucketDataSource,
  BucketDefinitionId,
  ParameterIndexId,
  ParameterIndexLookupCreator,
  SyncConfigWithErrors
} from '@powersync/service-sync-rules';
import { SyncConfigDefinition } from '../storage-index.js';

export type { BucketDefinitionId, ParameterIndexId };

export interface SyncConfigWithMapping {
  syncConfigId?: string;
  syncConfig: SyncConfigWithErrors;
  mapping: BucketDefinitionMapping | null;
}

export interface SyncConfigWithRequiredMapping {
  syncConfigId: string;
  syncConfig: SyncConfigWithErrors;
  mapping: BucketDefinitionMapping;
}

/**
 * Represents a mapping from bucket data sources and parameter lookup sources to stable IDs used for bucket definition and parameter index persistence.
 *
 * An instance of BucketDefinitionMapping is associated with a specific SyncConfig.
 * MongoHydrationState will handle the mapping across multiple SyncConfigs in the same replication stream.
 */
export class BucketDefinitionMapping {
  static fromSyncConfig(doc: Pick<SyncConfigDefinition, 'rule_mapping'>): BucketDefinitionMapping {
    return new BucketDefinitionMapping(doc.rule_mapping?.definitions ?? {}, doc.rule_mapping?.parameter_indexes ?? {});
  }

  static fromParsedSyncRules(syncRules: SyncConfigWithErrors): BucketDefinitionMapping {
    const definitionNames = syncRules.config.bucketDataSources.map((source) => source.uniqueName).sort();
    const parameterKeys = syncRules.config.bucketParameterLookupSources
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
    const key = this.parameterLookupKey(source.sourceId.lookupName, source.sourceId.queryId);
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
