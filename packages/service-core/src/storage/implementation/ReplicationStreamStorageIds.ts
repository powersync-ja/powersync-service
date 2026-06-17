import { BucketDefinitionId, ParameterIndexId } from '@powersync/service-sync-rules';
import { SingleSyncConfigBucketDefinitionMapping } from './BucketDefinitionMapping.js';

/**
 * Persisted storage ids for a replication stream, derived from the rule_mapping documents
 * of all its sync configs.
 *
 * This requires no parsed sync configs, and deliberately does not resolve individual
 * sources - use the parsed-set-bound {@link BucketDefinitionMapping} for that.
 */
export class ReplicationStreamStorageIds {
  constructor(private readonly mappings: SingleSyncConfigBucketDefinitionMapping[]) {}

  get bucketDefinitionIds(): BucketDefinitionId[] {
    return [...new Set(this.mappings.flatMap((mapping) => mapping.allBucketDefinitionIds()))];
  }

  get parameterIndexIds(): ParameterIndexId[] {
    return [...new Set(this.mappings.flatMap((mapping) => mapping.allParameterIndexIds()))];
  }
}
