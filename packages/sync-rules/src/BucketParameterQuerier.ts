import { BucketDescription } from './BucketDescription.js';
import { RequestParameters, SqliteJsonRow, SqliteJsonValue } from './types.js';

/**
 * Represents a set of parameter queries for a specific request.
 */
export interface BucketParameterQuerier {
  /**
   * These buckets do not change for the lifetime of the connection.
   *
   * This includes parameter queries such as:
   *
   *     select request.user_id() as user_id()
   *     select value as project_id from json_each(request.jwt() -> 'project_ids')
   */
  readonly staticBuckets: BucketDescription[];

  /**
   * True if there are dynamic buckets, meaning queryDynamicBucketDescriptions() should be used.
   *
   * If this is false, queryDynamicBucketDescriptions() will always return an empty array,
   * and dynamicBucketDefinitions.size == 0.
   */
  readonly hasDynamicBuckets: boolean;

  readonly dynamicBucketDefinitions: Set<string>;

  /**
   * These buckets depend on parameter storage, and needs to be retrieved dynamically for each checkpoint.
   *
   * The ParameterLookupSource should perform the query for the current checkpoint - that is not passed
   * as a parameter.
   *
   * This includes parameter queries such as:
   *
   *     select id as user_id from users where users.id = request.user_id()
   */
  queryDynamicBucketDescriptions(source: ParameterLookupSource): Promise<BucketDescription[]>;
}

export interface ParameterLookupSource {
  getParameterSets: (lookups: SqliteJsonValue[][]) => Promise<SqliteJsonRow[]>;
}

export interface QueryBucketDescriptorOptions extends ParameterLookupSource {
  parameters: RequestParameters;
}

export function mergeBucketParameterQueriers(queriers: BucketParameterQuerier[]): BucketParameterQuerier {
  const dynamicBucketDefinitions = new Set<string>(queriers.flatMap((q) => [...q.dynamicBucketDefinitions]));
  return {
    staticBuckets: queriers.flatMap((q) => q.staticBuckets),
    hasDynamicBuckets: dynamicBucketDefinitions.size > 0,
    dynamicBucketDefinitions,
    async queryDynamicBucketDescriptions(source: ParameterLookupSource) {
      let results: BucketDescription[] = [];
      for (let q of queriers) {
        if (q.hasDynamicBuckets) {
          results.push(...(await q.queryDynamicBucketDescriptions(source)));
        }
      }
      return results;
    }
  };
}
