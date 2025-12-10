import { ResolvedBucket } from './BucketDescription.js';
import { ParameterLookupScope } from './HydrationState.js';
import { RequestedStream } from './SqlSyncRules.js';
import { RequestParameters, SqliteJsonRow, SqliteJsonValue } from './types.js';
import { normalizeParameterValue } from './utils.js';

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
  readonly staticBuckets: ResolvedBucket[];

  /**
   * True if there are dynamic buckets, meaning queryDynamicBucketDescriptions() should be used.
   *
   * If this is false, queryDynamicBucketDescriptions() will always return an empty array,
   * and parameterQueryLookups.length == 0.
   */
  readonly hasDynamicBuckets: boolean;

  readonly parameterQueryLookups: ParameterLookup[];

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
  queryDynamicBucketDescriptions(source: ParameterLookupSource): Promise<ResolvedBucket[]>;
}

/**
 * An error that occurred while trying to resolve the bucket ids a request should have access to.
 *
 * A common scenario that could cause this to happen is when parameters need to have a certain structure. For instance,
 * `... WHERE id IN stream.parameters -> 'ids'` is unresolvable when `ids` is not set to a JSON array.
 */
export interface QuerierError {
  descriptor: string;
  subscription?: RequestedStream;
  message: string;
}

export interface PendingQueriers {
  queriers: BucketParameterQuerier[];
  errors: QuerierError[];
}

export interface ParameterLookupSource {
  getParameterSets: (lookups: ParameterLookup[]) => Promise<SqliteJsonRow[]>;
}

export interface QueryBucketDescriptorOptions extends ParameterLookupSource {
  parameters: RequestParameters;
}

export function mergeBucketParameterQueriers(queriers: BucketParameterQuerier[]): BucketParameterQuerier {
  const parameterQueryLookups = queriers.flatMap((q) => q.parameterQueryLookups);
  return {
    staticBuckets: queriers.flatMap((q) => q.staticBuckets),
    hasDynamicBuckets: parameterQueryLookups.length > 0,
    parameterQueryLookups: parameterQueryLookups,
    async queryDynamicBucketDescriptions(source: ParameterLookupSource) {
      let results: ResolvedBucket[] = [];
      for (let q of queriers) {
        if (q.hasDynamicBuckets) {
          results.push(...(await q.queryDynamicBucketDescriptions(source)));
        }
      }
      return results;
    }
  };
}

/**
 * Represents an equality filter from a parameter query.
 *
 * Other query types are not supported yet.
 */
export class ParameterLookup {
  // bucket definition name, parameter query index, ...lookup values
  readonly values: SqliteJsonValue[];

  static normalized(scope: ParameterLookupScope, values: SqliteJsonValue[]): ParameterLookup {
    return new ParameterLookup([scope.lookupName, scope.queryId, ...values.map(normalizeParameterValue)]);
  }

  /**
   *
   * @param values must be pre-normalized (any integer converted into bigint)
   */
  private constructor(values: SqliteJsonValue[]) {
    this.values = values;
  }
}
