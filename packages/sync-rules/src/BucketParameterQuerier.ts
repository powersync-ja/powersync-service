import { JSONBig } from '@powersync/service-jsonbig';
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
  getParameterSets: (lookups: ScopedParameterLookup[]) => Promise<SqliteJsonRow[]>;
}

export interface QueryBucketDescriptorOptions extends ParameterLookupSource {
  parameters: RequestParameters;
}

export function mergeBucketParameterQueriers(queriers: BucketParameterQuerier[]): BucketParameterQuerier {
  return {
    staticBuckets: queriers.flatMap((q) => q.staticBuckets),
    hasDynamicBuckets: queriers.findIndex((q) => q.hasDynamicBuckets) != -1,
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
export class ScopedParameterLookup {
  // bucket definition name, parameter query index, ...lookup values
  readonly values: readonly SqliteJsonValue[];

  #cachedSerializedForm?: string;

  /**
   * {@link values} of this lookup encoded via {@link JSONBig}.
   *
   * The result of this getter is cached to avoid re-computing the JSON value for lookups that get reused.
   */
  get serializedRepresentation(): string {
    return (this.#cachedSerializedForm ??= JSONBig.stringify(this.values));
  }

  static normalized(scope: ParameterLookupScope, lookup: UnscopedParameterLookup): ScopedParameterLookup {
    return new ScopedParameterLookup([scope.lookupName, scope.queryId, ...lookup.lookupValues]);
  }

  /**
   * Primarily for test fixtures.
   */
  static direct(scope: ParameterLookupScope, values: SqliteJsonValue[]): ScopedParameterLookup {
    return new ScopedParameterLookup([scope.lookupName, scope.queryId, ...values.map(normalizeParameterValue)]);
  }

  /**
   *
   * @param values must be pre-normalized (any integer converted into bigint)
   */
  private constructor(values: SqliteJsonValue[]) {
    this.values = Object.freeze(values);
  }
}

export class UnscopedParameterLookup {
  readonly lookupValues: SqliteJsonValue[];

  static normalized(values: SqliteJsonValue[]): UnscopedParameterLookup {
    return new UnscopedParameterLookup(values.map(normalizeParameterValue));
  }

  constructor(lookupValues: SqliteJsonValue[]) {
    this.lookupValues = lookupValues;
  }
}
