import { BucketInclusionReason, ResolvedBucket } from '../BucketDescription.js';
import { BucketParameterQuerier, ParameterLookup } from '../BucketParameterQuerier.js';
import {
  BucketParameterLookupSource,
  BucketParameterLookupSourceDefinition,
  CreateSourceParams
} from '../BucketSource.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { TablePattern } from '../TablePattern.js';
import {
  BucketIdTransformer,
  EvaluatedParametersResult,
  EvaluateRowOptions,
  RequestParameters,
  SqliteJsonValue,
  SqliteRow,
  TableRow
} from '../types.js';
import { isJsonValue, JSONBucketNameSerialize, normalizeParameterValue } from '../utils.js';
import { BucketParameter, SubqueryEvaluator } from './parameter.js';
import { SyncStream } from './stream.js';
import { cartesianProduct } from './utils.js';

/**
 * A variant of a stream.
 *
 * Variants are introduced on {@link Or} filters, since different sub-filters (with potentially different) bucket
 * parameters can both cause a row to be matched.
 *
 * Consider the query `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user()) OR request.is_admin()`.
 * Here, the filter is an or clause matching rows where:
 *
 *   - An {@link InOperator} associatates comments in issues owned by the requesting user. This gets implemented with a
 *     parameter lookup index mapping `issue.owner_id => issue.id`. `comments.issue_id` is a bucket parameter resolved
 *     dynamically.
 *   - Or, the user is an admin, in which case all comments are matched. There are no bucket parameters for this
 *     variant.
 *
 * The introduction of stream variants allows the `evaluateParameterRow` and `queriersForSubscription` implementations
 * to operate independently.
 *
 * Multiple variants may cause the same row to get synced via different buckets. Depending on the request, users may
 * also receive multiple buckets with the same data. This is not an issue! Clients deduplicate rows received across
 * buckets, so we don't have to filter for this case in the sync service.
 */
export class StreamVariant {
  id: number;
  parameters: BucketParameter[];
  subqueries: SubqueryEvaluator[];

  /**
   * Additional filters that don't introduce bucket parameters, but can exclude rows.
   *
   * This is introduced for streams like `SELECT * FROM assets WHERE LENGTH(assets.name < 10)`.
   */
  additionalRowFilters: ((row: TableRow) => boolean)[];

  /**
   * Additional filters that are evaluated against the request of the stream subscription.
   *
   * These filters can either only depend on values in the request alone (e.g. `WHERE token_parameters.is_admin`), or
   * on results from a subquery (e.g. `WHERE request.user_id() IN (SELECT id FROM user WHERE is_admin)`).
   */
  requestFilters: RequestFilter[];

  constructor(id: number) {
    this.id = id;
    this.parameters = [];
    this.subqueries = [];
    this.additionalRowFilters = [];
    this.requestFilters = [];
  }

  defaultBucketPrefix(streamName: string): string {
    return `${streamName}|${this.id}`;
  }

  lookupSources(streamName: string): BucketParameterLookupSourceDefinition[] {
    return this.subqueries.flatMap((subquery) => subquery.lookupSources(streamName));
  }

  /**
   * Given a row in the table this stream selects from, returns all ids of buckets to which that row belongs to.
   */
  bucketIdsForRow(streamName: string, options: TableRow, transformer: BucketIdTransformer): string[] {
    return this.instantiationsForRow(options).map((values) => this.buildBucketId(streamName, values, transformer));
  }

  /**
   * Given a row to evaluate, returns all instantiations of parameters that satisfy conditions.
   *
   * The inner arrays will have a length equal to the amount of parameters in this variant.
   */
  instantiationsForRow(options: TableRow): SqliteJsonValue[][] {
    for (const additional of this.additionalRowFilters) {
      if (!additional(options)) {
        return [];
      }
    }

    // Contains an array of all values satisfying each parameter. So this array has the same length as the amount of
    // parameters, and each nested array has a dynamic length.
    const parameterInstantiations: SqliteJsonValue[][] = [];
    for (const parameter of this.parameters) {
      const matching = parameter.filterRow(options);
      if (matching.length == 0) {
        // The final list of bucket ids is the cartesian product of all matching parameters. So if there's no parameter
        // satisfying this value, we know the final list will be empty.
        return [];
      }

      parameterInstantiations.push(matching);
    }

    // Combine the map of values like {param_1: [foo, bar], param_2: [baz]} into parameter arrays:
    // [foo, baz], [bar, baz].
    return this.cartesianProductOfParameterInstantiations(parameterInstantiations);
  }

  /**
   * Turns an array of values for each parameter into an array of all instantiations by effectively building the
   * cartesian product of the parameter sets.
   *
   * @param instantiations An array containing values for each parameter.
   * @returns Each instantiation, with each sub-array having a value for a parameter.
   */
  private cartesianProductOfParameterInstantiations(instantiations: SqliteJsonValue[][]): SqliteJsonValue[][] {
    return [...cartesianProduct(...instantiations)];
  }

  querier(
    stream: SyncStream,
    reason: BucketInclusionReason,
    params: RequestParameters,
    bucketIdTransformer: BucketIdTransformer
  ): BucketParameterQuerier | null {
    const instantiation = this.partiallyEvaluateParameters(params);
    if (instantiation == null) {
      return null;
    }

    interface ResolvedDynamicParameter {
      index: number;
      subquery: SubqueryEvaluator;
    }

    const dynamicRequestFilters: SubqueryRequestFilter[] = this.requestFilters.filter((f) => f.type == 'dynamic');
    const dynamicParameters: ResolvedDynamicParameter[] = [];
    const subqueryToLookups = new Map<SubqueryEvaluator, ParameterLookup[]>();

    for (let i = 0; i < this.parameters.length; i++) {
      const parameter = this.parameters[i];
      const lookup = parameter.lookup;

      if (lookup.type == 'in' || lookup.type == 'overlap') {
        dynamicParameters.push({
          index: i,
          subquery: lookup.subquery
        });
      }
    }

    for (const subquery of this.subqueries) {
      subqueryToLookups.set(subquery, subquery.lookupsForRequest(params));
    }

    const staticBuckets: ResolvedBucket[] = [];
    if (dynamicParameters.length == 0 && dynamicRequestFilters.length == 0) {
      // When we have no dynamic parameters, the partial evaluation is a full instantiation.
      const instantiations = this.cartesianProductOfParameterInstantiations(instantiation as SqliteJsonValue[][]);
      for (const instantiation of instantiations) {
        staticBuckets.push(this.resolveBucket(stream, instantiation, reason, bucketIdTransformer));
      }
    }

    const variant = this;
    return {
      staticBuckets: staticBuckets,
      hasDynamicBuckets: this.subqueries.length != 0,
      parameterQueryLookups: [...subqueryToLookups.values()].flatMap((f) => f),
      async queryDynamicBucketDescriptions(source) {
        // Evaluate subqueries
        const subqueryResults = new Map<SubqueryEvaluator, SqliteJsonValue[]>();
        for (const [subquery, lookups] of subqueryToLookups.entries()) {
          const rows = await source.getParameterSets(lookups);
          // The result column used in parameter sets is always named result, see pushParameterRowEvaluation
          const values = rows.map((r) => r.result);
          subqueryResults.set(subquery, values);
        }

        // Check if we have a subquery-based request filter rejecting the row.
        for (const filter of dynamicRequestFilters) {
          if (!filter.matches(params, subqueryResults.get(filter.subquery)!)) {
            return [];
          }
        }

        const perParameterInstantiation: (SqliteJsonValue | BucketParameter)[][] = [];
        for (const parameter of instantiation) {
          if (Array.isArray(parameter)) {
            // Statically-resolved values
            perParameterInstantiation.push(parameter);
          } else {
            // to be instantiated with dynamic lookup
            perParameterInstantiation.push([parameter as BucketParameter]);
          }
        }

        for (const lookup of dynamicParameters) {
          perParameterInstantiation[lookup.index] = subqueryResults.get(lookup.subquery)!;
        }

        const product = variant.cartesianProductOfParameterInstantiations(
          perParameterInstantiation as SqliteJsonValue[][]
        );

        return Promise.resolve(product.map((e) => variant.resolveBucket(stream, e, reason, bucketIdTransformer)));
      }
    };
  }

  findStaticInstantiations(params: RequestParameters): SqliteJsonValue[][] {
    if (this.subqueries.length) {
      return [];
    }

    return this.cartesianProductOfParameterInstantiations(
      // This will be an array of values (i.e. a total evaluation) because there are no dynamic parameters.
      this.partiallyEvaluateParameters(params) as SqliteJsonValue[][]
    );
  }

  debugRepresentation(): any {
    return {
      id: this.id,
      parameters: this.parameters.map((p) => ({
        type: p.lookup.type
      })),
      subqueries: this.subqueries.map((s) => ({
        table: s.parameterTable
      })),
      additional_row_filters: this.additionalRowFilters.length,
      request_filters: this.requestFilters.map((f) => f.type)
    };
  }

  /**
   * Replaces {@link StreamVariant.parameters} with static values looked up in request parameters.
   *
   * Dynamic parameters that depend on subquery results are not replaced.
   * This returns null if there's a {@link StaticRequestFilter} that doesn't match the request.
   */
  private partiallyEvaluateParameters(params: RequestParameters): (SqliteJsonValue[] | BucketParameter)[] | null {
    for (const filter of this.requestFilters) {
      if (filter.type == 'static' && !filter.matches(params)) {
        return null;
      }
    }

    const instantiation: (SqliteJsonValue[] | BucketParameter)[] = [];
    for (const parameter of this.parameters) {
      const lookup = parameter.lookup;
      if (lookup.type == 'static') {
        const values = lookup.fromRequest(params)?.filter(isJsonValue);
        if (values.length == 0) {
          // Parameter not instantiable for this request. Since parameters in a single variant form a conjunction, that
          // means the whole request won't find anything here.
          return null;
        }

        instantiation.push(values);
      } else {
        instantiation.push(parameter);
      }
    }

    return instantiation;
  }

  /**
   * Builds a bucket id for an instantiation, like `stream|0[1,2,"foo"]`.
   *
   * @param streamName The name of the stream, included in the bucket id
   * @param instantiation An instantiation for all parameters in this variant.
   * @param transformer A transformer adding version information to the inner id.
   * @returns The generated bucket id
   */
  private buildBucketId(streamName: string, instantiation: SqliteJsonValue[], transformer: BucketIdTransformer) {
    if (instantiation.length != this.parameters.length) {
      throw Error('Internal error, instantiation length mismatch');
    }

    return transformer(`${streamName}|${this.id}${JSONBucketNameSerialize.stringify(instantiation)}`);
  }

  private resolveBucket(
    stream: SyncStream,
    instantiation: SqliteJsonValue[],
    reason: BucketInclusionReason,
    bucketIdTransformer: BucketIdTransformer
  ): ResolvedBucket {
    return {
      definition: stream.name,
      inclusion_reasons: [reason],
      bucket: this.buildBucketId(stream.name, instantiation, bucketIdTransformer),
      priority: stream.priority
    };
  }
}
/**
 * A stateless filter condition that only depends on the request itself, e.g. `WHERE token_parameters.is_admin`.
 */
export interface StaticRequestFilter {
  type: 'static';
  matches(params: RequestParameters): boolean;
}

/**
 * A filter condition that depends on parameters and an evaluated subquery, e.g.
 * `WHERE request.user_id() IN (SELECT id FROM users WHERE ...)`.
 */
export interface SubqueryRequestFilter {
  type: 'dynamic';
  subquery: SubqueryEvaluator;

  /**
   * Checks whether the parameter matches values from the subquery.
   *
   * @param results The values that the subquery evaluates to.
   */
  matches(params: RequestParameters, results: SqliteJsonValue[]): boolean;
}
export type RequestFilter = StaticRequestFilter | SubqueryRequestFilter;
