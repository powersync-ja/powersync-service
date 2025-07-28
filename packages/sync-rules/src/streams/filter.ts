import _ from 'lodash';
import { isParameterValueClause, isRowValueClause, SQLITE_TRUE, sqliteBool } from '../sql_support.js';
import { TablePattern } from '../TablePattern.js';
import {
  EvaluatedParameters,
  EvaluatedParametersResult,
  EvaluateRowOptions,
  EvaluationError,
  ParameterMatchClause,
  ParameterValueClause,
  ParameterValueSet,
  RequestParameters,
  RowValueClause,
  SqliteJsonRow,
  SqliteJsonValue,
  SqliteRow,
  SqliteValue
} from '../types.js';
import { isJsonValue, JSONBucketNameSerialize, normalizeParameterValue } from '../utils.js';
import { SqlTools } from '../sql_filters.js';
import { OPERATOR_NOT } from '../sql_functions.js';
import { BucketInclusionReason, ResolvedBucket } from '../BucketDescription.js';
import { BucketParameterQuerier, ParameterLookup } from '../BucketParameterQuerier.js';
import { SyncStream } from './stream.js';
import { SourceTableInterface } from '../SourceTableInterface.js';

export abstract class FilterOperator {
  abstract compile(context: BucketCompilationContext): void;

  compileVariants(streamName: string): StreamVariant[] {
    const initialVariant = new StreamVariant(0);
    const allVariants: StreamVariant[] = [initialVariant];

    this.compile({ streamName, currentVariant: initialVariant, allVariants, queryCounter: 0 });
    return allVariants;
  }
}

/**
 * A source of parameterization, causing data from the source table to be distributed into multiple buckets instead of
 * a single one.
 *
 * Parameters are introduced when the select statement defining the stream has a where clause with elements where:
 *
 *   1. Values in the row to sync are compared against request parameters: {@link CompareRowValueWithStreamParameter}.
 *   2. Values in the row to sync are compared against a subquery: {@link InOperator}.
 */
interface BucketParameter {
  lookup: StaticLookup | DynamicLookup;

  /**
   * Given a row in the table the stream is selecting from, return all possible instantiations of this parameter that
   * would match the row.
   *
   * This is used to assign rows to buckets. For instance, considering the query
   * `SELECT * FROM asset WHERE owner = request.user_id()`, we would introduce a parameter. For that parameter,
   * `filterRow(assetRow)` would return `assetRow.owner`.
   * When a user connects, {@link StaticLookup} would return the user ID from the token. A matching bucket would
   * then contain the oplog data for assets with the matching `owner` column.
   */
  filterRow(options: EvaluateRowOptions): SqliteJsonValue[];
}

/**
 * An association of rows to subscription parameters that does not depend on a subquery.
 */
interface StaticLookup {
  fromRequest(parameters: ParameterValueSet): SqliteValue | null;
}

interface DynamicLookup {
  parameterTable: TablePattern;
  createLookups(sourceTable: SourceTableInterface, row: SqliteRow): EvaluatedParametersResult[];
  instantiateForRequest(params: RequestParameters): DynamicParameterInstantiation[];
}

interface DynamicParameterInstantiation {
  /**
   * The lookup created in the {@link DynamicLookup.createLookups} call for a parameter row.
   */
  lookup: ParameterLookup;

  /**
   * Interprets the result of a lookup with the value instantiation of the {@link BucketParameter} being looked up.
   */
  interpretResult(lookupResults: SqliteJsonRow): SqliteValue | null;
}

export function isStaticLookup(lookup: StaticLookup | DynamicLookup): lookup is StaticLookup {
  return (lookup as StaticLookup).fromRequest != null;
}

export function isDynamicLookup(lookup: StaticLookup | DynamicLookup): lookup is DynamicLookup {
  return !isStaticLookup(lookup);
}

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
 */
export class StreamVariant {
  id: number;
  parameters: BucketParameter[];

  /**
   * Additional filters that don't affect bucket parameters, but can exclude rows.
   */
  additionalRowFilters: ((options: EvaluateRowOptions) => boolean)[];

  /**
   * Additional filters that only affect the queriers without introducing or relying on bucket parameters.
   */
  additionalParameterFilters: ((options: RequestParameters) => boolean)[];

  constructor(id: number) {
    this.id = id;
    this.parameters = [];
    this.additionalRowFilters = [];
    this.additionalParameterFilters = [];
  }

  get hasDynamicallyResolvedParameter(): boolean {
    return !_.every(this.parameters, (p: BucketParameter) => isStaticLookup(p.lookup));
  }

  /**
   * Given a row in the table this stream selects from, returns all ids of buckets to which that row belongs to.
   */
  bucketIdsForRow(streamName: string, options: EvaluateRowOptions): string[] {
    return this.instantiationsForRow(options).map((values) => this.buildBucketId(streamName, values));
  }

  /**
   * Given a row to evaluate, returns all instantiations of parameters that satisfy conditions.
   *
   * The inner arrays will have a length equal to the amount of parameters in this variant.
   */
  instantiationsForRow(options: EvaluateRowOptions): SqliteJsonValue[][] {
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
    if (instantiations.length == 0) {
      return [];
    }

    const cartesianProduct: SqliteJsonValue[][] = [];
    const totalLength = _.reduce(instantiations, (a, b) => a * b.length, 1);
    for (let i = 0; i < totalLength; i++) {
      const instantiation: SqliteJsonValue[] = [];
      let indexInSet = i;

      for (const parameterSet of instantiations) {
        instantiation.push(normalizeParameterValue(parameterSet[Math.floor(indexInSet % parameterSet.length)]));
        indexInSet = Math.floor(indexInSet / parameterSet.length);
      }

      cartesianProduct.push(instantiation);
    }

    return cartesianProduct;
  }

  querier(stream: SyncStream, reason: BucketInclusionReason, params: RequestParameters): BucketParameterQuerier | null {
    const instantiation = this.partiallyEvaluateParameters(params);
    if (instantiation == null) {
      return null;
    }

    let hasDynamicParameter = false;
    interface ResolvedDynamicParameter {
      index: number;
      instantiations: DynamicParameterInstantiation[];
    }

    const dynamicLookups: ResolvedDynamicParameter[] = [];

    for (let i = 0; i < this.parameters.length; i++) {
      const parameter = this.parameters[i];
      const lookup = parameter.lookup;

      if (isDynamicLookup(lookup)) {
        hasDynamicParameter = true;
        dynamicLookups.push({
          index: i,
          instantiations: lookup.instantiateForRequest(params)
        });
      }
    }

    const staticBuckets: ResolvedBucket[] = [];
    if (!hasDynamicParameter) {
      // When we have no dynamic parameters, the partial evaluation is a full instantiation.
      staticBuckets.push(this.resolveBucket(stream, instantiation as SqliteJsonValue[], reason));
    }

    const variant = this;
    return {
      staticBuckets: staticBuckets,
      hasDynamicBuckets: hasDynamicParameter,
      parameterQueryLookups: dynamicLookups.flatMap((l) => l.instantiations.map((i) => i.lookup)),
      async queryDynamicBucketDescriptions(source) {
        const perParameterInstantiation: (SqliteJsonValue | BucketParameter)[][] = [];
        for (const parameter of instantiation) {
          perParameterInstantiation.push([parameter]);
        }

        for (const lookup of dynamicLookups) {
          const allValues: SqliteJsonValue[] = [];

          for (const instantiation of lookup.instantiations) {
            const results = await source.getParameterSets([instantiation.lookup]);
            const interpreted = results.map((e) => instantiation.interpretResult(e)).filter(isJsonValue);

            allValues.push(...interpreted);
          }

          perParameterInstantiation[lookup.index] = allValues;
        }

        const product = variant.cartesianProductOfParameterInstantiations(
          perParameterInstantiation as SqliteJsonValue[][]
        );

        return Promise.resolve(product.map((e) => variant.resolveBucket(stream, e, reason)));
      }
    };
  }

  findStaticInstantiation(params: RequestParameters): SqliteJsonValue[] | null {
    if (this.hasDynamicallyResolvedParameter) {
      return null;
    }

    // This will be an array of values (i.e. a total evaluation) because there are no dynamic parameters.
    return this.partiallyEvaluateParameters(params) as SqliteJsonValue[];
  }

  /**
   * Creates lookup indices for dynamically-resolved parameters.
   *
   * Resolving dynamic parameters is a two-step process: First, for tables referenced in subqueries, we create an index
   * to resolve which request parameters would match rows in subqueries. Then, when resolving bucket ids for a request,
   * we compute subquery results by looking up results in that index.
   *
   * This implements the first step of that process.
   *
   * @param result The array into which evaluation results should be written to.
   * @param sourceTable A table we depend on in a subquery.
   * @param row Row data to index.
   */
  pushParameterRowEvaluation(result: EvaluatedParametersResult[], sourceTable: SourceTableInterface, row: SqliteRow) {
    for (const parameter of this.parameters) {
      const lookup = parameter.lookup;
      if (isDynamicLookup(lookup)) {
        if (lookup.parameterTable.matches(sourceTable)) {
          result.push(...lookup.createLookups(sourceTable, row));
        }
      }
    }
  }

  /**
   * Replaces {@link StreamVariant.parameters} with static values looked up in request parameters.
   */
  private partiallyEvaluateParameters(params: RequestParameters): (SqliteJsonValue | BucketParameter)[] | null {
    for (const filter of this.additionalParameterFilters) {
      if (!filter(params)) {
        return null;
      }
    }

    const instantiation: (SqliteJsonValue | BucketParameter)[] = [];
    for (const parameter of this.parameters) {
      const lookup = parameter.lookup;
      if (isStaticLookup(lookup)) {
        const value = lookup.fromRequest(params);
        if (isJsonValue(value)) {
          instantiation.push(value);
        } else {
          return null;
        }
      } else {
        instantiation.push(parameter);
      }
    }

    return instantiation;
  }

  private buildBucketId(streamName: string, instantiation: SqliteJsonValue[]) {
    if (instantiation.length != this.parameters.length) {
      throw Error('Internal error, instantiation length mismatch');
    }

    return `${streamName}|${this.id}${JSONBucketNameSerialize.stringify(instantiation)}`;
  }

  private resolveBucket(
    stream: SyncStream,
    instantiation: SqliteJsonValue[],
    reason: BucketInclusionReason
  ): ResolvedBucket {
    return {
      definition: stream.name,
      inclusion_reasons: [reason],
      bucket: this.buildBucketId(stream.name, instantiation),
      priority: stream.priority
    };
  }
}

interface BucketCompilationContext {
  streamName: string;
  allVariants: StreamVariant[];
  currentVariant: StreamVariant;
  queryCounter: number;
}

/**
 * A `NOT` operator that inverts an inner operator.
 *
 * This is only used temporarily when compiling the filter: It is illegal to negate {@link InOperator} and
 * {@link CompareRowValueWithStreamParameter} clauses, as those are implemented with special lookups based on bucket
 * parameters.
 * For {@link EvaluateSimpleCondition}, a wrapping {@link Not} clause is unecessary, because we can simply push the
 * negation into the inner evaluator.
 */
export class Not extends FilterOperator {
  private operand: FilterOperator;

  constructor(operand: FilterOperator) {
    super();
    this.operand = operand;
  }

  compile(): void {
    throw Error('Not operator should have been desugared before compilation.');
  }
}

export class And extends FilterOperator {
  private left: FilterOperator;
  private right: FilterOperator;

  constructor(left: FilterOperator, right: FilterOperator) {
    super();
    this.left = left;
    this.right = right;
  }

  compile(context: BucketCompilationContext) {
    // Within a variant, added conditions and parameters form a conjunction.
    this.left.compile(context);
    this.right.compile(context);
  }
}

export class Or extends FilterOperator {
  private left: FilterOperator;
  private right: FilterOperator;

  constructor(left: FilterOperator, right: FilterOperator) {
    super();
    this.left = left;
    this.right = right;
  }

  compile(context: BucketCompilationContext): void {
    throw 'todo: compile or operators';
  }
}

export class Subquery {
  private table: TablePattern;
  private column: RowValueClause;
  private filter: CompareRowValueWithStreamParameter | EvaluateSimpleCondition;

  constructor(
    table: TablePattern,
    column: RowValueClause,
    filter: CompareRowValueWithStreamParameter | EvaluateSimpleCondition
  ) {
    this.table = table;
    this.column = column;
    this.filter = filter;
  }

  createLookup(context: BucketCompilationContext): DynamicLookup {
    const innerVariants = this.filter.compileVariants(context.streamName).map((variant) => {
      const id = context.queryCounter.toString();
      context.queryCounter++;

      return [variant, id] satisfies [StreamVariant, string];
    });

    const column = this.column;

    return {
      parameterTable: this.table,

      createLookups(sourceTable, row) {
        const value = column.evaluate({ [sourceTable.table]: row });
        if (!isJsonValue(value)) {
          return [];
        }

        const lookups: EvaluatedParameters[] = [];
        for (const [variant, id] of innerVariants) {
          for (const instantiation of variant.instantiationsForRow({ sourceTable, record: row })) {
            const lookup = ParameterLookup.normalized(context.streamName, id, instantiation);
            lookups.push({
              lookup,
              bucketParameters: [
                {
                  result: value
                }
              ]
            });
          }
        }
        return lookups;
      },
      instantiateForRequest(parameters) {
        const variantInstantiations: DynamicParameterInstantiation[] = [];

        for (const [variant, id] of innerVariants) {
          const instantiation = variant.findStaticInstantiation(parameters);
          if (instantiation == null) {
            continue;
          }

          variantInstantiations.push({
            lookup: ParameterLookup.normalized(context.streamName, id, instantiation),
            interpretResult(lookupResults) {
              // In createLookups, we always create a row with a single column: result, containing the expression the
              // subquery evaluates to.
              return lookupResults['result'];
            }
          });
        }

        return variantInstantiations;
      }
    };
  }
}

export type ScalarExpression = RowValueClause | ParameterValueClause;

/**
 * A filter that matches when _something_ is contained in a subquery or a JSON array.
 *
 * Examples:
 *
 *  - `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user())`
 *  - `SELECT * FROM secrets WHERE request.user_id() IN (SELECT user_id FROM users WHERE is_admin)`
 *
 *  An `IN` operator can also be handled without requiring a subquery:
 *
 *  - `SELECT * FROM comments WHERE id IN request.params('id')` ({@link CompareRowValueWithStreamParameter})
 *  - `SELECT * FROM comments WHERE request.user() IN comments.tagged_users` ({@link CompareRowValueWithStreamParameter})
 */
export class InOperator extends FilterOperator {
  private left: ScalarExpression;
  private right: Subquery;

  constructor(left: ScalarExpression, right: Subquery) {
    super();
    this.left = left;
    this.right = right;
  }

  compile(context: BucketCompilationContext): void {
    if (isRowValueClause(this.left)) {
      const filter = this.left;
      // Something like `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user())`
      // This groups rows into buckets identified by comments.issue_id, which happens in filterRow.
      // When a user connects, we need to resolve all the issue ids they own. This happens with an indirection:
      //  1. In the dynamic lookup, we create an index from owner_id to issue ids.
      //  2. When we have users, we use that lookup to find issue ids dynamically, with which we can build the buckets
      //     to sync.
      context.currentVariant.parameters.push({
        lookup: this.right.createLookup(context),
        filterRow(options) {
          const tables = { [options.sourceTable.table]: options.record };
          const value = filter.evaluate(tables);
          if (isJsonValue(value)) {
            return [normalizeParameterValue(value)];
          } else {
            return [];
          }
        }
      });
    } else if (isParameterValueClause(this.left)) {
      throw 'todo: compile in operators';
    } else {
      const _: never = this.left; // Exhaustive check
    }
  }
}

/**
 * A filter that matches if a column of the input table matches some condition that depends on the request parameters.
 *
 * E.g. `SELECT * FROM issue_id WHERE owner_id = request.user_id()`. This will add the referenced column as a bucket
 * parameter, but doesn't require a dynamic lookup.
 *
 * We only allow a few operators that are efficient to compute here (effectively just an equality operator).
 */
export class CompareRowValueWithStreamParameter extends FilterOperator {
  private match: ParameterMatchClause;

  constructor(match: ParameterMatchClause) {
    super();
    this.match = match;
  }

  compile(context: BucketCompilationContext): void {
    const match = this.match;

    for (const filters of match.inputParameters) {
      context.currentVariant.parameters.push({
        lookup: {
          fromRequest(parameters) {
            return filters.parametersToLookupValue(parameters);
          }
        },
        filterRow(options) {
          const tables = { [options.sourceTable.table]: options.record };
          const matchingValues: SqliteJsonValue[] = [];

          for (const matchingParameters of match.filterRow(tables)) {
            const matchingValue = matchingParameters[filters.key];
            if (matchingValue != null) {
              matchingValues.push(normalizeParameterValue(matchingValue));
            }
          }

          return matchingValues;
        }
      });
    }
  }
}

/**
 * A simple condition that is either static, only depends on the current row being matched, or only depends on the input
 * parameters.
 */
export class EvaluateSimpleCondition extends FilterOperator {
  expression: ScalarExpression;

  constructor(expression: ScalarExpression) {
    super();
    this.expression = expression;
  }

  compile(context: BucketCompilationContext): void {
    if (isRowValueClause(this.expression)) {
      const filter = this.expression;

      context.currentVariant.additionalRowFilters.push((row) => {
        return sqliteBool(filter.evaluate({ [row.sourceTable.table]: row.record })) == SQLITE_TRUE;
      });
    } else if (isParameterValueClause(this.expression)) {
      const filter = this.expression;

      context.currentVariant.additionalParameterFilters.push((request) => {
        return sqliteBool(filter.lookupParameterValue(request)) == SQLITE_TRUE;
      });
    } else {
      const _: never = this.expression; // Exhaustive check
    }
  }

  negate(tools: SqlTools): EvaluateSimpleCondition {
    return new EvaluateSimpleCondition(tools.composeFunction(OPERATOR_NOT, [this.expression], []) as ScalarExpression);
  }
}
