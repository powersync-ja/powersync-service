import { ParameterLookupSource, ScopedParameterLookup, UnscopedParameterLookup } from '../../BucketParameterQuerier.js';
import { ParameterIndexLookupCreator } from '../../BucketSource.js';
import { HydrationState } from '../../HydrationState.js';
import { cartesianProduct } from '../../streams/utils.js';
import { RequestParameters, SqliteParameterValue, SqliteValue } from '../../types.js';
import { isValidParameterValue } from '../../utils.js';
import { MapSourceVisitor, visitExpr } from '../expression_visitor.js';
import * as plan from '../plan.js';
import { StreamInput } from './bucket_source.js';
import {
  mapExternalDataToInstantiation,
  TableValuedFunction,
  TableValuedFunctionOutput
} from '../engine/scalar_expression_engine.js';

/**
 * Finds bucket parameters for a given request or subscription.
 *
 * In sync streams, queriers are represented as a DAG structure describing how to get from connection data to bucket
 * parameters.
 *
 * As an example, consider the following stream:
 *
 * ```
 * SELECT projects.* FROM projects
 *  INNER JOIN orgs ON orgs.id = projects.org_id
 * WHERE orgs.name = auth.parameter('org')
 * ```
 *
 * This would partition data into a bucket with a single parameter (grouping by `projects.org_id`). It would also
 * prepare a lookup from `orgs.name` to `orgs.id`.
 *
 * The querier for this would have:
 *
 *  1. A single lookup stage with a single {@link plan.ParameterLookup}. That lookup would have an instantiation
 *     reflecting `auth.parameter('org')` as a `request` {@link plan.ParameterValue}.
 *  2. A single {@link plan.StreamQuerier.sourceInstantiation}, a `lookup` {@link plan.ParameterValue} referencing the
 *     lookup from step 1.
 *
 * On this prepared evaluator, lookup stages and parameter values are tracked as {@link PreparedExpandingLookup}s and
 * {@link PreparedParameterValue}s, respectively. These correspond to their definitions on sync plans, except that:
 *
 *   1. Instead of being a description of the parameter, they're a JavaScript function that can be invoked to compute
 *      parameters.
 *   2. After being called once, we can replace them with a cached value. This enables a partial instantiation, and
 *      avoids recomputing everything whenever a parameter lookup changes. In the example stream, we would run and cache
 *      the outputs of `auth.parameter('org')` for a given connection. This sub-expression would not get re-evaluated
 *      when the `org-name` -> `org.id` lookup changes.
 *
 * For queriers that don't use parameter lookups, e.g. for streams like `SELECT * FROM users WHERE id = auth.user_id()`,
 * the partial instantiation based on connection data happens to be a complete instantiation. We use this when building
 * queriers by indicating that no lookups will be used.
 */
export class RequestParameterEvaluators {
  private constructor(
    /**
     * Pending lookup stages, or their cached outputs.
     */
    readonly lookupStages: PreparedExpandingLookup[][],
    /**
     * Pending parameter values, or their cached outputs.
     */
    readonly parameterValues: PreparedParameterValue[]
  ) {}

  /**
   * Returns a copy of this instance.
   *
   * We use this to be able to "fork" partial instantiations. For instance, we can evaluate paremeters not depending on
   * parameter lookups as soon as the user connects (and keep that instantiation static along the lifetime of the
   * connection).
   */
  clone(): RequestParameterEvaluators {
    return new RequestParameterEvaluators(
      this.lookupStages.map((stage) => [...stage]),
      [...this.parameterValues]
    );
  }

  /**
   * Evaluates those lookups and parameter values that be evaluated without looking up parameter indexes.
   *
   * This is also used to determine whether a querier is static - if the partial instantiation depending on request data
   * fully resolves the stream, we don't need to lookup any parameters.
   */
  partiallyInstantiate(input: PartialInstantiationInput) {
    const helper = new PartialInstantiator(input, this);

    this.lookupStages.forEach((stage, stageIndex) => {
      stage.forEach((_, indexInStage) => helper.expandingLookupSync(stageIndex, indexInStage));
    });

    this.parameterValues.forEach((_, i) => helper.parameterSync(this.parameterValues, i));
  }

  /**
   * Resolves and caches all lookup stages and parameter values.
   *
   * Because this needs to lookup parameter indexes, it is asynchronous.
   */
  async instantiate(input: InstantiationInput): Promise<Generator<SqliteParameterValue[]>> {
    const helper = new FullInstantiator(input, this);

    for (let i = 0; i < this.lookupStages.length; i++) {
      // Within a stage, we can resolve lookups concurrently.
      await Promise.all(this.lookupStages[i].map((_, j) => helper.expandingLookup(i, j)));
    }

    // At this point, all lookups have been resolved and we can synchronously evaluate parameters which might depend on
    // those lookups.
    return helper.resolveInputs(this.parameterValues);
  }

  /**
   * Whether these evaluators are known to not result in any buckets, for instance because parameters are instanted to
   * `NULL` values that aren't equal to anything.
   *
   * This is fairly efficient to compute and can be used to short-circuit further evaluation.
   */
  isDefinitelyUninstantiable() {
    for (const parameter of this.parameterValues) {
      if (parameter.type != 'cached') {
        return false; // Unknown
      }

      if (parameter.values.length === 0) {
        // Missing parameter.
        return true;
      }
    }

    return false;
  }

  extractFullInstantiation(): SqliteParameterValue[][] | undefined {
    // All lookup stages need to be resolved, even if they're not used in a parameter. The reason is that queries like
    // `WHERE 'static_value' IN (SELECT name FROM users WHERE id = auth.user_id())` are implemented as lookup stages,
    // so we can't ignore them.
    for (const stage of this.lookupStages) {
      for (const element of stage) {
        if (element.type !== 'cached') {
          return undefined;
        }
      }
    }

    // Outer array represents parameters, inner array represents values for a given parameter.
    const parameters: SqliteParameterValue[][] = [];
    for (const parameter of this.parameterValues) {
      if (parameter.type !== 'cached') {
        return undefined;
      }

      parameters.push(parameter.values);
    }

    // Transform to array of complete instantiations.
    return [...cartesianProduct(...parameters)];
  }

  /**
   * Prepares evaluators for a description of parameter values obtained from a compiled querier in the sync plan.
   *
   * @param lookupStages The {@link plan.StreamQuerier.lookupStages} of the querier to compile.
   * @param values The {@link plan.StreamQuerier.sourceInstantiation} of the querier to compile.
   * @param input Access to bucket and parameter sources generated for buckets and parameter lookups referenced by the
   * querier.
   */
  static prepare(lookupStages: plan.ExpandingLookup[][], values: plan.ParameterValue[], input: StreamInput) {
    const mappedStages: PreparedExpandingLookup[][] = [];
    const lookupToStage = new Map<plan.ExpandingLookup, { stage: number; index: number }>();

    function mapParameterValue(value: plan.ParameterValue): PreparedParameterValue {
      if (value.type == 'request') {
        // Prepare an expression evaluating the expression derived from request data.
        const mapper = mapExternalDataToInstantiation<plan.RequestSqlParameterValue>();
        const prepared = input.engine.prepareEvaluator({ filters: [], outputs: [mapper.transform(value.expr)] });
        const instantiation = mapper.instantiation;

        return {
          type: 'request',
          read(request) {
            return prepared.evaluate(parametersForRequest(request, instantiation))[0][0];
          }
        };
      } else if (value.type == 'lookup') {
        const stagePosition = lookupToStage.get(value.lookup)!;
        return { type: 'lookup', lookup: stagePosition, resultIndex: value.resultIndex };
      } else {
        return { type: 'intersection', values: mapParameterValues(value.values) };
      }
    }

    function mapParameterValues(values: plan.ParameterValue[]) {
      return values.map(mapParameterValue);
    }

    for (const stage of lookupStages) {
      const stageIndex = mappedStages.length;
      const mappedStage: PreparedExpandingLookup[] = [];

      for (const lookup of stage) {
        const index = mappedStage.length;
        lookupToStage.set(lookup, { stage: stageIndex, index });

        if (lookup.type == 'parameter') {
          mappedStage.push({
            type: 'parameter',
            lookup: input.preparedLookups.get(lookup.lookup)!,
            instantiation: mapParameterValues(lookup.instantiation)
          });
        } else {
          // Create an expression like SELECT <output> FROM table_valued(<functionInputs>) WHERE <filters>
          const mapInputs = mapExternalDataToInstantiation();
          const fn: TableValuedFunction = {
            name: lookup.functionName,
            inputs: lookup.functionInputs.map((e) => mapInputs.transform(e))
          };
          const mapOutputs = new MapSourceVisitor<plan.ColumnSqlParameterValue, TableValuedFunctionOutput>(
            ({ column }) => ({
              function: fn,
              column
            })
          );

          const prepared = input.engine.prepareEvaluator({
            tableValuedFunctions: [fn],
            outputs: lookup.outputs.map((e) => visitExpr(mapOutputs, e, null)),
            filters: lookup.filters.map((e) => visitExpr(mapOutputs, e, null))
          });

          mappedStage.push({
            type: 'table_valued',
            read(request) {
              return [
                ...filterParameterRows(prepared.evaluate(parametersForRequest(request, mapInputs.instantiation)))
              ];
            }
          });
        }
      }
    }

    return new RequestParameterEvaluators(mappedStages, mapParameterValues(values));
  }
}

class PartialInstantiator<I extends PartialInstantiationInput = PartialInstantiationInput> {
  constructor(
    protected readonly input: I,
    protected readonly evaluators: RequestParameterEvaluators
  ) {}

  /**
   * If possible, evaluates an element in an array of parameter values and replaces the parameter with a marker
   * indicating it as cached.
   */
  parameterSync(parent: PreparedParameterValue[], index: number): SqliteParameterValue[] | undefined {
    const current = parent[index];
    if (current.type === 'cached') {
      return current.values;
    } else if (current.type === 'intersection') {
      let intersection: Set<SqliteParameterValue> | null = null;
      for (let i = 0; i < current.values.length; i++) {
        const evaluated = this.parameterSync(current.values, i);
        if (evaluated == null) {
          return undefined; // Can't evaluate sub-parameter
        }

        if (intersection == null) {
          intersection = new Set(evaluated);
        } else {
          // TODO: Remove as any once we can use ES2025 in TypeScript
          intersection = (intersection as any).intersection(new Set(evaluated)) as Set<SqliteParameterValue>;
        }

        if (intersection.size == 0) {
          // Empty intersection, we don't even need to evaluate the rest.
          break;
        }
      }

      let values: SqliteParameterValue[] = [];
      if (intersection) {
        values.push(...intersection.keys());
      }

      parent[index] = { type: 'cached', values };
      return values;
    } else if (current.type === 'lookup') {
      const resolvedLookup = this.expandingLookupSync(current.lookup.stage, current.lookup.index);
      if (resolvedLookup) {
        const values = resolvedLookup.map((row) => row[current.resultIndex]);
        parent[index] = { type: 'cached', values };
        return values;
      }
    } else if (current.type === 'request') {
      const value = current.read(this.input.request);
      const values: SqliteParameterValue[] = isValidParameterValue(value) ? [value] : [];

      parent[index] = { type: 'cached', values };
      return values;
    }

    return undefined;
  }

  expandingLookupSync(stage: number, index: number): SqliteParameterValue[][] | undefined {
    const lookup = this.evaluators.lookupStages[stage][index];
    if (lookup.type == 'table_valued') {
      // We can evaluate this table-valued function already.
      const values = lookup.read(this.input.request);
      this.evaluators.lookupStages[stage][index] = { type: 'cached', values };
      return values;
    } else if (lookup.type == 'cached') {
      return lookup.values;
    }

    return undefined;
  }
}

class FullInstantiator extends PartialInstantiator<InstantiationInput> {
  *resolveInputs(params: PreparedParameterValue[]): Generator<SqliteParameterValue[]> {
    const parameterValues = params.map((_, index) => {
      const cached = this.parameterSync(params, index);
      if (cached == null) {
        // This method is only called for inputs from an earlier stage, which should have been resolved at this point.
        throw new Error('Should have been able to resolve parameter from earlier stage synchronously.');
      }
      return cached;
    });

    yield* cartesianProduct(...parameterValues);
  }

  async expandingLookup(stage: number, index: number): Promise<SqliteParameterValue[][]> {
    const lookup = this.evaluators.lookupStages[stage][index];
    if (lookup.type == 'parameter') {
      const scope = this.input.hydrationState.getParameterIndexLookupScope(lookup.lookup);
      lookup.instantiation;

      const outputs = await this.input.source.getParameterSets(
        [...this.resolveInputs(lookup.instantiation)].map((instantiation) =>
          ScopedParameterLookup.normalized(scope, UnscopedParameterLookup.normalized(instantiation))
        )
      );

      // Stream parameters generate an output row like {0: <expr>, 1: <expr>, ...}.
      const values = outputs.map((row) => {
        const length = Object.entries(row).length;
        const asArray: SqliteParameterValue[] = [];

        for (let i = 0; i < length; i++) {
          asArray.push(row[i.toString()] as SqliteParameterValue);
        }
        return asArray;
      });

      this.evaluators.lookupStages[stage][index] = { type: 'cached', values };
      return values;
    }

    const other = this.expandingLookupSync(stage, index);
    if (other == null) {
      throw new Error('internal error: Unable to resolve non-parameter lookup synchronously?');
    }
    return other;
  }
}

export type PreparedExpandingLookup =
  | { type: 'parameter'; lookup: ParameterIndexLookupCreator; instantiation: PreparedParameterValue[] }
  | { type: 'table_valued'; read(request: RequestParameters): SqliteParameterValue[][] }
  | { type: 'cached'; values: SqliteParameterValue[][] };

/**
 * A {@link plan.ParameterValue} that can be evaluated against request parameters.
 *
 * Additionally, this includes the `cached` variant which allows partially instantiating parameters.
 */
export type PreparedParameterValue =
  | { type: 'request'; read(request: RequestParameters): SqliteValue }
  | { type: 'lookup'; lookup: { stage: number; index: number }; resultIndex: number }
  | { type: 'intersection'; values: PreparedParameterValue[] }
  | { type: 'cached'; values: SqliteParameterValue[] };

export interface PartialInstantiationInput {
  request: RequestParameters;
}

export interface InstantiationInput extends PartialInstantiationInput {
  hydrationState: HydrationState;
  source: ParameterLookupSource;
}

export function isValidParameterValueRow(row: SqliteValue[]): row is SqliteParameterValue[] {
  for (const value of row) {
    if (!isValidParameterValue(value)) {
      return false;
    }
  }

  return true;
}

export function parametersForRequest(parameters: RequestParameters, values: plan.SqlParameterValue[]): string[] {
  return values.map((v) => {
    if ('request' in v) {
      switch (v.request) {
        case 'auth':
          return parameters.rawTokenPayload;
        case 'subscription':
          return parameters.rawStreamParameters!;
        case 'connection':
          return parameters.rawUserParameters;
      }
    } else {
      throw new Error('Illegal column reference in request filter');
    }
  });
}

function* filterParameterRows(rows: SqliteValue[][]): Generator<SqliteParameterValue[]> {
  for (const row of rows) {
    if (isValidParameterValueRow(row)) {
      yield row;
    }
  }
}
