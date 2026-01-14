import { ParameterLookupSource, ScopedParameterLookup, UnscopedParameterLookup } from '../BucketParameterQuerier.js';
import { ParameterIndexLookupCreator } from '../BucketSource.js';
import { HydrationState } from '../HydrationState.js';
import { cartesianProduct } from '../streams/utils.js';
import { RequestParameters, SqliteJsonValue, SqliteValue } from '../types.js';
import { isJsonValue } from '../utils.js';
import * as plan from './plan.js';
import { StreamInput } from './querier_impl.js';
import { parametersForRequest } from './request_filters.js';
import { SqlBuilder } from './sql_engine.js';

export type PreparedExpandingLookup =
  | { type: 'parameter'; lookup: ParameterIndexLookupCreator; instantiation: PreparedParameterValue[] }
  | { type: 'table_valued'; read(request: RequestParameters): SqliteParameterValue[][]; usesSubscriptionData: boolean }
  | { type: 'cached'; values: SqliteParameterValue[][] };

/**
 * A {@link plan.ParameterValue} that can be evalauted against request parameters.
 *
 * Additionally, this includes the `cached` variant which allows partially instantiating parameters.
 */
export type PreparedParameterValue =
  | { type: 'request'; read(request: RequestParameters): SqliteValue; usesSubscriptionData: boolean }
  | { type: 'lookup'; lookup: { stage: number; index: number }; resultIndex: number }
  | { type: 'intersection'; values: PreparedParameterValue[] }
  | { type: 'cached'; values: SqliteParameterValue[] };

export interface PartialInstantiationInput {
  request: RequestParameters;
  hasSubscriptionData: boolean;
}

export interface InstantiationInput extends PartialInstantiationInput {
  hydrationState: HydrationState;
  source: ParameterLookupSource;
}

export interface CompleteInstantiationInput extends PartialInstantiationInput {}

export class RequestParameterEvaluators {
  private constructor(
    readonly lookupStages: PreparedExpandingLookup[][],
    readonly parameterValues: PreparedParameterValue[]
  ) {}

  /**
   * Returns a copy of this instance.
   *
   * We use this to be able to "fork" partial instantiations. For instance, we can evaluate paremeters not depending on
   * subscription data as soon as the user connects (and reuse those instantiations for each subscription).
   *
   * Then for each subscription, we can further instantiate lookup stages and parameter values.
   */
  clone(): RequestParameterEvaluators {
    return new RequestParameterEvaluators(
      this.lookupStages.map((stage) => [...stage]),
      [...this.parameterValues]
    );
  }

  /**
   * Evaluates lookups and parameter values that be evaluated with a subset of the final input.
   *
   * This is used to determine whether a querier is static - the partial instantiation depending on request data fully
   * resolves the stream, we don't need to lookup any parameters.
   */
  partiallyInstantiate(input: PartialInstantiationInput) {
    const helper = new PartialInstantiator(input, this);

    this.lookupStages.forEach((stage, stageIndex) => {
      stage.forEach((_, indexInStage) => helper.expandingLookupSync(stageIndex, indexInStage));
    });

    this.parameterValues.forEach((_, i) => helper.parameterSync(this.parameterValues, i));
  }

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

    // Outer array represents parameter, inner array represents values for a given parameter.
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

  static prepare(lookupStages: plan.ExpandingLookup[][], values: plan.ParameterValue[], input: StreamInput) {
    const mappedStages: PreparedExpandingLookup[][] = [];
    const lookupToStage = new Map<plan.ExpandingLookup, { stage: number; index: number }>();

    function mapParameterValue(value: plan.ParameterValue): PreparedParameterValue {
      if (value.type == 'request') {
        const usesSubscriptionData = value.expr.values.find((e) => e.request === 'subscription') != null;
        const builder = new SqlBuilder('SELECT ');
        builder.addExpression(value.expr);

        const prepared = input.engine.prepare(builder.sql);
        return {
          type: 'request',
          read(request) {
            return prepared.evaluateScalar(parametersForRequest(request, builder.values))![0];
          },
          usesSubscriptionData
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
          const builder = new SqlBuilder('SELECT ');
          builder.addExpressions(lookup.outputs);
          builder.sql += ` FROM "${lookup.functionName}"(`;
          builder.addExpressions(lookup.functionInputs);
          builder.sql += ')';

          if (lookup.filters) {
            builder.sql += ` WHERE `;
            builder.addExpressions(lookup.outputs);
          }

          const stmt = input.engine.prepare(builder.sql);
          const usesSubscriptionData =
            lookup.functionInputs.find((e) => e.values.find((v) => v.request === 'subscription') != null) != null;
          mappedStage.push({
            type: 'table_valued',
            usesSubscriptionData,
            read(request) {
              return [...filterParameterRows(stmt.evaluateMultiple(parametersForRequest(request, builder.values)))];
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

  canResolve(usesSubscriptionData: boolean) {
    return usesSubscriptionData ? this.input.hasSubscriptionData : true;
  }

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
          intersection = intersection.intersection(new Set(evaluated));
        }

        if (intersection.size == 0) {
          // We don't even need to evaluate the rest.
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
    } else if (current.type === 'request' && this.canResolve(current.usesSubscriptionData)) {
      const value = current.read(this.input.request);
      const values: SqliteParameterValue[] = isValidParameterValue(value) ? [value] : [];

      parent[index] = { type: 'cached', values };
      return values;
    }

    return undefined;
  }

  expandingLookupSync(stage: number, index: number): SqliteParameterValue[][] | undefined {
    const lookup = this.evaluators.lookupStages[stage][index];
    if (lookup.type == 'table_valued' && this.canResolve(lookup.usesSubscriptionData)) {
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

      const outputs = await this.input.source.getParameterSets([
        ...this.resolveInputs(lookup.instantiation).map((instantiation) =>
          ScopedParameterLookup.normalized(scope, UnscopedParameterLookup.normalized(instantiation))
        )
      ]);

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

// All parameters are compared via the equals operator, and null is not equal to anything. Also, we can only
// persist JSON values
export type SqliteParameterValue = NonNullable<SqliteJsonValue>;

export function isValidParameterValue(value: SqliteValue): value is SqliteParameterValue {
  return value != null && isJsonValue(value);
}

export function isValidParameterValueRow(row: SqliteValue[]): row is SqliteParameterValue[] {
  for (const value of row) {
    if (!isValidParameterValue(value)) {
      return false;
    }
  }

  return true;
}

function* filterParameterRows(rows: SqliteValue[][]): Generator<SqliteParameterValue[]> {
  for (const row of rows) {
    if (isValidParameterValueRow(row)) {
      yield row;
    }
  }
}
