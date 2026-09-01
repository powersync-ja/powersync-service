import { ParameterLookupSource, ScopedParameterLookup, UnscopedParameterLookup } from '../../BucketParameterQuerier.js';
import { ParameterIndexLookupCreator } from '../../BucketSource.js';
import { HydrationState } from '../../HydrationState.js';
import { RequestParameters, SqliteParameterValue, SqliteValue } from '../../types.js';
import { isValidParameterValue } from '../../utils.js';
import {
  mapExternalDataToInstantiation,
  ScalarExpressionEngine,
  TableValuedFunction,
  TableValuedFunctionOutput
} from '../engine/scalar_expression_engine.js';
import { MapSourceVisitor, visitExpr } from '../expression_visitor.js';
import * as plan from '../plan.js';
import { StreamInput } from './bucket_source.js';
import { PreparedParameterIndexLookupCreator } from './parameter_index_lookup_creator.js';
import { AsyncJoinLookup, ResultSet, ResultSetColumn, ResultSetElement } from './result_set.js';

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
    readonly stream: plan.StreamOptions,
    /**
     * Pending lookup stages, or their cached outputs.
     */
    readonly lookupStages: PreparedExpandingLookup[][],
    /**
     * Pending parameter values, or their cached outputs.
     */
    readonly parameterValues: PreparedParameterValue[],

    /**
     * The materialized result set into which
     */
    readonly resultSet: ResultSet
  ) {}

  /**
   * Returns a copy of this instance.
   *
   * Since resolved values are replaced with their instantiation, we need to use closed evaluators before evaluating
   * them on inputs that might change (like parameter lookups).
   *
   * Static data (like connection parameters) can be resolved sooner, and cloning that partially-instantiated evaluator
   * graph essentially forks it. This allows us to cache connection parameters for the lifetime of the connection
   * instead of re-evaluating them on every parameter lookup change.
   */
  clone(): RequestParameterEvaluators {
    const copiedStages = this.lookupStages.map((s) => s.map((e) => e.clone()));
    const outputValues = this.parameterValues.map((v) => v.clone());

    return new RequestParameterEvaluators(this.stream, copiedStages, outputValues, this.resultSet.clone());
  }

  /**
   * Evaluates those lookups and parameter values that be evaluated without looking up parameter indexes.
   *
   * If this partial instantiation happens to be a total one (i.e. there are no remaining dynamic lookups that could
   * affect resolved parameters), returns all instantiations as an array.
   *
   * If dynamic lookups are required to resolve parameters, returns `undefined`.
   */
  partiallyInstantiate(input: PartialInstantiationInput): SqliteParameterValue[][] | undefined {
    try {
      // At this point, we can resolve table-valued lookups and parameter values based only on request data.
      for (const stage of this.lookupStages) {
        for (const element of stage) {
          if (element instanceof TableValuedExpandingLookup) {
            const outputs = element.read(input.request);
            element.wasResolved = true;
            this.resultSet.multiply(element.resultSetIndex, outputs);

            this.#checkInstantiable();
          } else {
            for (const instantiation of element.instantiation) {
              if (instantiation instanceof RequestParameterValue) instantiation.resolveWith(input);
            }
          }
        }
      }

      for (const parameter of this.parameterValues) {
        if (parameter instanceof RequestParameterValue) parameter.resolveWith(input);
      }

      return this.#readParameters();
    } catch (e) {
      if (e === uninstantiableException) return [];

      throw e;
    }
  }

  /**
   * Resolves and caches all lookup stages and parameter values.
   *
   * Because this needs to lookup parameter indexes, it is asynchronous.
   */
  async instantiate(input: InstantiationInput): Promise<SqliteParameterValue[][]> {
    try {
      for (const stage of this.lookupStages) {
        for (const lookup of stage) {
          if (lookup instanceof ParameterIndexExpandingLookup) {
            await this.#instantiateLookup(lookup, input);
          }
        }
      }

      return this.#readParameters()!;
    } catch (e) {
      if (e === uninstantiableException) return [];

      throw e;
    }
  }

  #checkInstantiable() {
    if (this.resultSet.length === 0) throw uninstantiableException;
  }

  #readParameters(): SqliteParameterValue[][] | undefined {
    for (const stage of this.lookupStages) {
      for (const element of stage) {
        if (!element.wasResolved) {
          return undefined;
        }
      }
    }

    return this.#readValues(this.parameterValues);
  }

  #readValues(values: PreparedParameterValue[]): SqliteParameterValue[][] {
    const allInstantiations: SqliteParameterValue[][] = [];

    for (const row of this.resultSet.projectUnique(values.filter((v) => v instanceof LookupParameterValue))) {
      allInstantiations.push(this.#evaluateAgainstRow(row, values));
    }

    return allInstantiations;
  }

  #evaluateAgainstRow(row: SqliteParameterValue[], projection: PreparedParameterValue[]) {
    let lookupIndex = 0;

    return projection.map((v) => {
      if (v instanceof LookupParameterValue) {
        return row[lookupIndex++];
      } else {
        if (!v.resolved) throw new Error('Expected request values to be resolved here');
        return v.resolved;
      }
    });
  }

  async #instantiateLookup(lookup: ParameterIndexExpandingLookup, input: InstantiationInput) {
    const scope = input.hydrationState.getParameterIndexLookupScope(lookup.lookup);
    const resolvedLookup = lookup.lookup as PreparedParameterIndexLookupCreator;

    await this.resultSet.joinAsync(
      lookup.instantiation.filter((v) => v instanceof LookupParameterValue),
      lookup.resultSetIndex,
      async (inputs) => {
        const bucketStorageLookups = new Map<ScopedParameterLookup, AsyncJoinLookup>();
        for (const input of inputs) {
          bucketStorageLookups.set(
            ScopedParameterLookup.normalized(
              scope,
              UnscopedParameterLookup.normalized(this.#evaluateAgainstRow(input.inputs, lookup.instantiation))
            ),
            input
          );
        }

        const outputs = await input.source.getParameterSets(
          [...bucketStorageLookups.keys()],
          `Stream ${this.stream.name} evaluating parameter on ${resolvedLookup.sourceTable.tablePattern}`
        );

        for (const { lookup, rows } of outputs) {
          const join = bucketStorageLookups.get(lookup)!;

          for (const row of rows) {
            const length = Object.entries(row).length;
            const asArray: SqliteParameterValue[] = [];
            for (let i = 0; i < length; i++) {
              asArray.push(row[i.toString()] as SqliteParameterValue);
            }

            join.foundRows.push(asArray);
          }
        }
      }
    );
    lookup.wasResolved = true;
    this.#checkInstantiable();
  }

  /**
   * Prepares evaluators for a description of parameter values obtained from a compiled querier in the sync plan.
   *
   * @param stream Used to show the name of the stream for debugging purposes.
   * @param lookupStages The {@link plan.StreamQuerier.lookupStages} of the querier to compile.
   * @param values The {@link plan.StreamQuerier.sourceInstantiation} of the querier to compile.
   * @param input Access to bucket and parameter sources generated for buckets and parameter lookups referenced by the
   * querier.
   * @param engine The scalar SQL engine used to evaluate operators and functions on request data.
   */
  static prepare(
    stream: plan.StreamOptions,
    lookupStages: plan.ExpandingLookup[][],
    values: plan.ParameterValue[],
    input: StreamInput,
    engine: ScalarExpressionEngine
  ) {
    const mappedStages: PreparedExpandingLookup[][] = [];
    let amountOfLookups = 0;
    const lookupToStage = new Map<plan.ExpandingLookup, PreparedExpandingLookup>();

    function mapParameterValue(value: plan.ParameterValue): PreparedParameterValue {
      if (value.type == 'request') {
        // Prepare an expression evaluating the expression derived from request data.
        const mapper = mapExternalDataToInstantiation<plan.RequestSqlParameterValue>();
        const prepared = engine.prepareEvaluator({ filters: [], outputs: [mapper.transform(value.expr)] });
        const instantiation = mapper.instantiation;

        return new RequestParameterValue(
          (request) => prepared.evaluate(parametersForRequest(request, instantiation))[0][0]
        );
      } else if (value.type == 'lookup') {
        const lookup = lookupToStage.get(value.lookup)!;
        return new LookupParameterValue(lookup, value.resultIndex);
      } else {
        throw new Error('TODO: intersection');
      }
    }

    function mapParameterValues(values: plan.ParameterValue[]) {
      return values.map(mapParameterValue);
    }

    for (const stage of lookupStages) {
      const mappedStage: PreparedExpandingLookup[] = [];
      mappedStages.push(mappedStage);

      for (const lookup of stage) {
        let resolved: PreparedExpandingLookup;

        if (lookup.type == 'parameter') {
          resolved = new ParameterIndexExpandingLookup(
            amountOfLookups++,
            input.preparedLookups.get(lookup.lookup)!,
            mapParameterValues(lookup.instantiation)
          );
        } else {
          // Create an expression like SELECT <output> FROM table_valued(<functionInputs>) WHERE <filters>
          const mapInputs = mapExternalDataToInstantiation();
          const fn: TableValuedFunction = {
            name: lookup.functionName,
            inputs: lookup.functionInputs.map((e) => mapInputs.transformWithoutTableValued(e))
          };
          const mapOutputs = new MapSourceVisitor<plan.ColumnSqlParameterValue, TableValuedFunctionOutput>(
            ({ column }) => ({
              function: fn,
              column
            })
          );

          const prepared = engine.prepareEvaluator({
            tableValuedFunctions: [fn],
            outputs: lookup.outputs.map((e) => visitExpr(mapOutputs, e, null)),
            filters: lookup.filters.map((e) => visitExpr(mapOutputs, e, null))
          });

          resolved = new TableValuedExpandingLookup(amountOfLookups++, (request) => [
            ...filterParameterRows(prepared.evaluate(parametersForRequest(request, mapInputs.instantiation)))
          ]);
        }

        lookupToStage.set(lookup, resolved);
        mappedStage.push(resolved);
      }
    }

    const rs = new ResultSet(amountOfLookups);
    return new RequestParameterEvaluators(stream, mappedStages, mapParameterValues(values), rs);
  }
}

/**
 * An internal exception thrown when no instantiation exists for a parameter.
 *
 * This is an exception to allow aborting the evaluator early.
 */
const uninstantiableException = Symbol.for('uninstantiable');

export type PreparedExpandingLookup = TableValuedExpandingLookup | ParameterIndexExpandingLookup;

abstract class BasePreparedExpandingLookup implements ResultSetElement {
  wasResolved = false;

  constructor(readonly resultSetIndex: number) {}

  abstract clone(): BasePreparedExpandingLookup;
}

class TableValuedExpandingLookup extends BasePreparedExpandingLookup {
  constructor(
    resultSetIndex: number,
    readonly read: (request: RequestParameters) => SqliteParameterValue[][]
  ) {
    super(resultSetIndex);
  }

  override clone(): TableValuedExpandingLookup {
    const lookup = new TableValuedExpandingLookup(this.resultSetIndex, this.read);
    lookup.wasResolved = this.wasResolved;
    return lookup;
  }
}

class ParameterIndexExpandingLookup extends BasePreparedExpandingLookup {
  constructor(
    resultSetIndex: number,
    readonly lookup: ParameterIndexLookupCreator,
    readonly instantiation: PreparedParameterValue[]
  ) {
    super(resultSetIndex);
  }

  override clone(): ParameterIndexExpandingLookup {
    const lookup = new ParameterIndexExpandingLookup(this.resultSetIndex, this.lookup, this.instantiation);
    lookup.wasResolved = this.wasResolved;
    return lookup;
  }
}

/**
 * A {@link plan.ParameterValue} that can be evaluated against request parameters.
 *
 * Additionally, this includes the `static` variant which allows partially instantiating parameters.
 */
export type PreparedParameterValue = RequestParameterValue | LookupParameterValue;

class RequestParameterValue {
  resolved: SqliteParameterValue | undefined;

  constructor(private readonly read: (request: RequestParameters) => SqliteValue) {}

  resolveWith({ request }: PartialInstantiationInput): SqliteParameterValue {
    if (this.resolved) return this.resolved;

    const value = this.read(request);
    if (isValidParameterValue(value)) {
      return (this.resolved = value);
    } else {
      throw uninstantiableException;
    }
  }

  clone(): RequestParameterValue {
    const clone = new RequestParameterValue(this.read);
    clone.resolved = this.resolved;
    return clone;
  }
}

class LookupParameterValue implements ResultSetColumn {
  constructor(
    readonly lookup: PreparedExpandingLookup,
    readonly outputIndex: number
  ) {}

  clone(): LookupParameterValue {
    return new LookupParameterValue(this.lookup, this.outputIndex);
  }
}

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
