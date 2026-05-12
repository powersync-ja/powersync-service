import { ParameterLookupSource, ScopedParameterLookup, UnscopedParameterLookup } from '../../BucketParameterQuerier.js';
import { ParameterIndexLookupCreator } from '../../BucketSource.js';
import { HydrationState } from '../../HydrationState.js';
import { RequestParameters, SqliteParameterValue, SqliteValue } from '../../types.js';
import { isValidParameterValue } from '../../utils.js';
import {
  mapExternalDataToInstantiation,
  TableValuedFunction,
  TableValuedFunctionOutput
} from '../engine/scalar_expression_engine.js';
import { MapSourceVisitor, visitExpr } from '../expression_visitor.js';
import * as plan from '../plan.js';
import { StreamInput } from './bucket_source.js';
import { PreparedParameterIndexLookupCreator } from './parameter_index_lookup_creator.js';

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
    readonly parameterValues: PreparedParameterValue[]
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
    function cloneValue(value: PreparedParameterValue): PreparedParameterValue {
      switch (value.type) {
        case 'intersection':
          return { type: 'intersection', values: value.values.map(cloneValue) };
        case 'request':
        case 'lookup':
        case 'cached':
          return value;
      }
    }

    function cloneLookup(lookup: PreparedExpandingLookup): PreparedExpandingLookup {
      switch (lookup.type) {
        case 'parameter':
          // We need to clone the instantiation array as well.
          return { type: 'parameter', lookup: lookup.lookup, instantiation: lookup.instantiation.map(cloneValue) };
        case 'table_valued':
        case 'cached':
          return lookup;
      }
    }

    return new RequestParameterEvaluators(
      this.stream,
      this.lookupStages.map((stage) => stage.map(cloneLookup)),
      this.parameterValues.map(cloneValue)
    );
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
    const helper = new PartialInstantiator(input, this);

    this.lookupStages.forEach((stage, stageIndex) => {
      stage.forEach((_, indexInStage) => helper.expandingLookupSync(stageIndex, indexInStage));
    });

    return helper.tryResolveInstantiation(this.parameterValues)?.map(withoutProvenance);
  }

  /**
   * Resolves and caches all lookup stages and parameter values.
   *
   * Because this needs to lookup parameter indexes, it is asynchronous.
   */
  async instantiate(input: InstantiationInput): Promise<SqliteParameterValue[][]> {
    const helper = new FullInstantiator(input, this);

    for (let i = 0; i < this.lookupStages.length; i++) {
      // Within a stage, we can resolve lookups concurrently.
      await Promise.all(this.lookupStages[i].map((_, j) => helper.expandingLookup(i, j)));
    }

    // At this point, all lookups have been resolved and we can synchronously evaluate parameters which might depend on
    // those lookups.
    return helper.resolveInstantiation(this.parameterValues).map(withoutProvenance);
  }

  /**
   * Prepares evaluators for a description of parameter values obtained from a compiled querier in the sync plan.
   *
   * @param stream Used to show the name of the stream for debugging purposes.
   * @param lookupStages The {@link plan.StreamQuerier.lookupStages} of the querier to compile.
   * @param values The {@link plan.StreamQuerier.sourceInstantiation} of the querier to compile.
   * @param input Access to bucket and parameter sources generated for buckets and parameter lookups referenced by the
   * querier.
   */
  static prepare(
    stream: plan.StreamOptions,
    lookupStages: plan.ExpandingLookup[][],
    values: plan.ParameterValue[],
    input: StreamInput
  ) {
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
      mappedStages.push(mappedStage);

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
            inputs: lookup.functionInputs.map((e) => mapInputs.transformWithoutTableValued(e))
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

    return new RequestParameterEvaluators(stream, mappedStages, mapParameterValues(values));
  }
}

class PartialInstantiator<I extends PartialInstantiationInput = PartialInstantiationInput> {
  constructor(
    protected readonly input: I,
    protected readonly evaluators: RequestParameterEvaluators
  ) {}

  tryResolveInstantiation(params: PreparedParameterValue[]): ParameterValueWithRow[][] | undefined {
    const stages = this.evaluators.lookupStages;
    let hasUninstantiatedStage = false;
    for (let stageIndex = 0; stageIndex < stages.length; stageIndex++) {
      const stage = stages[stageIndex];
      for (let indexInStage = 0; indexInStage < stage.length; indexInStage++) {
        const resolvedValues = this.expandingLookupSync(stageIndex, indexInStage);
        if (resolvedValues == null) {
          // Requires an asynchronous lookup to instantiate.
          hasUninstantiatedStage = true;
          continue;
        }

        if (resolvedValues.length == 0) {
          // Empty lookup stages make the entire graph uninstantiable, even if they're not used as a parameter. The
          // reason for that is that queries like `WHERE 'static_value' IN (SELECT name FROM users WHERE id = auth.user_id())`
          // are implemented as lookup stages, so we can't ignore them.
          // Note that there is no construct like `OR` in a querier lookup (those always get compiled into separate
          // queries), so any stage being empty guarantees that everything is uninstantiable.
          return [];
        }
      }
    }

    if (hasUninstantiatedStage) {
      return undefined;
    }

    // If we got to this point, all stages have been resolved. So we can resolve parameters without further async work.
    return [...this.resolveInputs(params)];
  }

  protected *resolveInputs(params: PreparedParameterValue[]): Generator<ParameterValueWithRow[]> {
    const parameterValues = params.map((_, index) => {
      const cached = this.parameterSync(params, index);
      if (cached == null) {
        // This method is only called for inputs from an earlier stage, which should have been resolved at this point.
        throw new Error('Should have been able to resolve parameter from earlier stage synchronously.');
      }
      return cached;
    });

    yield* mergeValueCombinations(parameterValues);
  }

  /**
   * If possible, evaluates an element in an array of parameter values and replaces the parameter with a marker
   * indicating it as cached.
   */
  parameterSync(parent: PreparedParameterValue[], index: number): ParameterValueWithRow[] | undefined {
    const current = parent[index];
    if (current.type === 'cached') {
      return current.values;
    } else if (current.type === 'intersection') {
      const columns: ParameterValueWithRow[][] = [];
      for (let i = 0; i < current.values.length; i++) {
        const evaluated = this.parameterSync(current.values, i);
        if (evaluated == null) {
          return undefined; // Can't evaluate sub-parameter
        }
        columns.push(evaluated);
      }

      // For the most part, this just needs to find an intersection of values present in all columns. It gets more
      // complicated for rows with provenance, however. For those. we need to ensure we find an intersection of values
      // with compatible source rows. For example, consider an intersection of the same parameter lookup with columns
      // `c1` and `c2`, and assume that we had the following rows:
      //
      //   1. Row {c1: 'a', c2: 'a'}
      //   2. Row {c1: 'a', c2: 'b'}
      //
      // The intersection of this has one value: `a`, with a provenance of Row 1.  To achieve this, we re-create rows
      // by tracking a canonical value per row. If we see another value in the same row, we know that row can't
      // contribute to the intersection because it has different values for `c1` and `c2`.
      const poison = Symbol('poison');
      const valuesByResultSet = new Map<symbol, Map<number, SqliteParameterValue | typeof poison>>();
      const completedRows = new Map<symbol, Set<number>>();
      function markRowAsCompleted(resultSet: symbol, rowid: number) {
        let rowids = completedRows.get(resultSet);
        if (rowids == null) {
          rowids = new Set();
          completedRows.set(resultSet, rowids);
        }

        rowids.add(rowid);
      }

      // Eliminate rows with conflicting values.
      for (const column of columns) {
        nextValue: for (const value of column) {
          for (const row of value.provenance) {
            let forResultSet = valuesByResultSet.get(row.resultSet);
            if (forResultSet == null) {
              forResultSet = new Map();
              valuesByResultSet.set(row.resultSet, forResultSet);
            }

            const existingValue = forResultSet.get(row.row);
            if (existingValue != null && existingValue != value.value) {
              forResultSet.set(row.row, poison);
              markRowAsCompleted(row.resultSet, row.row);
              continue nextValue;
            } else {
              forResultSet.set(row.row, value.value);
            }
          }
        }
      }

      function shouldSkipValue(value: ParameterValueWithRow): boolean {
        for (const { resultSet, row } of value.provenance) {
          const ignoredRowIds = completedRows.get(resultSet);
          if (ignoredRowIds?.has(row)) return true;

          const valuesForResultSet = valuesByResultSet.get(resultSet);
          if (valuesForResultSet == null) continue;

          if (valuesForResultSet.get(row) != value.value) return true;
        }

        return false;
      }

      let intersection: Map<SqliteParameterValue, VirtualSourceRow[][]> | null = null;
      for (const column of columns) {
        if (intersection == null) {
          intersection = new Map();

          for (const value of column) {
            if (shouldSkipValue(value)) continue;

            const existing = intersection.get(value.value);
            if (existing != null) {
              existing.push(value.provenance);
            } else {
              intersection.set(value.value, [value.provenance]);
            }

            for (const { resultSet, row } of value.provenance) {
              // Any other value derived from this row must have the same value (otherwise we would have eliminated it).
              // So we don't have to consider this row again.
              markRowAsCompleted(resultSet, row);
            }
          }
        } else {
          const unmatchedValues = new Set(intersection.keys());

          for (const value of column) {
            const existing = intersection.get(value.value);
            if (existing == null) {
              // Value not in intersection, ignore.
            } else {
              unmatchedValues.delete(value.value);

              if (!shouldSkipValue(value)) {
                // An intersection value is derived from all inputs, so we track them all as provenance.
                existing.push(value.provenance);
              }
            }
          }

          for (const unmatched of unmatchedValues) {
            // Values in intersection before, but not in evaluated
            intersection.delete(unmatched);
          }
        }

        if (intersection!.size == 0) {
          // Empty intersection, we don't even need to evaluate the rest.
          break;
        }
      }

      let values: ParameterValueWithRow[] = [];
      if (intersection) {
        intersection.forEach((provenances, value) => {
          for (const provenance of provenances) {
            values.push({ value, provenance });
          }
        });
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
      const values: ParameterValueWithRow[] = isValidParameterValue(value)
        ? [
            {
              value,
              provenance: []
            }
          ]
        : [];

      parent[index] = { type: 'cached', values };
      return values;
    }

    return undefined;
  }

  expandingLookupSync(stage: number, index: number): ParameterValueWithRow[][] | undefined {
    const lookup = this.evaluators.lookupStages[stage][index];
    if (lookup.type == 'table_valued') {
      // We can evaluate this table-valued function already.
      const resultSetMarker = Symbol();
      const values = lookup.read(this.input.request).map((values, rowid) => {
        const provenance: [VirtualSourceRow] = [{ resultSet: resultSetMarker, row: rowid }];
        return values.map((value) => ({ value, provenance }) satisfies ParameterValueWithRow);
      });

      this.evaluators.lookupStages[stage][index] = { type: 'cached', values };
      return values;
    } else if (lookup.type == 'cached') {
      return lookup.values;
    }

    return undefined;
  }
}

class FullInstantiator extends PartialInstantiator<InstantiationInput> {
  resolveInstantiation(params: PreparedParameterValue[]): ParameterValueWithRow[][] {
    const resolved = this.tryResolveInstantiation(params);
    if (resolved == null) {
      throw new Error('internal error: Should have been able to resolve instantiation after instantiating stages.');
    }

    return resolved;
  }

  async expandingLookup(stage: number, index: number): Promise<ParameterValueWithRow[][]> {
    const lookup = this.evaluators.lookupStages[stage][index];
    if (lookup.type == 'parameter') {
      const scope = this.input.hydrationState.getParameterIndexLookupScope(lookup.lookup);
      const resolvedLookup = lookup.lookup as PreparedParameterIndexLookupCreator;
      const lookupsToProvenance = new Map<ScopedParameterLookup, { origin: VirtualSourceRow[]; symbol: symbol }>();

      for (const values of this.resolveInputs(lookup.instantiation)) {
        const provenance: VirtualSourceRow[] = [];
        for (const value of values) {
          provenance.push(...value.provenance);
        }

        const lookup = ScopedParameterLookup.normalized(
          scope,
          UnscopedParameterLookup.normalized(withoutProvenance(values))
        );
        lookupsToProvenance.set(lookup, { origin: provenance, symbol: Symbol(`lookup ${stage}.${index}`) });
      }

      const outputs = await this.input.source.getParameterSets(
        [...lookupsToProvenance.keys()],
        `Stream ${this.evaluators.stream.name} evaluating parameter on ${resolvedLookup.sourceTable.name}`
      );

      // Stream parameters generate an output row like {0: <expr>, 1: <expr>, ...}.
      const values = outputs.flatMap(({ lookup, rows }) => {
        const { symbol, origin } = lookupsToProvenance.get(lookup)!;

        return rows.map((row, rowid) => {
          const length = Object.entries(row).length;
          const asArray: ParameterValueWithRow[] = [];

          for (let i = 0; i < length; i++) {
            const value = row[i.toString()] as SqliteParameterValue;
            asArray.push({
              value,
              // Note: Not tracking provenance for parameters with just a single output is purely a performance
              // optimization. If there's just a single value, we don't need to correlate it with other columns in the
              // row. Not adding provenance saves some work in mergeValueCombinations.
              provenance: length > 1 ? [...origin, { resultSet: symbol, row: rowid }] : origin
            });
          }
          return asArray;
        });
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
  | { type: 'cached'; values: ParameterValueWithRow[][] };

/**
 * A {@link plan.ParameterValue} that can be evaluated against request parameters.
 *
 * Additionally, this includes the `cached` variant which allows partially instantiating parameters.
 */
export type PreparedParameterValue =
  | { type: 'request'; read(request: RequestParameters): SqliteValue }
  | { type: 'lookup'; lookup: { stage: number; index: number }; resultIndex: number }
  | { type: 'intersection'; values: PreparedParameterValue[] }
  | { type: 'cached'; values: ParameterValueWithRow[] };

interface ParameterValueWithRow {
  value: SqliteParameterValue;

  /**
   * Information on how this value was resolved.
   *
   * We track how a parameter value was resolved to be able to merge parameters correctly. A Sync Stream with multiple
   * independent parameters generates their cartesian product as buckets. When multiple parameters are resolved from the
   * same row though, we can't use the full cartesian product. As an example, consider this stream:
   *
   * ```sql
   * SELECT products.* FROM products, stores
   *   WHERE stores.name = products.store_name
   *     AND stores.region = products.region
   *     AND stores.id = subscription.parameter('store');
   * ```
   *
   * Here, the bucket shape consists of two parameters (`store_name` and `region`). But since they're derived from the
   * same `stores` row, we can't combine them freely. For each parameter, `store_name` and `region` must come from the
   * same row. Here, the `provenance` would have a single entry and both parameters would have the same
   * {@link VirtualSourceRow.resultSet}.
   *
   * For static values, such as constants or scalar values derived from request parameters, this array is empty. It's
   * also possible for this to contain more than one entry, though:
   *
   *  1. For intersection values, we track the provenance of all input values.
   *  2. It's possible to nest parameters. For instance, in the query `SELECT data.* FROM data, a, b WHERE a.a = data.a
   *     AND b.b = a.b AND data.c = b.c`, we have to parameters (`a` and `c`). `a` can be resolved from a lookup in
   *     table `a`, but we need to go through a second lookup to resolve `c`. Here, we can only combine value `c` with
   *     value `a` if the two were derived from the same row in `a`. So, the row `b` derived through `a` would include
   *     provenance elements of row `a` here.
   */
  provenance: VirtualSourceRow[];
}

interface VirtualSourceRow {
  /**
   * An opaque identifier for the result set this row was derived from.
   */
  resultSet: symbol;
  /**
   * A number uniquely identifying this row in its result set.
   */
  row: number;
}

function withoutProvenance(source: ParameterValueWithRow[]): SqliteParameterValue[] {
  return source.map(({ value }) => value);
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

/**
 * Builds a cartesian product of parameter instantiations, taking {@link ParameterValueWithRow.provenance} into account
 * to avoid combining values from different rows of the same result set.
 *
 * @param valuesByParameter An array containing possible instantiations for each parameter.
 * @returns All allowed instantiations.
 */
function* mergeValueCombinations(valuesByParameter: ParameterValueWithRow[][]): Generator<ParameterValueWithRow[]> {
  // Partial backtracking results, the current instantiation is fixed for 0..nextParameter in generateCombinations.
  const partialResults = new Array(valuesByParameter.length);
  // A map from result sets to roows used in the partial instantiation.
  const usedRows = new Map<symbol, number>();

  function installRowIfNoConflict(value: ParameterValueWithRow): [boolean, symbol[]] {
    const addedResultSets: symbol[] = [];

    for (const origin of value.provenance) {
      if (origin != null) {
        const { resultSet, row } = origin;
        const existingRow = usedRows.get(resultSet);
        if (existingRow === undefined) {
          addedResultSets.push(resultSet);
          usedRows.set(resultSet, row);
        } else if (existingRow == row) {
          continue;
        } else {
          // The current instantiation already constains a value from the same result set but derived from a different
          // row. So we must ignore this parameter value.
          return [false, addedResultSets];
        }
      }
    }

    return [true, addedResultSets];
  }

  function uninstallResultSets(resultSets: symbol[]) {
    for (const rs of resultSets) {
      usedRows.delete(rs);
    }
  }

  function* generateCombinations(nextParameter: number): Generator<ParameterValueWithRow[]> {
    if (nextParameter >= valuesByParameter.length) {
      yield [...partialResults];
      return;
    }

    const availableValues = valuesByParameter[nextParameter];
    for (const available of availableValues) {
      const [canUse, addedResultSets] = installRowIfNoConflict(available);
      if (canUse) {
        partialResults[nextParameter] = available;
        yield* generateCombinations(nextParameter + 1);
      }

      uninstallResultSets(addedResultSets);
    }
  }

  yield* generateCombinations(0);
}
