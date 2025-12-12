import { ScopedParameterLookup, UnscopedParameterLookup } from '../BucketParameterQuerier.js';
import { SqlTools } from '../sql_filters.js';
import { checkJsonArray, OPERATOR_NOT } from '../sql_functions.js';
import { isParameterValueClause, isRowValueClause, SQLITE_TRUE, sqliteBool } from '../sql_support.js';
import { TablePattern } from '../TablePattern.js';
import {
  EvaluatedParametersResult,
  ParameterMatchClause,
  ParameterValueClause,
  RequestParameters,
  RowValueClause,
  SqliteJsonValue,
  SqliteRow,
  UnscopedEvaluatedParametersResult
} from '../types.js';
import { isJsonValue, normalizeParameterValue } from '../utils.js';

import { NodeLocation } from 'pgsql-ast-parser';
import { ParameterIndexLookupCreator, CreateSourceParams } from '../BucketSource.js';
import { HydrationState, ParameterLookupScope } from '../HydrationState.js';
import { SourceTableInterface } from '../SourceTableInterface.js';
import { SubqueryEvaluator } from './parameter.js';
import { cartesianProduct } from './utils.js';
import { StreamVariant } from './variant.js';

/**
 * An intermediate representation of a `WHERE` clause for stream queries.
 *
 * This representation is only used to compile expressions into the {@link StreamVariant}s representing streams in the
 * end.
 */
export abstract class FilterOperator {
  /**
   * The syntactic source creating this filter operator.
   */
  readonly location: NodeLocation | null;

  constructor(location: NodeLocation | null) {
    this.location = location;
  }

  abstract compile(context: StreamCompilationContext): void;

  compileVariants(streamName: string): StreamVariant[] {
    const initialVariant = new StreamVariant(0);
    const allVariants: StreamVariant[] = [initialVariant];

    this.compile({ streamName, currentVariant: initialVariant, allVariants, queryCounter: 0 });
    return allVariants;
  }

  /**
   * Transforms this filter operator to a DNF representation, a {@link Or} operator consisting of {@link And} operators
   * internally.
   *
   * We need to represent filters as a sum-of-products because that lets us turn each product into a
   * {@link StreamVariant}, simplifying the rest of the compilation.
   */
  toDisjunctiveNormalForm(tools: SqlTools): FilterOperator {
    // https://en.wikipedia.org/wiki/Disjunctive_normal_form#..._by_syntactic_means
    if (this instanceof Not) {
      const inner = this.operand;
      if (inner instanceof Not) {
        // !!x => x
        return inner.operand.toDisjunctiveNormalForm(tools);
      }

      if (inner instanceof EvaluateSimpleCondition) {
        // We can push the NOT into the simple condition to eliminate it.
        return inner.negate(tools);
      }

      let inverse;
      if (inner instanceof And) {
        inverse = Or; // !(a AND b) => (!a) OR (!b)
      } else if (inner instanceof Or) {
        inverse = And; // !(a OR B) => (!a) AND (!b)
      } else {
        return this;
      }

      const terms = inner.inner.map((e) => new Not(e.location, e).toDisjunctiveNormalForm(tools));
      return new inverse(inner.location, ...terms);
    } else if (this instanceof And) {
      const atomarFactors: FilterOperator[] = [];
      const orTerms: Or[] = [];

      for (const originalTerm of this.inner) {
        const normalized = originalTerm.toDisjunctiveNormalForm(tools);
        if (normalized instanceof And) {
          atomarFactors.push(...normalized.inner);
        } else if (normalized instanceof Or) {
          orTerms.push(normalized);
        } else {
          atomarFactors.push(normalized);
        }
      }

      if (orTerms.length == 0) {
        return new And(this.location, ...atomarFactors);
      }

      // If there's an OR term within the AND, apply the distributive law to turn the term into an ANDs within an outer
      // OR.
      // First, apply distributive law on orTerms to turn e.g. [[a, b], [c, d]] into [ac, ad, bc, bd].
      const multiplied = [...cartesianProduct(...orTerms.map((e) => e.inner))];

      // Then, combine those with the inner AND to turn e.g `A & (B | C) & D` into `(B & A & D) | (C & A & D)`.
      return new Or(
        this.location,
        ...multiplied.map((distributedTerms) => new And(this.location, ...distributedTerms, ...atomarFactors))
      );
    } else if (this instanceof Or) {
      // Already in DNF, but we want to simplify `(A OR B) OR C` into `A OR B OR C` if necessary.
      return new Or(
        this.location,
        ...this.inner.flatMap((e) => {
          const normalized = e.toDisjunctiveNormalForm(tools);
          return normalized instanceof Or ? normalized.inner : [normalized];
        })
      );
    }

    return this;
  }

  /**
   * Checks that this filter is valid, meaning that no `NOT` operators appear before subqueries or other operators that
   * don't support negations.
   */
  isValid(tools: SqlTools): boolean {
    let hasError = false;

    for (const literal of this.visitLiterals()) {
      if (literal instanceof Not) {
        tools.error('Negations are not allowed here', literal.location ?? undefined);
        hasError = true;
      }
    }

    return !hasError;
  }

  private *visitLiterals(): Generator<FilterOperator> {
    if (this instanceof Or || this instanceof And) {
      for (const term of this.inner) {
        yield* term.visitLiterals();
      }
    } else {
      yield this;
    }
  }
}

interface StreamCompilationContext {
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
  readonly operand: FilterOperator;

  constructor(location: NodeLocation | null, operand: FilterOperator) {
    super(location);
    this.operand = operand;
  }

  compile(): void {
    throw Error('Not operator should have been desugared before compilation.');
  }
}

export class And extends FilterOperator {
  readonly inner: FilterOperator[];

  constructor(location: NodeLocation | null, ...operators: FilterOperator[]) {
    super(location);
    this.inner = operators;
  }

  compile(context: StreamCompilationContext) {
    // Within a variant, added conditions and parameters form a conjunction.
    for (const condition of this.inner) {
      condition.compile(context);
    }
  }
}

export class Or extends FilterOperator {
  readonly inner: FilterOperator[];

  constructor(location: NodeLocation | null, ...operators: FilterOperator[]) {
    super(location);
    this.inner = operators;
  }

  compile(context: StreamCompilationContext): void {
    throw Error('An OR operator can only appear at the highest level of the filter chain');
  }

  compileVariants(streamName: string): StreamVariant[] {
    const allVariants: StreamVariant[] = [];
    const context = {
      streamName,
      currentVariant: undefined as StreamVariant | undefined,
      allVariants,
      queryCounter: 0
    };

    for (const condition of this.inner) {
      const variant = new StreamVariant(allVariants.length);
      allVariants.push(variant);

      context.currentVariant = variant;
      condition.compile(context as StreamCompilationContext);
    }

    return allVariants;
  }
}

export class Subquery {
  private table: TablePattern;
  readonly column: RowValueClause;
  private filter: FilterOperator;

  constructor(
    table: TablePattern,
    column: RowValueClause,
    filter: CompareRowValueWithStreamParameter | EvaluateSimpleCondition
  ) {
    this.table = table;
    this.column = column;
    this.filter = filter;
  }

  addFilter(filter: CompareRowValueWithStreamParameter | EvaluateSimpleCondition) {
    this.filter = new And(this.filter.location, this.filter, filter);
  }

  compileEvaluator(context: StreamCompilationContext): SubqueryEvaluator {
    const innerVariants = this.filter.compileVariants(context.streamName).map((variant) => {
      const id = context.queryCounter.toString();
      context.queryCounter++;

      if (variant.parameters.length == 0) {
        throw new Error('Unsupported subquery without parameter, must depend on request parameters');
      }

      return [variant, id] satisfies [StreamVariant, string];
    });

    const column = this.column;

    let lookupCreators: ParameterIndexLookupCreator[] = [];
    let lookupsForRequest: ((
      hydrationState: HydrationState
    ) => (parameters: RequestParameters) => ScopedParameterLookup[])[] = [];

    for (let [variant, id] of innerVariants) {
      const source = new SubqueryParameterLookupSource(this.table, column, variant, id, context.streamName);
      lookupCreators.push(source);
      lookupsForRequest.push((hydrationState: HydrationState) => {
        const scope = hydrationState.getParameterIndexLookupScope(source);
        return (parameters: RequestParameters) => {
          const lookups: ScopedParameterLookup[] = [];
          const instantiations = variant.findStaticInstantiations(parameters);
          for (const instantiation of instantiations) {
            lookups.push(ScopedParameterLookup.normalized(scope, UnscopedParameterLookup.normalized(instantiation)));
          }
          return lookups;
        };
      });
    }

    const evaluator: SubqueryEvaluator = {
      parameterTable: this.table,
      indexLookupCreators() {
        return lookupCreators;
      },
      hydrateLookupsForRequest(hydrationState: HydrationState) {
        const hydrated = lookupsForRequest.map((fn) => fn(hydrationState));
        return (parameters: RequestParameters) => {
          const lookups: ScopedParameterLookup[] = [];
          for (const getLookups of hydrated) {
            lookups.push(...getLookups(parameters));
          }
          return lookups;
        };
      }
    };

    context.currentVariant.subqueries.push(evaluator);
    return evaluator;
  }
}

export type ScalarExpression = RowValueClause | ParameterValueClause;

/**
 * A filter that matches when row values are contained in a subquery depending on parameter values.
 *
 * Examples:
 *
 *  - `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user())`
 *
 *  An `IN` operator can also be handled without requiring a subquery, those don't create {@link InOperator}s:
 *
 *  - `SELECT * FROM comments WHERE id IN request.params('id')` ({@link CompareRowValueWithStreamParameter})
 *  - `SELECT * FROM comments WHERE request.user() IN comments.tagged_users` ({@link CompareRowValueWithStreamParameter})
 *
 * Finally, it's also possible to `IN` operators for parameter values:
 *
 *  - `SELECT * FROM comments WHERE request.user_id() IN (SELECT * FROM users WHERE is_admin)`.
 *
 * However, these are not represented as {@link InOperator}s in the filter graph. Instead, we push the
 * `request.user_id()` filter into the subquery and then compile the operator into a {@link ExistsOperator}.
 */
export class InOperator extends FilterOperator {
  private left: RowValueClause;
  private right: Subquery;

  constructor(location: NodeLocation | null, left: RowValueClause, right: Subquery) {
    super(location);
    this.left = left;
    this.right = right;
  }

  compile(context: StreamCompilationContext): void {
    const subqueryEvaluator = this.right.compileEvaluator(context);
    const filter = this.left;

    // Something like `SELECT * FROM comments WHERE issue_id IN (SELECT id FROM issue WHERE owner_id = request.user())`
    // This groups rows into buckets identified by comments.issue_id, which happens in filterRow.
    // When a user connects, we need to resolve all the issue ids they own. This happens with an indirection:
    //  1. In the subquery evaluator, we create an index from owner_id to issue ids.
    //  2. When we have users, we use that index to find issue ids dynamically, with which we can build the buckets
    //     to sync.
    context.currentVariant.parameters.push({
      lookup: {
        type: 'in',
        subquery: subqueryEvaluator
      },
      filterRow(options) {
        const tables = { [options.sourceTable.name]: options.record };
        const value = filter.evaluate(tables);
        if (isJsonValue(value)) {
          return [normalizeParameterValue(value)];
        } else {
          return [];
        }
      }
    });
  }
}

/**
 * An operator of the form `<left> && <right>`, where `right` is a subqery.
 */
export class OverlapOperator extends FilterOperator {
  private left: RowValueClause;
  private right: Subquery;

  constructor(location: NodeLocation | null, left: RowValueClause, right: Subquery) {
    super(location);
    this.left = left;
    this.right = right;
  }

  compile(context: StreamCompilationContext): void {
    const subqueryEvaluator = this.right.compileEvaluator(context);
    const filter = this.left;

    context.currentVariant.parameters.push({
      lookup: {
        type: 'overlap',
        subquery: subqueryEvaluator
      },
      filterRow(options) {
        const tables = { [options.sourceTable.name]: options.record };
        const value = filter.evaluate(tables);
        if (value == null) {
          return [];
        }

        const parsed = checkJsonArray(value, 'Left side of && must evaluate to an array');
        return parsed.map(normalizeParameterValue);
      }
    });
  }
}

/**
 * A filter that matches when _something_ exists in a subquery.
 *
 * This is exclusively used to compile `IN` operators where the left operand does not depend on row values, e.g.
 *
 *  - `SELECT * FROM comments WHERE request.user_id() IN (SELECT id FROM users WHERE is_admin)`
 *
 * These queries are desugared to something that is semantically equivalent to
 * `WHERE EXISTS (SELECT _ FROM users WHERE is_admin AND id = request.user_id())`.
 */
export class ExistsOperator extends FilterOperator {
  private subquery: Subquery;

  constructor(location: NodeLocation | null, subquery: Subquery) {
    super(location);
    this.subquery = subquery;
  }

  compile(context: StreamCompilationContext): void {
    const subqueryEvaluator = this.subquery.compileEvaluator(context);

    context.currentVariant.requestFilters.push({
      type: 'dynamic',
      subquery: subqueryEvaluator,
      matches(_, results) {
        return results.length > 0;
      }
    });
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

  constructor(location: NodeLocation | null, match: ParameterMatchClause) {
    super(location);
    this.match = match;
  }

  compile(context: StreamCompilationContext): void {
    const match = this.match;

    for (const filters of match.inputParameters) {
      context.currentVariant.parameters.push({
        lookup: {
          type: 'static',
          fromRequest(parameters) {
            const value = filters.parametersToLookupValue(parameters);
            if (filters.expands) {
              if (typeof value != 'string') {
                return [];
              }
              let values: SqliteJsonValue[] = JSON.parse(value);
              if (!Array.isArray(values)) {
                return [];
              }

              return values;
            } else {
              return [value];
            }
          }
        },
        filterRow(options) {
          const tables = { [options.sourceTable.name]: options.record };
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

  constructor(location: NodeLocation | null, expression: ScalarExpression) {
    super(location);
    this.expression = expression;
  }

  compile(context: StreamCompilationContext): void {
    if (isRowValueClause(this.expression)) {
      const filter = this.expression;

      context.currentVariant.additionalRowFilters.push((row) => {
        return sqliteBool(filter.evaluate({ [row.sourceTable.name]: row.record })) == SQLITE_TRUE;
      });
    } else if (isParameterValueClause(this.expression)) {
      const filter = this.expression;

      context.currentVariant.requestFilters.push({
        type: 'static',
        matches(params) {
          return sqliteBool(filter.lookupParameterValue(params)) == SQLITE_TRUE;
        }
      });
    } else {
      const _: never = this.expression; // Exhaustive check
    }
  }

  negate(tools: SqlTools): EvaluateSimpleCondition {
    return new EvaluateSimpleCondition(
      this.location,
      tools.composeFunction(OPERATOR_NOT, [this.expression], []) as ScalarExpression
    );
  }
}

export class SubqueryParameterLookupSource implements ParameterIndexLookupCreator {
  constructor(
    private parameterTable: TablePattern,
    private column: RowValueClause,
    private innerVariant: StreamVariant,
    private defaultQueryId: string,
    private streamName: string
  ) {}

  public get defaultLookupScope() {
    return {
      lookupName: this.streamName,
      queryId: this.defaultQueryId
    };
  }

  getSourceTables(): Set<TablePattern> {
    let result = new Set<TablePattern>();
    result.add(this.parameterTable);
    return result;
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
  evaluateParameterRow(sourceTable: SourceTableInterface, row: SqliteRow): UnscopedEvaluatedParametersResult[] {
    if (this.parameterTable.matches(sourceTable)) {
      // Theoretically we're doing duplicate work by doing this for each innerVariant in a subquery.
      // In practice, we don't have more than one innerVariant per subquery right now, so this is fine.
      const value = this.column.evaluate({ [sourceTable.name]: row });
      if (!isJsonValue(value)) {
        return [];
      }

      const lookups: UnscopedParameterLookup[] = [];
      for (const instantiation of this.innerVariant.instantiationsForRow({ sourceTable, record: row })) {
        lookups.push(UnscopedParameterLookup.normalized(instantiation));
      }

      // The row of the subquery. Since we only support subqueries with a single column, we unconditionally name the
      // column `result` for simplicity.
      const resultRow = { result: value };

      return lookups.map((l) => ({
        lookup: l,
        bucketParameters: [resultRow]
      }));
    }
    return [];
  }

  tableSyncsParameters(table: SourceTableInterface): boolean {
    return this.parameterTable.matches(table);
  }
}
