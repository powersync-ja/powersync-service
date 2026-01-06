import {
  astMapper,
  BinaryOperator,
  Expr,
  ExprParameter,
  From,
  nil,
  NodeLocation,
  PGNode,
  SelectedColumn,
  Statement
} from 'pgsql-ast-parser';
import { PhysicalSourceResultSet, SourceResultSet, SyntacticResultSetSource } from '../ir/table.js';
import { ColumnSource, ExpressionColumnSource, StarColumnSource } from '../ir/rows.js';
import {
  ColumnInRow,
  ConnectionParameter,
  ConnectionParameterSource,
  ExpressionInput,
  SyncExpression
} from '../ir/expression.js';
import {
  BaseTerm,
  EqualsClause,
  InvalidExpressionError,
  Or,
  And,
  RowExpression,
  SingleDependencyExpression
} from '../ir/filter.js';
import { expandNodeLocations } from '../errors.js';
import { cartesianProduct } from '../streams/utils.js';

/**
 * A parsed stream query in its canonical form.
 *
 * The canonical form of stream queries is as follows:
 *
 * ```SQL
 * SELECT <expr>* FROM sourceTable a
 *   LEFT OUTER JOIN table2 b
 *   -- ... additional joins
 * WHERE <expr>
 * ```
 *
 * Expressions in the result set must all refer to columns in the {@link sourceTable}.
 *
 * Additionally, the where clause is in disjunctive normal form. Inner terms are either scalar expressions only
 * depending on data from a single source result set, or match clauses.
 *
 * Subqueries are not allowed in the canonical form. Instead, they are lowered into outer joins. So e.g. the query:
 *
 * ```SQL
 * SELECT * FROM users WHERE id IN (SELECT user FROM org WHERE id = auth.user_id())
 * ```
 *
 * would get lowered to:
 * ```SQL
 * SELECT * FROM users tmp0
 *   LEFT OUTER JOIN org tmp1
 * WHERE tmp0.id = org.user AND org.id = (:auth_token) ->> '$.sub';
 * ```
 */
interface ParsedStreamQuery {
  resultColumns: ColumnSource[];
  sourceTable: PhysicalSourceResultSet;
  joined: SourceResultSet[];
  where: Or;
}

interface ParsingErrorListener {
  report(message: string, location: NodeLocation | PGNode): void;
}

class SqlScope {
  readonly parent?: SqlScope;
  private readonly nameToResultSet = new Map<string, SyntacticResultSetSource>();

  constructor(options: { parent?: SqlScope }) {
    this.parent = options.parent;
  }

  get resultSets(): SyntacticResultSetSource[] {
    return [...this.nameToResultSet.values()];
  }

  registerResultSet(errors: ParsingErrorListener, name: string, source: SyntacticResultSetSource) {
    const lower = name.toLowerCase();
    if (this.nameToResultSet.has(lower)) {
      errors.report(`Table with name ${name} already exists in scope`, source.origin);
      return;
    } else {
      this.nameToResultSet.set(lower, source);
    }
  }

  resolveResultSetForReference(name: string): SyntacticResultSetSource | undefined {
    return this.nameToResultSet.get(name.toLowerCase());
  }
}

export class StreamQueryParser {
  // Note: This is not the same as SqlScope since some result sets are inlined from CTEs or subqueries. These are not in
  // scope, but we still add them here to correctly track dependencies.
  private readonly resultSets = new Map<SyntacticResultSetSource, SourceResultSet>();
  private readonly resultColumns: ColumnSource[] = [];
  private where: [SqlScope, Expr][] = [];

  /** The result set for which rows are synced. Set when analyzing result columns. */
  private primaryResultSet?: PhysicalSourceResultSet;

  constructor(readonly errors: ParsingErrorListener) {}

  parse(stmt: Statement): ParsedStreamQuery | null {
    if (this.processAst(stmt, { forSubquery: false })) {
      if (this.primaryResultSet == null) {
        return null;
      }

      let where: Or;
      if (this.where) {
        where = this.compileFilterClause(this.where);
      } else {
        const emptyAnd: And = { terms: [] };
        where = { terms: [emptyAnd] };
      }

      const joined: SourceResultSet[] = [];
      for (const source of this.resultSets.values()) {
        if (source != this.primaryResultSet) {
          joined.push(source);
        }
      }

      return {
        resultColumns: this.resultColumns,
        sourceTable: this.primaryResultSet,
        joined,
        where: where
      };
    } else {
      return null;
    }
  }

  /**
   * Process the AST of a statement, returning whether it's close enough to a valid select statement to be supported for
   * sync streams (allowing us to process an invalid statement further to perhaps collect more errors).
   */
  private processAst(node: Statement, options: { forSubquery: boolean; parentScope?: SqlScope }): boolean {
    if (node.type != 'select') {
      // TODO: Support with statements
      this.errors.report('Expected a SELECT statement', node);
      return false;
    }

    const scope = new SqlScope({ parent: options.parentScope });
    node.from?.forEach((f) => this.processFrom(scope, f));
    if (node.where) {
      this.where.push([scope, node.where]);
    }

    if (!options.forSubquery && node.columns) {
      this.processResultColumns(scope, node.columns);
    }

    this.warnUnsupported(node.groupBy, 'GROUP BY');
    this.warnUnsupported(node.having, 'GROUP BY');
    this.warnUnsupported(node.limit, 'LIMIT');
    this.warnUnsupported(node.orderBy, 'ORDER BY');
    this.warnUnsupported(node.for, 'FOR');
    this.warnUnsupported(node.skip, 'SKIP');

    return true;
  }

  private processFrom(scope: SqlScope, from: From) {
    let handled = false;
    if (from.type == 'table') {
      const source = new SyntacticResultSetSource(from.name);
      const resultSet = new PhysicalSourceResultSet(from.name.name, source);
      scope.registerResultSet(this.errors, from.name.alias ?? from.name.name, source);
      this.resultSets.set(source, resultSet);
      handled = true;
      return;
    } else if (from.type == 'call') {
      // TODO: Resolve table-valued functions
    } else if (from.type == 'statement') {
      // TODO: We could technically allow selecting from subqueries once we support CTEs.
    }

    const join = from.join;
    if (join && handled) {
      if (join.type == 'FULL JOIN') {
        // We really only support inner joins, but because there is a constraint that every joined table must have a
        // filter created through `=` or `IN` (which both exclude nulls), all join types are equivalent to inner joins
        // anyway.
        this.warnUnsupported(join, 'FULL JOIN');
      }

      if (join.using) {
        // We don't support using because interpreting it depends on the schema. Users should spell it out explicitly
        // using an ON clause.
        this.warnUnsupported(join.using, 'USING');
      }

      if (join.on) {
        this.where.push([scope, join.on]);
      }
    }

    if (!handled) {
      this.warnUnsupported(from, 'This source');
    }
  }

  private processResultColumns(scope: SqlScope, columns: SelectedColumn[]) {
    const selectsFrom = (source: SourceResultSet, node: PGNode) => {
      if (source instanceof PhysicalSourceResultSet) {
        if (this.primaryResultSet == null) {
          this.primaryResultSet = source;
        } else if (this.primaryResultSet !== source) {
          this.errors.report(
            `Sync streams can only select from a single table, and this one already selects from '${this.primaryResultSet.tablePattern}'.`,
            node
          );
        }
      } else {
        this.errors.report('Sync streams can only selecting from actual tables', node);
      }
    };

    for (const column of columns) {
      if (column.expr.type == 'ref' && column.expr.name == '*') {
        const resolved = this.resolveTableName(scope, column.expr, column.expr.table?.name);
        if (resolved != null) {
          selectsFrom(resolved, column.expr);
        }

        this.resultColumns.push(StarColumnSource.instance);
      } else {
        const expr = this.parseExpression(scope, column.expr, true);

        for (const dependency of expr.instantiation) {
          if (dependency instanceof ColumnInRow) {
            selectsFrom(dependency.resultSet, dependency.syntacticOrigin);
          } else {
            this.errors.report(
              'This attempts to sync a connection parameter. Only values from the source database can be synced.',
              dependency.syntacticOrigin
            );
          }
        }

        try {
          this.resultColumns.push(new ExpressionColumnSource(new RowExpression(expr), column.alias?.name));
        } catch (e) {
          if (e instanceof InvalidExpressionError) {
            // Invalid dependencies, we've already logged errors for this. Ignore.
          } else {
            throw e;
          }
        }
      }
    }
  }

  private parseExpression(scope: SqlScope, source: Expr, desugar: boolean): SyncExpression {
    if (desugar) {
      source = desugarSubqueries(source);
    }

    return trackDependencies(this, scope, source);
  }

  resolveTableName(scope: SqlScope, node: PGNode, name: string | nil): SourceResultSet | null {
    if (name == null) {
      // For unqualified references, there must be a single table in scope. We don't allow unqualified references if
      // there are multiple tables because we don't know which column is available in which table with certainty (and
      // don't want to re-compile sync streams on schema changes). So, we just refuse to resolve those ambigious
      // references.
      const resultSets = scope.resultSets;
      if (resultSets.length == 1) {
        return this.resultSets.get(resultSets[0])!;
      } else {
        this.errors.report('Invalid unqualified reference since multiple tables are in scope', node);
        return null;
      }
    } else {
      const result = scope.resolveResultSetForReference(name);
      if (result == null) {
        this.errors.report(`Table '${name}'has not been added in a FROM clause here.`, node);
        return null;
      }

      return this.resultSets.get(result)!;
    }
  }

  private warnUnsupported(node: PGNode | PGNode[] | nil, description: string) {
    if (node != null) {
      let location: PGNode | NodeLocation;
      if (Array.isArray(node)) {
        if (node.length == 0) {
          return;
        }

        location = expandNodeLocations(node)!;
      } else {
        location = node;
      }

      this.errors.report(`${description} is not supported`, location);
    }
  }

  private compileFilterClause(terms: [SqlScope, Expr][]): Or {
    const andTerms: PendingFilterExpression[] = [];
    for (const [scope, expr] of terms) {
      andTerms.push(this.extractBooleanOperators(scope, desugarSubqueries(expr)));
    }

    const pendingDnf = toDisjunctiveNormalForm({ type: 'and', inner: andTerms });

    // Within the DNF, each base expression (that is, anything not an OR or AND) is either:
    //
    //  1. An expression only depending on a single row, which wee use to potentially skip that row when evaluating rows
    //     or parameters.
    //  2. An expression only depending on request data, which we use as a filter when querying buckets for a
    //     connection.
    //  3. A static expression, which we'll add to both 1. and 2.
    //  4. An EqualsClause.
    const mappedTerms: And[] = pendingDnf.inner.map((conjunction) => {
      if (conjunction.type != 'and') {
        throw new Error('internal: DNF should have returned OR of ANDs');
      }

      return {
        terms: conjunction.inner.map((base) => {
          if (base.type != 'base') {
            throw new Error('internal: DNF transformation did not have inner type as base');
          }

          return this.mapBaseExpression(base);
        })
      };
    });

    return { terms: mappedTerms };
  }

  private mapBaseExpression(pending: PendingBaseTerm): BaseTerm {
    if (pending.inner.type == 'binary') {
      if (pending.inner.op == '=') {
        // The expression is of the form A = B. This introduces a parameter, allow A and B to reference different
        // result sets.
        const left = this.parseExpression(pending.scope, pending.inner.left, false);
        const right = this.parseExpression(pending.scope, pending.inner.right, false);

        if (SingleDependencyExpression.extractSingleDependency([...left.instantiation, ...right.instantiation])) {
          // Special case: Left and right reference the same table (e.g. tbl.foo = tbl.bar). This is not a match clause,
          // we can merge that into a single expression.
          throw 'todo';
        }

        return new EqualsClause(this.mustBeSingleDependency(left), this.mustBeSingleDependency(right));
      }
    }

    return this.mustBeSingleDependency(this.parseExpression(pending.scope, pending.inner, false));
  }

  /**
   * Emit diagnostics if the expression references data from more than one table, otherwise returns it as a
   * single-dependency expression.
   */
  private mustBeSingleDependency(inner: SyncExpression): SingleDependencyExpression {
    let referencingResultSet: [SourceResultSet, PGNode] | null = null;
    let referencingConnection: PGNode | null = null;
    let hadError = false;

    for (const dependency of inner.instantiation) {
      if (dependency instanceof ColumnInRow) {
        if (referencingConnection != null) {
          this.errors.report(
            "This expression already references connection parameters, so it can't also reference row data unless the two are compared with an equals operator.",
            dependency.syntacticOrigin
          );
          hadError = true;
        } else if (referencingResultSet != null && referencingResultSet[0] != dependency.resultSet) {
          const name = referencingResultSet[0].description;
          this.errors.report(
            `This expression already references '${name}', so it can't also reference data from this row unless the two are compared with an equals operator.`,
            dependency.syntacticOrigin
          );
          hadError = true;
        }

        referencingResultSet = [dependency.resultSet, dependency.syntacticOrigin];
      } else {
        if (referencingResultSet != null) {
          this.errors.report(
            "This expression already references row data, so it can't also reference connection parameters unless the two are compared with an equals operator.",
            dependency.syntacticOrigin
          );
          hadError = true;
        }

        referencingConnection = dependency.syntacticOrigin;
      }
    }

    if (hadError) {
      // Return a bogus expression to keep going / potentially collect more errors.
      return new SingleDependencyExpression(new SyncExpression({ type: 'null' }, []));
    } else {
      return new SingleDependencyExpression(inner);
    }
  }

  private extractBooleanOperators(scope: SqlScope, source: Expr): PendingFilterExpression {
    if (source.type == 'binary') {
      if (source.op == 'AND') {
        return {
          type: 'and',
          inner: [this.extractBooleanOperators(scope, source.left), this.extractBooleanOperators(scope, source.right)]
        };
      } else if (source.op == 'OR') {
        return {
          type: 'or',
          inner: [this.extractBooleanOperators(scope, source.left), this.extractBooleanOperators(scope, source.right)]
        };
      }
    } else if (source.type == 'unary' && source.op == 'NOT') {
      return { type: 'not', inner: this.extractBooleanOperators(scope, source.operand) };
    }

    return { type: 'base', scope, inner: source };
  }
}

function desugarSubqueries(source: Expr): Expr {
  const mapper = astMapper((map) => ({
    binary: (expr) => {
      if (expr.op == 'IN' || expr.op == 'NOT IN') {
        throw 'TODO: Desugar subqueries';
      }

      return map.super().binary(expr);
    }
  }));

  return mapper.expr(source)!;
}

function trackDependencies(parser: StreamQueryParser, scope: SqlScope, source: Expr): SyncExpression {
  const instantiation: ExpressionInput[] = [];
  const mapper = astMapper((map) => {
    function createParameter(node: PGNode): ExprParameter {
      return {
        type: 'parameter',
        name: `?${instantiation.length}`,
        _location: node._location
      };
    }

    function replaceWithParameter(node: PGNode) {
      return map.super().parameter(createParameter(node));
    }

    return {
      ref: (val) => {
        const resultSet = parser.resolveTableName(scope, val, val.table?.name);
        if (resultSet == null) {
          // resolveTableName will have logged an error, so transform with a bogus value to keep going.
          return { type: 'null', _location: val._location };
        }

        instantiation.push(new ColumnInRow(val, resultSet, val.name));
        return replaceWithParameter(val);
      },
      call: (val) => {
        const schemaName = val.function.schema;
        const source: ConnectionParameterSource | null =
          schemaName === 'auth' || schemaName === 'subscription' || schemaName === 'connection' ? schemaName : null;
        if (!source) {
          return map.super().call(val);
        }

        const parameter = new ConnectionParameter(val, source);
        instantiation.push(parameter);
        const replacement = createParameter(val);

        switch (val.function.name.toLowerCase()) {
          case 'parameters':
            break;
          case 'parameter':
            // Desugar .param(x) into .parameters() ->> '$.' || x
            if (val.args.length == 1) {
              return map.super().binary({
                type: 'binary',
                left: replacement,
                op: '->>' as BinaryOperator,
                right: {
                  type: 'binary',
                  left: { type: 'string', value: '$.' },
                  op: '||',
                  right: val.args[0]
                },
                _location: val._location
              });
            } else {
              parser.errors.report('Expected a single argument here', val.function);
            }
          case 'user_id':
            if (source == 'auth') {
              // Desugar auth.user_id() into auth.parameters() ->> '$.sub'
              return map.super().binary({
                type: 'binary',
                left: replacement,
                op: '->>' as BinaryOperator,
                right: { type: 'string', value: '$.sub' },
                _location: val._location
              });
            } else {
              parser.errors.report('.user_id() is only available on auth schema', val.function);
            }
            break;
          default:
            parser.errors.report('Unknown request function', val.function);
        }

        // Return the entire JSON object
        return map.super().parameter(replacement);
      }
    };
  });

  const transformed = mapper.expr(source)!;
  return new SyncExpression(transformed, instantiation);
}

/**
 * An arbitrary boolean expression we're about to transform to DNF.
 */
type PendingFilterExpression =
  | {
      type: 'not';
      inner: PendingFilterExpression;
    }
  | { type: 'and'; inner: PendingFilterExpression[] }
  | PendingOr
  | PendingBaseTerm;

type PendingBaseTerm = { type: 'base'; scope: SqlScope; inner: Expr };
type PendingOr = { type: 'or'; inner: PendingFilterExpression[] };

function toDisjunctiveNormalForm(source: PendingFilterExpression): PendingOr {
  const prepared = prepareToDNF(source);
  switch (prepared.type) {
    case 'or':
      return {
        type: 'or',
        inner: prepared.inner.map((e) => {
          if (e.type == 'and') {
            return e;
          } else {
            return { type: 'and', inner: [e] };
          }
        })
      };
    case 'and':
      return { type: 'or', inner: [prepared] };
    default:
      return { type: 'or', inner: [{ type: 'and', inner: [prepared] }] };
  }
}

function prepareToDNF(expr: PendingFilterExpression): PendingFilterExpression {
  switch (expr.type) {
    case 'not': {
      // Push NOT downwards, depending on the inner term.
      const inner = expr.inner;
      switch (inner.type) {
        case 'not':
          return inner.inner; // Double negation, !x => x
        case 'and':
          // !(a AND b) => (!a) OR (!b)
          return { type: 'or', inner: inner.inner.map((e) => prepareToDNF({ type: 'not', inner: e })) };
        case 'or':
          // !(a OR b) => (!a) AND (!b)
          return { type: 'or', inner: inner.inner.map((e) => prepareToDNF({ type: 'not', inner: e })) };
        case 'base':
          return {
            type: 'base',
            scope: inner.scope,
            inner: {
              type: 'unary',
              op: 'NOT',
              operand: inner.inner
            }
          };
      }
    }
    case 'and': {
      const baseFactors: PendingBaseTerm[] = [];
      const orTerms: PendingOr[] = [];

      for (const originalTerm of expr.inner) {
        const normalized = prepareToDNF(originalTerm);
        if (normalized.type == 'and') {
          // Normalized and will only have base terms as children
          baseFactors.push(...(normalized.inner as PendingBaseTerm[]));
        } else if (normalized.type == 'or') {
          orTerms.push(normalized);
        } else {
          // prepareToDNF would have eliminated NOT operators
          baseFactors.push(normalized as PendingBaseTerm);
        }
      }

      if (orTerms.length == 0) {
        return { type: 'and', inner: baseFactors };
      }

      // If there's an OR term within the AND, apply the distributive law to turn the term into an AND within an outer
      // OR. First, we transform orTerms to turn e.g. (a OR b) AND (c OR d) into (a AND c) OR (a AND d) OR (b AND c) OR
      // (b AND d).
      const multiplied = cartesianProduct(...orTerms.map((e) => e.inner));

      // Then, combine those with the inner AND to turn `A & (B | C) & D` into `(B & A & D) | (C & A & D)`.
      const finalFactors: PendingFilterExpression[] = [];
      for (const distributedTerms of multiplied) {
        finalFactors.push({ type: 'and', inner: [...distributedTerms, ...baseFactors] });
      }
      return { type: 'or', inner: finalFactors };
    }
    case 'or': {
      // If the outer expression is an OR, it's already in DNF. But we want to simplify `(A OR B) OR C` into `A OR B OR C`
      // if possible.
      const expanded: PendingFilterExpression[] = [];
      for (const term of expr.inner) {
        const normalized = prepareToDNF(term);
        if (normalized.type == 'or') {
          expanded.push(...normalized.inner);
        } else {
          expanded.push(normalized);
        }

        return { type: 'or', inner: expanded };
      }
    }
    case 'base':
      // There are no boolean operators to adopt.
      return expr;
  }
}
