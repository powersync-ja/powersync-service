import {
  assignChanged,
  astMapper,
  BinaryOperator,
  Expr,
  ExprCall,
  ExprParameter,
  ExprRef,
  From,
  nil,
  NodeLocation,
  PGNode,
  SelectedColumn,
  SelectFromStatement,
  Statement
} from 'pgsql-ast-parser';
import {
  PhysicalSourceResultSet,
  RequestTableValuedResultSet,
  SourceResultSet,
  SyntacticResultSetSource
} from './table.js';
import { ColumnSource, ExpressionColumnSource, StarColumnSource } from './rows.js';
import {
  ColumnInRow,
  ConnectionParameter,
  ConnectionParameterSource,
  ExpressionInput,
  SyncExpression
} from './expression.js';
import {
  BaseTerm,
  EqualsClause,
  InvalidExpressionError,
  Or,
  And,
  RowExpression,
  SingleDependencyExpression,
  RequestExpression
} from './filter.js';
import { expandNodeLocations } from '../errors.js';
import { cartesianProduct } from '../streams/utils.js';
import { intrinsicContains, PostgresToSqlite } from './sqlite.js';
import { SqlScope } from './scope.js';
import { ParsingErrorListener } from './compiler.js';
import { TablePattern } from '../TablePattern.js';
import { FilterConditionSimplifier } from './filter_simplifier.js';

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
export interface ParsedStreamQuery {
  resultColumns: ColumnSource[];
  sourceTable: PhysicalSourceResultSet;
  joined: SourceResultSet[];
  where: Or;
}

export class StreamQueryParser {
  readonly errors: ParsingErrorListener;
  private readonly originalText: string;
  private readonly statementScope: SqlScope;
  // Note: This is not the same as SqlScope since some result sets are inlined from CTEs or subqueries. These are not in
  // scope, but we still add them here to correctly track dependencies.
  private readonly resultSets = new Map<SyntacticResultSetSource, SourceResultSet>();
  private readonly resultColumns: ColumnSource[] = [];
  private where: Expr[] = [];

  /** The result set for which rows are synced. Set when analyzing result columns. */
  private primaryResultSet?: PhysicalSourceResultSet;
  private syntheticSubqueryCounter: number = 0;

  constructor(options: { originalText: string; errors: ParsingErrorListener; parentScope?: SqlScope }) {
    this.originalText = options.originalText;
    this.errors = options.errors;
    this.statementScope = new SqlScope({ parent: options.parentScope });
  }

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
  private processAst(node: Statement, options: { forSubquery: boolean }): boolean {
    if (node.type != 'select') {
      // TODO: Support with statements
      this.errors.report('Expected a SELECT statement', node);
      return false;
    }

    // Create scope and bind to statement
    this.statementScope.bindingVisitor(node).statement(node);

    node.from?.forEach((f) => this.processFrom(f));
    if (node.where) {
      this.where.push(node.where);
    }

    if (!options.forSubquery && node.columns) {
      this.processResultColumns(node.columns);
    }

    this.warnUnsupported(node.groupBy, 'GROUP BY');
    this.warnUnsupported(node.having, 'GROUP BY');
    this.warnUnsupported(node.limit, 'LIMIT');
    this.warnUnsupported(node.orderBy, 'ORDER BY');
    this.warnUnsupported(node.for, 'FOR');
    this.warnUnsupported(node.skip, 'SKIP');

    return true;
  }

  private processFrom(from: From) {
    const scope = this.statementScope;
    let handled = false;
    if (from.type == 'table') {
      const source = new SyntacticResultSetSource(from.name);
      const resultSet = new PhysicalSourceResultSet(new TablePattern(from.name.schema ?? '', from.name.name), source);
      scope.registerResultSet(this.errors, from.name.alias ?? from.name.name, source);
      this.resultSets.set(source, resultSet);
      handled = true;
    } else if (from.type == 'call') {
      const source = new SyntacticResultSetSource(from);
      scope.registerResultSet(this.errors, from.alias?.name ?? from.function.name, source);
      this.resultSets.set(source, this.resolveTableValued(from, source));
      handled = true;
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
        this.where.push(join.on);
      }
    }

    if (!handled) {
      this.warnUnsupported(from, 'This source');
    }
  }

  private resolveTableValued(call: ExprCall, source: SyntacticResultSetSource): RequestTableValuedResultSet {
    // Currently, inputs to table-valued functions must be derived from connection/subscription data only. We might
    // revisit this in the future.
    const resolvedArguments: RequestExpression[] = [];

    for (const argument of call.args) {
      const parsed = this.mustBeSingleDependency(this.parseExpression(argument, true));

      if (parsed.resultSet != null) {
        this.errors.report('Parameters to table-valued functions may not reference other tables', parsed.node);
      }
    }

    return new RequestTableValuedResultSet(call.function.name, resolvedArguments, source);
  }

  private processResultColumns(columns: SelectedColumn[]) {
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
        this.errors.report('Sync streams can only select from actual tables', node);
      }
    };

    for (const column of columns) {
      if (column.expr.type == 'ref' && column.expr.name == '*') {
        const resolved = this.resolveTableName(column.expr, column.expr.table?.name);
        if (resolved != null) {
          selectsFrom(resolved, column.expr);
        }

        this.resultColumns.push(StarColumnSource.instance);
      } else {
        const { expr, node } = this.parseExpression(column.expr, true);

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
          this.resultColumns.push(new ExpressionColumnSource(new RowExpression(expr, node), column.alias?.name));
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

  private parseExpression(source: Expr, desugar: boolean): SyncExpressionWithAst {
    if (desugar) {
      source = this.desugarSubqueries(source);
    }

    return trackDependencies(this, source);
  }

  resolveTableName(node: ExprRef, name: string | nil): SourceResultSet | null {
    const scope = SqlScope.readBoundScope(node);
    if (scope == null) {
      throw new Error('internal: Tried to resolve reference that has not been attached to a scope');
    }

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

  private compileFilterClause(terms: Expr[]): Or {
    const andTerms: PendingFilterExpression[] = [];
    for (const expr of terms) {
      andTerms.push(this.extractBooleanOperators(this.desugarSubqueries(expr)));
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

    return new FilterConditionSimplifier(this.originalText, this.errors).simplifyOr({ terms: mappedTerms });
  }

  private mapBaseExpression(pending: PendingBaseTerm): BaseTerm {
    if (pending.inner.type == 'binary') {
      if (pending.inner.op == '=') {
        // The expression is of the form A = B. This introduces a parameter, allow A and B to reference different
        // result sets.
        const left = this.parseExpression(pending.inner.left, false);
        const right = this.parseExpression(pending.inner.right, false);

        return new EqualsClause(this.mustBeSingleDependency(left), this.mustBeSingleDependency(right));
      }
    }

    return this.mustBeSingleDependency(this.parseExpression(pending.inner, false));
  }

  toSyncExpression(source: Expr, instantiation: ExpressionInput[]): SyncExpressionWithAst {
    const toSqlite = new PostgresToSqlite(this.originalText, this.errors);
    toSqlite.addExpression(source);

    return { expr: new SyncExpression(toSqlite.sql, instantiation, source._location), node: source };
  }

  /**
   * Emit diagnostics if the expression references data from more than one table, otherwise returns it as a
   * single-dependency expression.
   */
  private mustBeSingleDependency(inner: SyncExpressionWithAst): SingleDependencyExpression {
    let referencingResultSet: [SourceResultSet, PGNode] | null = null;
    let referencingConnection: PGNode | null = null;
    let hadError = false;

    for (const dependency of inner.expr.instantiation) {
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
      return new SingleDependencyExpression(new SyncExpression('NULL', [], inner.expr.originalLocation), {
        type: 'null'
      });
    } else {
      return new SingleDependencyExpression(inner.expr, inner.node);
    }
  }

  private extractBooleanOperators(source: Expr): PendingFilterExpression {
    if (source.type == 'binary') {
      if (source.op == 'AND') {
        return {
          type: 'and',
          inner: [this.extractBooleanOperators(source.left), this.extractBooleanOperators(source.right)]
        };
      } else if (source.op == 'OR') {
        return {
          type: 'or',
          inner: [this.extractBooleanOperators(source.left), this.extractBooleanOperators(source.right)]
        };
      }
    } else if (source.type == 'unary' && source.op == 'NOT') {
      return { type: 'not', inner: this.extractBooleanOperators(source.operand) };
    }

    return { type: 'base', inner: source };
  }

  /**
   * Desugars valid forms of subqueries in the source expression.
   *
   * In particular, this lowers `x IN (SELECT ...)` by adding the inner select statement as an `OUTER JOIN` and then
   * replacing the `IN` operator with `x = joinedTable.value`.
   */
  desugarSubqueries(source: Expr): Expr {
    const mapper = astMapper((map) => {
      // Desugar left IN ARRAY(...right) to "intrinsic:contains"(left, ...right). This will eventually get compiled to
      // the SQLite expression LEFT IN (...right), using row-values syntax.
      function desugarInValues(negated: boolean, left: Expr, right: Expr[]) {
        const containsCall: Expr = { type: 'call', function: { name: intrinsicContains }, args: [left, ...right] };
        if (negated) {
          return map.super().expr({ type: 'unary', op: 'NOT', operand: containsCall });
        } else {
          return map.super().expr(containsCall);
        }
      }

      const desugarInSubquery = (negated: boolean, left: Expr, right: SelectFromStatement) => {
        // Independently analyze the inner query.
        const parseInner = new StreamQueryParser({
          originalText: this.originalText,
          errors: this.errors,
          parentScope: this.statementScope
        });
        let success = parseInner.processAst(right, { forSubquery: true });
        let resultColumn: Expr | null = null;
        if (right.columns?.length == 1) {
          const column = right.columns[0].expr;
          // The result of a subquery must be a scalar expression
          if (!(column.type == 'ref' && column.name == '*')) {
            resultColumn = column;
          }
        }

        if (resultColumn == null) {
          this.errors.report('Must return a single expression column', right);
          success = false;
        }
        if (!success || resultColumn == null) {
          return map.expr({ type: 'null', _location: right._location });
        }

        // Inline the subquery by adding all referenced tables to the main query, adding the filter and replacing
        // `a IN (SELECT b FROM ...)` with `a = joined.b`.
        parseInner.resultSets.forEach((v, k) => this.resultSets.set(k, v));
        let replacement: Expr = { type: 'binary', op: '=', left: left, right: resultColumn };
        if (parseInner.where != null) {
          replacement = parseInner.where.reduce((prev, current) => {
            return {
              type: 'binary',
              left: prev,
              right: current,
              op: 'AND'
            } satisfies Expr;
          }, replacement);
        }

        return replacement;
      };

      // Desugar left IN right, where right is a scalar expression. This is not valid SQL, but in PowerSync we interpret
      // that as `left IN (SELECT value FROM json_each(right))`.
      const desugarInScalar = (negated: boolean, left: Expr, right: Expr) => {
        const counter = this.syntheticSubqueryCounter++;
        const name = `synthetic:${counter}`;
        return desugarInSubquery(negated, left, {
          type: 'select',
          columns: [{ expr: { type: 'ref', name: 'value', table: { name } } }],
          from: [{ type: 'call', function: { name: 'json_each' }, args: [right], alias: { name } }]
        });
      };

      return {
        binary: (expr) => {
          if (expr.op == 'IN' || expr.op == 'NOT IN') {
            const right = expr.right;
            const negated = expr.op == 'NOT IN';

            if (right.type == 'select') {
              return desugarInSubquery(negated, expr.left, right);
            } else if (right.type == 'array') {
              return desugarInValues(negated, expr.left, right.expressions);
            } else if (right.type == 'call' && right.function.name.toLowerCase() == 'row') {
              return desugarInValues(negated, expr.left, right.args);
            } else {
              return desugarInScalar(negated, expr.left, right);
            }
          }

          return map.super().binary(expr);
        }
      };
    });

    return mapper.expr(source)!;
  }
}

function trackDependencies(parser: StreamQueryParser, source: Expr): SyncExpressionWithAst {
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
        const resultSet = parser.resolveTableName(val, val.table?.name);
        if (val.name == '*') {
          parser.errors.report('* columns are not supported here', val);
        }

        if (resultSet == null || val.name == '*') {
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
  return parser.toSyncExpression(transformed, instantiation);
}

type SyncExpressionWithAst = { node: Expr; expr: SyncExpression };

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

type PendingBaseTerm = { type: 'base'; inner: Expr };
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
        finalFactors.push(prepareToDNF({ type: 'and', inner: [...distributedTerms, ...baseFactors] }));
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
      }

      return { type: 'or', inner: expanded };
    }
    case 'base':
      // There are no boolean operators to adopt.
      return expr;
  }
}
