import {
  Expr,
  ExprCall,
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
  BaseSourceResultSet,
  PhysicalSourceResultSet,
  RequestTableValuedResultSet,
  SourceResultSet,
  SyntacticResultSetSource
} from './table.js';
import { ColumnSource, ExpressionColumnSource, StarColumnSource } from './rows.js';
import { ColumnInRow, ExpressionInput, NodeLocations, SyncExpression } from './expression.js';
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
import { PostgresToSqlite, PreparedSubquery } from './sqlite.js';
import { SqlScope } from './scope.js';
import { ParsingErrorListener, SyncStreamsCompiler } from './compiler.js';
import { TablePattern } from '../TablePattern.js';
import { composeExpressionNodes, FilterConditionSimplifier } from './filter_simplifier.js';
import { SqlExpression } from '../sync_plan/expression.js';

/**
 * A parsed stream query in its canonical form.
 *
 * The canonical form of stream queries is as follows:
 *
 * ```SQL
 * SELECT <expr>* FROM sourceTable a
 *   JOIN table2 b
 *   -- ... additional joins
 * WHERE <expr>
 * ```
 *
 * Expressions in the result set must all refer to columns in the {@link sourceTable}.
 *
 * Additionally, the where clause is in disjunctive normal form. Inner terms are either scalar expressions only
 * depending on data from a single source result set, or match clauses.
 *
 * Subqueries are not allowed in the canonical form. Instead, they are lowered into joins. So e.g. the query:
 *
 * ```SQL
 * SELECT * FROM users WHERE id IN (SELECT user FROM org WHERE id = auth.user_id())
 * ```
 *
 * would get lowered to:
 * ```SQL
 * SELECT tmp0.* FROM users tmp0
 *   JOIN org tmp1
 * WHERE tmp0.id = tmp1.user AND tmp1.id = (:auth_token) ->> '$.sub';
 * ```
 *
 * Unlike shown here, this lowering doesn't generate aliases that would have to be applied to expressions though.
 * References in expressions would get resolved earlier, and they continue being valid after the transformation.
 */
export interface ParsedStreamQuery {
  resultColumns: ColumnSource[];
  sourceTable: PhysicalSourceResultSet;
  joined: SourceResultSet[];
  /**
   * All filters, in disjunctive normal form (an OR of ANDs).
   */
  where: Or;
}

export interface StreamQueryParserOptions {
  compiler: SyncStreamsCompiler;
  originalText: string;
  errors: ParsingErrorListener;
  parentScope: SqlScope;
  locations: NodeLocations;
}

export class StreamQueryParser {
  readonly errors: ParsingErrorListener;
  private readonly compiler: SyncStreamsCompiler;
  readonly originalText: string;
  private readonly statementScope: SqlScope;
  // Note: This is not the same as SqlScope since some result sets are inlined from CTEs or subqueries. These are not in
  // scope, but we still add them here to correctly track dependencies.
  private readonly resultSets = new Map<SyntacticResultSetSource, SourceResultSet>();
  private readonly subqueryResultSets = new Map<SyntacticResultSetSource, PreparedSubquery>();

  private readonly resultColumns: ColumnSource[] = [];
  private where: SqlExpression<ExpressionInput>[] = [];

  /** The result set for which rows are synced. Set when analyzing result columns. */
  private primaryResultSet?: PhysicalSourceResultSet;
  private syntheticSubqueryCounter: number = 0;
  private nodeLocations: NodeLocations;
  private exprParser: PostgresToSqlite;

  constructor(options: StreamQueryParserOptions) {
    this.compiler = options.compiler;
    this.originalText = options.originalText;
    this.errors = options.errors;
    this.statementScope = new SqlScope({ parent: options.parentScope });
    this.nodeLocations = options.locations;

    this.exprParser = new PostgresToSqlite({
      originalText: this.originalText,
      errors: this.errors,
      locations: this.nodeLocations,
      scope: this.statementScope,
      resolveTableName: this.resolveTableName.bind(this),
      generateTableAlias: () => {
        const counter = this.syntheticSubqueryCounter++;
        return `synthetic:${counter}`;
      },
      joinSubqueryExpression: (expr) => {
        // Independently analyze the inner query.
        const parseInner = this.nestedParser(this.statementScope);
        let success = parseInner.processAst(expr, { forSubquery: true });
        if (!success) {
          return null;
        }

        let resultColumn: Expr | null = null;
        if (expr.columns?.length == 1) {
          const column = expr.columns[0].expr;
          // The result of a subquery must be a scalar expression
          if (!(column.type == 'ref' && column.name == '*')) {
            resultColumn = column;
          }
        }

        if (resultColumn == null) {
          // TODO: We could reasonably support syntax of the form (a, b) IN (SELECT a, b FROM ...) by desugaring that
          // into multiple equals operators? The rest of the compiler should already be able to handle that.
          this.errors.report('Must return a single expression column', expr);
          return null;
        }

        parseInner.resultSets.forEach((v, k) => this.resultSets.set(k, v));
        return { filters: parseInner.where, output: parseInner.parseExpression(resultColumn).node };
      }
    });
  }

  parse(stmt: Statement): ParsedStreamQuery | null {
    if (this.processAst(stmt, { forSubquery: false })) {
      if (this.primaryResultSet == null) {
        return null;
      }

      const where = this.compileFilterClause();
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

  parseAsSubquery(stmt: Statement, columnNames?: string[]): PreparedSubquery | null {
    if (this.processAst(stmt, { forSubquery: true })) {
      const resultColumns: Record<string, SqlExpression<ExpressionInput>> = {};
      let columnCount = 0;

      for (const column of (stmt as SelectFromStatement).columns ?? []) {
        if (column.expr.type == 'ref' && column.expr.name == '*') {
          // We don't support * columns in subqueries. The reason is that we want to be able to parse queries without
          // knowing the schema, so we can't know what * would resolve to.
          this.errors.report('* columns are not allowed in subqueries or common table expressions', column.expr);
        } else {
          const name = (columnNames && columnNames[columnCount]) ?? this.inferColumnName(column);
          const expr = this.parseExpression(column.expr);

          if (Object.hasOwn(resultColumns, name)) {
            this.errors.report(`There is a column named '${name}' already.`, column);
          }

          resultColumns[name] = expr.node;
          columnCount++;
        }
      }

      if (columnNames && columnNames.length != columnCount) {
        this.errors.report(
          `Expected this subquery to have ${columnNames.length} columns, it actually has ${columnCount}`,
          stmt
        );
      }

      return {
        resultColumns,
        tables: this.resultSets,
        where: this.where.length == 0 ? null : composeExpressionNodes(this.nodeLocations, 'and', this.where)
      };
    } else {
      return null;
    }
  }

  private nestedParser(parentScope: SqlScope): StreamQueryParser {
    return new StreamQueryParser({
      compiler: this.compiler,
      originalText: this.originalText,
      errors: this.errors,
      parentScope,
      locations: this.nodeLocations
    });
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

    node.from?.forEach((f) => this.processFrom(f));
    if (node.where) {
      this.addAndTermToWhereClause(node.where);
    }

    if (!options.forSubquery && node.columns) {
      this.processResultColumns(node, node.columns);
    }

    this.warnUnsupported(node.groupBy, 'GROUP BY');
    this.warnUnsupported(node.having, 'GROUP BY');
    this.warnUnsupported(node.limit, 'LIMIT');
    this.warnUnsupported(node.orderBy, 'ORDER BY');
    this.warnUnsupported(node.for, 'FOR');
    this.warnUnsupported(node.skip, 'SKIP');

    return true;
  }

  private addAndTermToWhereClause(expr: Expr) {
    this.where.push(this.parseExpression(expr).node);
  }

  private addSubquery(source: SyntacticResultSetSource, subquery: PreparedSubquery) {
    subquery.tables.forEach((v, k) => this.resultSets.set(k, v));
    if (subquery.where) {
      this.where.push(subquery.where);
    }

    this.subqueryResultSets.set(source, subquery);
  }

  private processFrom(from: From) {
    const scope = this.statementScope;
    if (from.type == 'table') {
      const name = from.name.alias ?? from.name.name;
      const source = new SyntacticResultSetSource(from.name, from.name.alias ?? null);
      scope.registerResultSet(this.errors, name, source);

      // If this references a CTE in scope, use that instead of names.
      const cte = from.name.schema == null ? scope.resolveCommonTableExpression(from.name.name) : null;
      if (cte) {
        this.addSubquery(source, cte);
      } else {
        // Not a CTE, so treat it as a source database table.
        const resultSet = new PhysicalSourceResultSet(
          new TablePattern(from.name.schema ?? this.compiler.defaultSchema, from.name.name),
          source
        );

        this.resultSets.set(source, resultSet);
      }
    } else if (from.type == 'call') {
      const source = new SyntacticResultSetSource(from, from.alias?.name ?? null);
      this.resultSets.set(source, this.resolveTableValued(from, source));
      scope.registerResultSet(this.errors, from.alias?.name ?? from.function.name, source);
    } else if (from.type == 'statement') {
      const source = new SyntacticResultSetSource(from, from.alias);

      // For subqueries in FROM, existing expressions are not in scope. So fork from the root scope instead.
      const parseInner = this.nestedParser(scope.rootScope);
      const parsedSubquery = parseInner.parseAsSubquery(
        from.statement,
        from.columnNames?.map((c) => c.name)
      );

      if (parsedSubquery) {
        scope.registerResultSet(this.errors, from.alias, source);
        this.addSubquery(source, parsedSubquery);
      }
    }

    const join = from.join;
    if (join) {
      if (join.type != 'INNER JOIN') {
        // We only support inner joins.
        this.warnUnsupported(join, 'FULL JOIN');
      }

      if (join.using) {
        // We don't support using because interpreting it depends on the schema. Users should spell it out explicitly
        // using an ON clause.
        this.warnUnsupported(join.using, 'USING');
      }

      if (join.on) {
        this.addAndTermToWhereClause(join.on);
      }
    }
  }

  private resolveTableValued(call: ExprCall, source: SyntacticResultSetSource): RequestTableValuedResultSet {
    // Currently, inputs to table-valued functions must be derived from connection/subscription data only. We might
    // revisit this in the future.
    const resolvedArguments: RequestExpression[] = [];

    for (const argument of call.args) {
      const parsed = this.mustBeSingleDependency(this.parseExpression(argument));

      if (parsed.resultSet != null) {
        this.errors.report(
          'Parameters to table-valued functions may not reference other tables',
          parsed.expression.location
        );
      }
    }

    return new RequestTableValuedResultSet(call.function.name, resolvedArguments, source);
  }

  private inferColumnName(column: SelectedColumn): string {
    return column.alias?.name ?? this.originalText.substring(column._location!.start, column._location!.end);
  }

  private processResultColumns(stmt: PGNode, columns: SelectedColumn[]) {
    const selectsFrom = (source: SourceResultSet, node: PGNode) => {
      if (source instanceof PhysicalSourceResultSet) {
        if (this.primaryResultSet == null) {
          this.primaryResultSet = source;
        } else if (this.primaryResultSet !== source) {
          this.errors.report(
            `Sync streams can only select from a single table, and this one already selects from '${this.primaryResultSet.tablePattern.name}'.`,
            node
          );
        }
      } else {
        this.errors.report('Sync streams can only select from actual tables', node);
      }
    };

    const addColumn = (expr: SyncExpression, name: string) => {
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
        this.resultColumns.push(new ExpressionColumnSource(new RowExpression(expr), name));
      } catch (e) {
        if (e instanceof InvalidExpressionError) {
          // Invalid dependencies, we've already logged errors for this. Ignore.
        } else {
          throw e;
        }
      }
    };

    for (const column of columns) {
      if (column.expr.type == 'ref' && column.expr.name == '*') {
        const resolved = this.resolveTableName(column.expr, column.expr.table?.name);
        if (resolved != null) {
          if (resolved instanceof BaseSourceResultSet) {
            selectsFrom(resolved, column.expr);
            this.resultColumns.push(StarColumnSource.instance);
          } else {
            // Selecting from a subquery, add all columns.
            for (const [name, column] of Object.entries(resolved.resultColumns)) {
              addColumn(new SyncExpression(column, this.nodeLocations), name);
            }
          }
        }
      } else {
        const expr = this.parseExpression(column.expr);
        const outputName = this.inferColumnName(column);
        addColumn(expr, outputName);
      }
    }

    if (this.primaryResultSet == null) {
      this.errors.report('Must have a result column selecting from a table', stmt);
    }
  }

  private parseExpression(source: Expr): SyncExpression {
    return this.exprParser.translateExpression(source);
  }

  private resolveTableName(node: ExprRef, name: string | nil): SourceResultSet | PreparedSubquery | null {
    if (name == null) {
      // For unqualified references, there must be a single table in scope. We don't allow unqualified references if
      // there are multiple tables because we don't know which column is available in which table with certainty (and
      // don't want to re-compile sync streams on schema changes). So, we just refuse to resolve those ambigious
      // references.
      const resultSets = this.statementScope.resultSets;
      if (resultSets.length == 1) {
        return this.resolveSoure(resultSets[0]);
      } else {
        this.errors.report('Invalid unqualified reference since multiple tables are in scope', node);
        return null;
      }
    } else {
      const result = this.statementScope.resolveResultSetForReference(name);
      if (result == null) {
        this.errors.report(`Table '${name}' has not been added in a FROM clause here.`, node);
        return null;
      }

      return this.resolveSoure(result);
    }
  }

  private resolveSoure(source: SyntacticResultSetSource): SourceResultSet | PreparedSubquery {
    if (this.resultSets.has(source)) {
      return this.resultSets.get(source)!;
    } else if (this.subqueryResultSets.has(source)) {
      return this.subqueryResultSets.get(source)!;
    }

    throw new Error('internal error: result set from scope has not been registered');
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

  private compileFilterClause(): Or {
    const andTerms: PendingFilterExpression[] = [];
    for (const expr of this.where) {
      andTerms.push(this.extractBooleanOperators(expr));
    }

    const pendingDnf = toDisjunctiveNormalForm({ type: 'and', inner: andTerms }, this.nodeLocations);

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

    return new FilterConditionSimplifier().simplifyOr({ terms: mappedTerms });
  }

  private mapBaseExpression(pending: PendingBaseTerm): BaseTerm {
    if (pending.inner.type == 'binary') {
      if (pending.inner.operator == '=') {
        // The expression is of the form A = B. This introduces a parameter, allow A and B to reference different
        // result sets.
        const left = new SyncExpression(pending.inner.left, this.nodeLocations);
        const right = new SyncExpression(pending.inner.right, this.nodeLocations);

        return new EqualsClause(this.mustBeSingleDependency(left), this.mustBeSingleDependency(right));
      }
    }

    return this.mustBeSingleDependency(new SyncExpression(pending.inner, this.nodeLocations));
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
      const value: SqlExpression<ExpressionInput> = { type: 'lit_null' };
      this.nodeLocations.sourceForNode.set(value, inner.location);
      return new SingleDependencyExpression(new SyncExpression(value, this.nodeLocations));
    } else {
      return new SingleDependencyExpression(inner);
    }
  }

  private extractBooleanOperators(source: SqlExpression<ExpressionInput>): PendingFilterExpression {
    if (source.type == 'binary') {
      if (source.operator == 'and') {
        return {
          type: 'and',
          inner: [this.extractBooleanOperators(source.left), this.extractBooleanOperators(source.right)]
        };
      } else if (source.operator == 'or') {
        return {
          type: 'or',
          inner: [this.extractBooleanOperators(source.left), this.extractBooleanOperators(source.right)]
        };
      }
    } else if (source.type == 'unary' && source.operator == 'not') {
      return { type: 'not', inner: this.extractBooleanOperators(source.operand) };
    }

    return { type: 'base', inner: source };
  }
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

type PendingBaseTerm = { type: 'base'; inner: SqlExpression<ExpressionInput> };
type PendingOr = { type: 'or'; inner: PendingFilterExpression[] };

function toDisjunctiveNormalForm(source: PendingFilterExpression, locations: NodeLocations): PendingOr {
  const prepared = prepareToDNF(source, locations);
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

function prepareToDNF(expr: PendingFilterExpression, locations: NodeLocations): PendingFilterExpression {
  switch (expr.type) {
    case 'not': {
      // Push NOT downwards, depending on the inner term.
      const inner = expr.inner;
      switch (inner.type) {
        case 'not':
          return inner.inner; // Double negation, !x => x
        case 'and':
          // !(a AND b) => (!a) OR (!b)
          return prepareToDNF(
            {
              type: 'or',
              inner: inner.inner.map((e) => prepareToDNF({ type: 'not', inner: e }, locations))
            },
            locations
          );
        case 'or':
          // !(a OR b) => (!a) AND (!b)
          return prepareToDNF(
            {
              type: 'and',
              inner: inner.inner.map((e) => prepareToDNF({ type: 'not', inner: e }, locations))
            },
            locations
          );
        case 'base':
          const mappedInner: SqlExpression<ExpressionInput> = {
            type: 'unary',
            operator: 'not',
            operand: inner.inner
          };
          locations.sourceForNode.set(mappedInner, locations.locationFor(inner.inner));
          return { type: 'base', inner: mappedInner };
      }
    }
    case 'and': {
      const baseFactors: PendingBaseTerm[] = [];
      const orTerms: PendingOr[] = [];

      for (const originalTerm of expr.inner) {
        const normalized = prepareToDNF(originalTerm, locations);
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
        finalFactors.push(prepareToDNF({ type: 'and', inner: [...distributedTerms, ...baseFactors] }, locations));
      }
      return { type: 'or', inner: finalFactors };
    }
    case 'or': {
      // If the outer expression is an OR, it's already in DNF. But we want to simplify `(A OR B) OR C` into `A OR B OR C`
      // if possible.
      const expanded: PendingFilterExpression[] = [];
      for (const term of expr.inner) {
        const normalized = prepareToDNF(term, locations);
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
