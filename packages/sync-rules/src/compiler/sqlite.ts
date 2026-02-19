import {
  BinaryOperator,
  Expr,
  ExprBinary,
  ExprCall,
  ExprRef,
  nil,
  PGNode,
  SelectFromStatement
} from 'pgsql-ast-parser';
import { CAST_TYPES } from '../sql_functions.js';
import { ColumnInRow, ConnectionParameter, ExpressionInput, NodeLocations, SyncExpression } from './expression.js';
import {
  BetweenExpression,
  LiteralExpression,
  SqlExpression,
  supportedFunctions,
  BinaryOperator as SupportedBinaryOperator
} from '../sync_plan/expression.js';
import { ConnectionParameterSource } from '../sync_plan/plan.js';
import { ParsingErrorListener } from './compiler.js';
import { BaseSourceResultSet, PhysicalSourceResultSet, SourceResultSet, SyntacticResultSetSource } from './table.js';
import { SqlScope } from './scope.js';
import { SourceSchema } from '../types.js';

export interface ResolvedSubqueryExpression {
  filters: SqlExpression<ExpressionInput>[];
  output: SqlExpression<ExpressionInput>;
}

/**
 * A prepared subquery or common table expression.
 */
export interface PreparedSubquery {
  /**
   * Columns selected by the query, indexed by their name.
   */
  resultColumns: Record<string, SqlExpression<ExpressionInput>>;

  /**
   * Tables the subquery selects from.
   */
  tables: Map<SyntacticResultSetSource, SourceResultSet>;

  /**
   * Filters affecting the subquery.
   */
  where: SqlExpression<ExpressionInput> | null;
}

export interface PostgresToSqliteOptions {
  readonly originalText: string;
  readonly scope: SqlScope;
  readonly errors: ParsingErrorListener;
  readonly locations: NodeLocations;

  /**
   * Attempt to resolve a table name in scope, returning the resolved result set.
   *
   * Should report an error if resolving the table failed, using `node` as the source location for the error.
   */
  resolveTableName(node: ExprRef, name: string | nil): SourceResultSet | PreparedSubquery | null;

  /**
   * Generates a table alias for synthetic subqueries like those generated to desugar `IN` expressions to `json_each`
   * subqueries.
   */
  generateTableAlias(): string;

  /**
   * Turns the given subquery into a join added to the main `FROM` section.
   *
   * Returns the parsed subquery expression, or null if resolving the subquery failed. In that case, this method should
   * report an error.
   */
  joinSubqueryExpression(expr: SelectFromStatement): ResolvedSubqueryExpression | null;
}

/**
 * Validates and lowers a Postgres expression into a scalar SQL expression.
 *
 * Unsupported Postgres features are reported as errors.
 */
export class PostgresToSqlite {
  constructor(private readonly options: PostgresToSqliteOptions) {}

  translateExpression(source: Expr): SyncExpression {
    return new SyncExpression(this.translateNodeWithLocation(source), this.options.locations);
  }

  private translateNodeWithLocation(expr: Expr): SqlExpression<ExpressionInput> {
    const translated = this.translateToNode(expr);
    this.options.locations.sourceForNode.set(translated, expr);
    return translated;
  }

  private translateToNode(expr: Expr): SqlExpression<ExpressionInput> {
    switch (expr.type) {
      case 'null':
        return { type: 'lit_null' };
      case 'boolean':
        return { type: 'lit_int', base10: expr.value ? '1' : '0' };
      case 'string':
        return { type: 'lit_string', value: expr.value };
      case 'numeric': {
        return { type: 'lit_double', value: expr.value };
      }
      case 'integer': {
        // JavaScript does not have a number type that can represent SQLite ints, so we try to reuse the source.
        if (expr._location) {
          return {
            type: 'lit_int',
            base10: this.options.originalText.substring(expr._location.start, expr._location.end)
          };
        } else {
          return { type: 'lit_int', base10: expr.value.toString() };
        }
      }
      case 'ref': {
        const resultSet = this.options.resolveTableName(expr, expr.table?.name);
        if (resultSet == null) {
          // resolveTableName will have logged an error, transform with a bogus value to keep going.
          return { type: 'lit_null' };
        }

        if (expr.name == '*') {
          return this.invalidExpression(expr, '* columns are not supported here');
        }

        // If this references something from a source table, warn if that column doesn't exist.
        if (resultSet instanceof PhysicalSourceResultSet && resultSet.schemaTablesForWarnings.length) {
          let columnExistsInAnySourceTable = false;

          for (const table of resultSet.schemaTablesForWarnings) {
            if (table.getColumn(expr.name) != null) {
              columnExistsInAnySourceTable = true;
              break;
            }
          }

          if (!columnExistsInAnySourceTable) {
            this.options.errors.report('Column not found.', expr, { isWarning: true });
          }
        }

        if (resultSet instanceof BaseSourceResultSet) {
          // This is an actual result set.
          const instantiation = new ColumnInRow(expr, resultSet, expr.name);
          return {
            type: 'data',
            source: instantiation
          };
        } else {
          // Resolved to a subquery, inline the reference.
          const expression = resultSet.resultColumns[expr.name];
          if (expression == null) {
            return this.invalidExpression(expr, 'Column not found in subquery.');
          }

          return expression;
        }
      }
      case 'parameter':
        return this.invalidExpression(
          expr,
          'SQL parameters are not allowed. Use parameter functions instead: https://docs.powersync.com/usage/sync-streams#accessing-parameters'
        );
      case 'substring': {
        const mappedArgs = [this.translateNodeWithLocation(expr.value)];
        if (expr.from) {
          mappedArgs.push(this.translateNodeWithLocation(expr.from));
        } else {
          mappedArgs.push({ type: 'lit_int', base10: '1' });
        }
        if (expr.for) {
          mappedArgs.push(this.translateNodeWithLocation(expr.for));
        }

        return { type: 'function', function: 'substr', parameters: mappedArgs };
      }
      case 'call': {
        const schemaName = expr.function.schema;
        const source: ConnectionParameterSource | null =
          schemaName === 'auth' || schemaName === 'subscription' || schemaName === 'connection' ? schemaName : null;

        if (schemaName) {
          if (source) {
            return this.translateRequestParameter(source, expr);
          } else {
            return this.invalidExpression(expr.function, 'Invalid schema in function name');
          }
        }

        if (expr.distinct != null || expr.orderBy != null || expr.filter != null || expr.over != null) {
          return this.invalidExpression(expr.function, 'DISTINCT, ORDER BY, FILTER and OVER clauses are not supported');
        }

        const functionName = expr.function.name.toLowerCase();
        const forbiddenReason = forbiddenFunctions[functionName];
        if (forbiddenReason) {
          return this.invalidExpression(expr.function, `Forbidden call: ${forbiddenReason}`);
        }
        let allowedArgs = supportedFunctions[functionName];
        if (allowedArgs == null) {
          return this.invalidExpression(expr.function, 'Unknown function');
        } else {
          if (typeof allowedArgs == 'number') {
            allowedArgs = { min: allowedArgs, max: allowedArgs };
          }

          const actualArgs = expr.args.length;
          if (actualArgs < allowedArgs.min) {
            this.options.errors.report(`Expected at least ${allowedArgs.min} arguments`, expr);
          } else if (allowedArgs.max && actualArgs > allowedArgs.max) {
            this.options.errors.report(`Expected at most ${allowedArgs.max} arguments`, expr);
          } else if (allowedArgs.mustBeEven && actualArgs % 2 == 1) {
            this.options.errors.report(`Expected an even amount of arguments`, expr);
          } else if (allowedArgs.mustBeOdd && actualArgs % 2 == 0) {
            this.options.errors.report(`Expected an odd amount of arguments`, expr);
          }
        }

        return {
          type: 'function',
          function: functionName,
          parameters: expr.args.map((a) => this.translateNodeWithLocation(a))
        };
      }
      case 'binary': {
        if (expr.op === 'IN' || expr.op === 'NOT IN') {
          return this.translateInOrOverlapOperator(expr, 'in', expr.op === 'NOT IN');
        }
        if (expr.op === '&&') {
          return this.translateInOrOverlapOperator(expr, 'overlap', false);
        }

        const left = this.translateNodeWithLocation(expr.left);
        const right = this.translateNodeWithLocation(expr.right);
        if (expr.op === 'LIKE') {
          // We don't support LIKE in the old bucket definition system, and want to make sure we're clear about ICU,
          // case sensitivity and changing the escape character first. TODO: Support later.
          this.options.errors.report('LIKE expressions are not currently supported.', expr);
          return { type: 'function', function: 'like', parameters: [left, right] };
        } else if (expr.op === 'NOT LIKE') {
          return this.negate(expr, { type: 'function', function: 'like', parameters: [left, right] });
        } else if (expr.op === '!=') {
          const equals: SqlExpression<ExpressionInput> = { type: 'binary', left, right, operator: '=' };
          return this.negate(expr, equals);
        }

        const supported = supportedBinaryOperators[expr.op];
        if (supported == null) {
          return this.invalidExpression(expr, 'Unsupported binary operator');
        } else {
          return { type: 'binary', left, right, operator: supported };
        }
      }
      case 'unary': {
        let not = false;
        let rightHandSideOfIs: SqlExpression<ExpressionInput>;

        switch (expr.op) {
          case '-':
            return this.invalidExpression(expr, 'Unary minus is not currently supported');
          case '+':
            return { type: 'unary', operator: expr.op, operand: this.translateNodeWithLocation(expr.operand) };
          case 'NOT':
            return this.negate(expr, this.translateToNode(expr.operand));
          case 'IS NOT NULL':
            not = true;
          case 'IS NULL': // fallthrough
            rightHandSideOfIs = { type: 'lit_null' };
            break;
          case 'IS NOT TRUE':
            not = true;
          case 'IS TRUE': // fallthrough
            rightHandSideOfIs = { type: 'lit_int', base10: '1' };
            break;
          case 'IS NOT FALSE': // fallthrough
            not = true;
          case 'IS FALSE':
            rightHandSideOfIs = { type: 'lit_int', base10: '0' };
            break;
        }

        const mappedIs: SqlExpression<ExpressionInput> = {
          type: 'binary',
          left: this.translateNodeWithLocation(expr.operand),
          operator: 'is',
          right: rightHandSideOfIs
        };

        if (not) {
          return this.negate(expr, mappedIs);
        } else {
          return mappedIs;
        }
      }
      case 'cast': {
        const to = (expr.to as any)?.name?.toLowerCase() as string | undefined;
        if (to == null || !CAST_TYPES.has(to)) {
          return this.invalidExpression(expr.to, 'Invalid SQLite cast');
        } else {
          return { type: 'cast', operand: this.translateNodeWithLocation(expr.operand), cast_as: to as any };
        }
      }
      case 'ternary': {
        const between: BetweenExpression<ExpressionInput> = {
          type: 'between',
          value: this.translateNodeWithLocation(expr.value),
          low: this.translateNodeWithLocation(expr.lo),
          high: this.translateNodeWithLocation(expr.hi)
        };

        return expr.op === 'NOT BETWEEN' ? this.negate(expr, between) : between;
      }
      case 'case': {
        return {
          type: 'case_when',
          operand: expr.value ? this.translateNodeWithLocation(expr.value) : undefined,
          whens: expr.whens.map((when) => ({
            when: this.translateNodeWithLocation(when.when),
            then: this.translateNodeWithLocation(when.value)
          })),
          else: expr.else ? this.translateNodeWithLocation(expr.else) : undefined
        };
      }
      case 'member': {
        const operand = this.translateNodeWithLocation(expr.operand);
        return {
          type: 'function',
          function: expr.op,
          parameters: [
            operand,
            typeof expr.member == 'number'
              ? { type: 'lit_int', base10: expr.member.toString() }
              : { type: 'lit_string', value: expr.member }
          ]
        };
      }
      case 'select':
      case 'union':
      case 'union all':
      case 'with':
      case 'with recursive':
        // Should have been desugared.
        return this.invalidExpression(
          expr,
          'Invalid position for subqueries. Subqueries are only supported in WHERE clauses.'
        );
      default:
        return this.invalidExpression(expr, 'This expression is not supported by PowerSync');
    }
  }

  private invalidExpression(source: PGNode, message: string): LiteralExpression {
    this.options.errors.report(message, source);
    return { type: 'lit_null' };
  }

  private translateInOrOverlapOperator(
    original: ExprBinary,
    type: 'in' | 'overlap',
    negated: boolean
  ): SqlExpression<ExpressionInput> {
    let translatedLeft: SqlExpression<ExpressionInput>;
    let translatedRight: SqlExpression<ExpressionInput>;
    let additionalFilters: SqlExpression<ExpressionInput>[] = [];

    const expand = (expr: Expr): SqlExpression<ExpressionInput> => {
      if (expr.type === 'select') {
        const resolved = this.options.joinSubqueryExpression(expr);
        if (resolved == null) {
          // An error would have been logged.
          const bogusValue: SqlExpression<ExpressionInput> = { type: 'lit_null' };
          this.options.locations.sourceForNode.set(bogusValue, expr);
          return bogusValue;
        }

        additionalFilters.push(...resolved.filters);
        return resolved.output;
      } else {
        if (expr.type == 'ref' && expr.table == null) {
          // This might be a reference to a common table expression, e.g. in  WHERE x IN $cte.
          const cte = this.options.scope.resolveCommonTableExpression(expr.name);
          if (cte) {
            // Translate $cte to (SELECT $onlyColumn FROM $cte)
            const columns = Object.keys(cte.resultColumns);
            if (columns.length != 1) {
              const bogus = this.invalidExpression(expr, 'Common-table expression must return a single column');
              this.options.locations.sourceForNode.set(bogus, expr);
              return bogus;
            }

            return expand({
              type: 'select',
              columns: [
                { expr: { type: 'ref', name: columns[0], table: { name: expr.name }, _location: expr._location } }
              ],
              from: [{ type: 'table', name: { name: expr.name } }]
            });
          }
        }

        // Translate `x IN a` to `x IN (SELECT value FROM json_each(a))`.
        const name = this.options.generateTableAlias();
        return expand({
          type: 'select',
          columns: [{ expr: { type: 'ref', name: 'value', table: { name }, _location: expr._location } }],
          from: [{ type: 'call', function: { name: 'json_each' }, args: [expr], alias: { name } }]
        });
      }
    };

    if (type === 'overlap') {
      translatedLeft = expand(original.left);
    } else {
      // For IN expressions, the left side is always a scalar.
      translatedLeft = this.translateNodeWithLocation(original.left);

      // Additionally, we support IN ARRAY[...] and IN ROW(...) expressions which are always scalar.
      // TODO: We might be able to simplify expressions by translating them into json_array() invocations in expand()?
      if (original.right.type == 'array') {
        return this.desugarInValues(negated, original, translatedLeft, original.right.expressions);
      } else if (original.right.type == 'call' && original.right.function.name.toLowerCase() == 'row') {
        return this.desugarInValues(negated, original, translatedLeft, original.right.args);
      }
    }

    translatedRight = expand(original.right);

    let replacement: SqlExpression<ExpressionInput> = {
      type: 'binary',
      operator: '=',
      left: translatedLeft,
      right: translatedRight
    };
    this.options.locations.sourceForNode.set(replacement, original);

    replacement = additionalFilters.reduce(
      (left, right) => ({ type: 'binary', operator: 'and', left, right }),
      replacement
    );

    if (negated) {
      replacement = this.negate(original, replacement);
    }

    return replacement;
  }

  private desugarInValues(
    negated: boolean,
    source: Expr,
    left: SqlExpression<ExpressionInput>,
    right: Expr[]
  ): SqlExpression<ExpressionInput> {
    const scalarIn: SqlExpression<ExpressionInput> = {
      type: 'scalar_in',
      target: left,
      in: right.map((e) => this.translateNodeWithLocation(e))
    };

    return negated ? this.negate(source, scalarIn) : scalarIn;
  }

  /// Generates a `NOT` wrapper around the `inner` expression, using `source` as a syntactic location.
  private negate(source: Expr, inner: SqlExpression<ExpressionInput>): SqlExpression<ExpressionInput> {
    this.options.locations.sourceForNode.set(inner, source);
    return { type: 'unary', operator: 'not', operand: inner };
  }

  private translateRequestParameter(source: ConnectionParameterSource, expr: ExprCall): SqlExpression<ExpressionInput> {
    const parameter = new ConnectionParameter(expr, source);
    const replacement: SqlExpression<ExpressionInput> = {
      type: 'data',
      source: parameter
    };
    this.options.locations.sourceForNode.set(replacement, expr.function);

    switch (expr.function.name.toLowerCase()) {
      case 'parameters':
        return replacement;
      case 'parameter':
        // Desugar .param(x) into .parameters() ->> '$.' || x
        if (expr.args.length == 1) {
          return {
            type: 'function',
            function: '->>',
            parameters: [replacement, this.translateNodeWithLocation(expr.args[0])]
          };
        } else {
          return this.invalidExpression(expr.function, 'Expected a single argument here');
        }
      case 'user_id':
        if (source == 'auth') {
          // Desugar auth.user_id() into auth.parameters() ->> '$.sub'
          return {
            type: 'function',
            function: '->>',
            parameters: [replacement, { type: 'lit_string', value: '$.sub' }]
          };
        } else {
          return this.invalidExpression(expr.function, '.user_id() is only available on auth schema');
        }
      default:
        return this.invalidExpression(expr.function, 'Unknown request function');
    }
  }
}

const supportedBinaryOperators: Partial<Record<BinaryOperator, SupportedBinaryOperator>> = {
  OR: 'or',
  AND: 'and',
  '=': '=',
  '<': '<',
  '>': '>',
  '<=': '<=',
  '>=': '>=',
  '&': '&',
  '|': '|',
  '<<': '<<',
  '>>': '>>',
  '+': '+',
  '-': '-',
  '*': '*',
  '/': '/',
  '%': '%',
  '||': '||'
};

const forbiddenFunctions: Record<string, string> = {
  random: 'Sync definitions must be deterministic.',
  randomBlob: 'Sync definitions must be deterministic.'
};
