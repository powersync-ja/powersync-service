import { SqlRuleError } from '../errors.js';
import { CompiledClause, QuerySchema, StaticValueClause, StreamParseOptions } from '../types.js';
import { isSelectStatement } from '../utils.js';
import {
  andFilters,
  checkUnsupportedFeatures,
  isClauseError,
  isParameterMatchClause,
  isParameterValueClause,
  isRowValueClause,
  isStaticValueClause,
  orFilters
} from '../sql_support.js';
import { TablePattern } from '../TablePattern.js';
import { TableQuerySchema } from '../TableQuerySchema.js';
import { SqlTools } from '../sql_filters.js';
import { BaseSqlDataQuery, BaseSqlDataQueryOptions, RowValueExtractor } from '../BaseSqlDataQuery.js';
import { ExpressionType } from '../ExpressionType.js';
import { SyncStream } from './stream.js';
import {
  And,
  CompareRowValueWithStreamParameter,
  EvaluateSimpleCondition,
  ExistsOperator,
  FilterOperator,
  InOperator,
  Not,
  Or,
  OverlapOperator,
  ScalarExpression,
  Subquery
} from './filter.js';
import {
  Expr,
  ExprBinary,
  nil,
  NodeLocation,
  parse,
  SelectFromStatement,
  SelectStatement,
  Statement
} from 'pgsql-ast-parser';
import { STREAM_FUNCTIONS } from './functions.js';

export function syncStreamFromSql(
  descriptorName: string,
  sql: string,
  options: StreamParseOptions
): [SyncStream, SqlRuleError[]] {
  const compiler = new SyncStreamCompiler(descriptorName, sql, options);
  return [compiler.compile(), compiler.errors];
}

class SyncStreamCompiler {
  descriptorName: string;
  sql: string;
  options: StreamParseOptions;

  errors: SqlRuleError[];

  constructor(descriptorName: string, sql: string, options: StreamParseOptions) {
    this.descriptorName = descriptorName;
    this.sql = sql;
    this.options = options;
    this.errors = [];
  }

  compile(): SyncStream {
    const [stmt, ...illegalRest] = parse(this.sql, { locationTracking: true });

    // TODO: Share more of this code with SqlDataQuery
    if (illegalRest.length > 0) {
      throw new SqlRuleError('Only a single SELECT statement is supported', this.sql, illegalRest[0]?._location);
    }

    const { query, tableRef, alias, querySchema, sourceTable } = this.checkValidSelectStatement(stmt);

    const tools = new SqlTools({
      table: alias,
      parameterTables: [],
      valueTables: [alias],
      sql: this.sql,
      schema: querySchema,
      parameterFunctions: STREAM_FUNCTIONS,
      supportsParameterExpressions: true,
      supportsExpandingParameters: true // needed for table.column IN (subscription.parameters() -> ...)
    });
    tools.checkSpecificNameCase(tableRef);
    let filter = this.whereClauseToFilters(tools, query.where);
    filter = filter.toDisjunctiveNormalForm(tools);

    const stream = new SyncStream(
      this.descriptorName,
      new BaseSqlDataQuery(this.compileDataQuery(tools, query, alias, sourceTable))
    );
    stream.subscribedToByDefault = this.options.default ?? false;
    if (filter.isValid(tools)) {
      stream.variants = filter.compileVariants(this.descriptorName);
    }

    this.errors.push(...tools.errors);
    return stream;
  }

  private compileDataQuery(
    tools: SqlTools,
    query: SelectFromStatement,
    alias: string,
    sourceTable: TablePattern
  ): BaseSqlDataQueryOptions {
    let hasId = false;
    let hasWildcard = false;
    let extractors: RowValueExtractor[] = [];
    const querySchema = tools.schema;

    for (let column of query.columns ?? []) {
      const name = tools.getOutputName(column);
      if (name != '*') {
        const clause = tools.compileRowValueExtractor(column.expr);
        if (isClauseError(clause)) {
          // Error logged already
          continue;
        }
        extractors.push({
          extract: (tables, output) => {
            output[name] = clause.evaluate(tables);
          },
          getTypes(schema, into) {
            const def = clause.getColumnDefinition(schema);

            into[name] = { name, type: def?.type ?? ExpressionType.NONE, originalType: def?.originalType };
          }
        });
      } else {
        extractors.push({
          extract: (tables, output) => {
            const row = tables[alias];
            for (let key in row) {
              if (key.startsWith('_')) {
                continue;
              }
              output[key] ??= row[key];
            }
          },
          getTypes(schema, into) {
            for (let column of schema.getColumns(alias)) {
              into[column.name] ??= column;
            }
          }
        });
      }
      if (name == 'id') {
        hasId = true;
      } else if (name == '*') {
        hasWildcard = true;
        if (querySchema == null) {
          // Not performing schema-based validation - assume there is an id
          hasId = true;
        } else {
          const idType = querySchema.getColumn(alias, 'id')?.type ?? ExpressionType.NONE;
          if (!idType.isNone()) {
            hasId = true;
          }
        }
      }
    }
    if (!hasId) {
      const error = new SqlRuleError(`Query must return an "id" column`, this.sql, query.columns?.[0]._location);
      if (hasWildcard) {
        // Schema-based validations are always warnings
        error.type = 'warning';
      }
      tools.errors.push(error);
    }

    return {
      sourceTable,
      table: alias,
      sql: this.sql,
      columns: query.columns ?? [],
      descriptorName: this.descriptorName,
      tools,
      extractors,
      // Streams don't have traditional parameters, and parameters aren't used in the rest of the stream implementation.
      // Instead, we represent parameters as an array in stream variants.
      bucketParameters: []
    } satisfies BaseSqlDataQueryOptions;
  }

  private checkUnsupportedFeatures(stmt: SelectFromStatement) {
    this.errors.push(...checkUnsupportedFeatures(this.sql, stmt));
  }

  private whereClauseToFilters(tools: SqlTools, clause: Expr | nil): FilterOperator {
    // We need to handle some functions specially here:
    //   1. IN subqueries are not allowed in regular data queries, so we handle them here instead of relying on SqlTools
    //   2. Since IN operators can be composed with other operators using AND and OR, we need to handle those operators
    //      as well.
    // Apart from that we can rely on compileClause
    if (clause != null) {
      if (clause.type == 'binary') {
        let operator, scalarCombinator;

        if (clause.op == 'AND') {
          operator = And;
          scalarCombinator = andFilters;
        } else if (clause.op == 'OR') {
          operator = Or;
          scalarCombinator = orFilters;
        } else if (clause.op == 'IN' || clause.op == 'NOT IN') {
          const filter = this.compileInOperator(tools, clause);
          return clause.op == 'NOT IN' ? new Not(clause._location ?? null, filter) : filter;
        } else if (clause.op == '&&') {
          return this.compileOverlapOperator(tools, clause);
        }

        // Try to combine AND and OR operators on a scalar level first, without introducing more filters.
        if (operator && scalarCombinator) {
          const left = this.whereClauseToFilters(tools, clause.left);
          const right = this.whereClauseToFilters(tools, clause.right);

          if (left instanceof EvaluateSimpleCondition && right instanceof EvaluateSimpleCondition) {
            let directCombination;

            try {
              directCombination = scalarCombinator(left.expression, right.expression);
            } catch (e) {
              // Left and right might be a combination of row and parameter values that can't be combined like this. Ok,
              // we can represent thas as separate filter instances.
            }

            if (directCombination && isScalarExpression(directCombination)) {
              return new EvaluateSimpleCondition(clause._location ?? null, directCombination);
            }
          }

          return new operator(clause._location ?? null, left, right);
        }
      } else if (clause.type == 'unary') {
        if (clause.op == 'NOT') {
          const inner = this.whereClauseToFilters(tools, clause.operand);
          if (inner instanceof EvaluateSimpleCondition) {
            // We can just negate that directly.
            return inner.negate(tools);
          } else {
            return new Not(clause._location ?? null, inner);
          }
        }
      }
    }

    const regularClause = tools.compileClause(clause);
    return compiledClauseToFilter(tools, clause?._location ?? null, regularClause);
  }

  private compileInOperator(tools: SqlTools, clause: ExprBinary): FilterOperator {
    // There are different kinds of `IN` operators we support in stream definitions:
    //
    //   1. Left row clause, right subquery: `WHERE issue_in IN (SELECT id FROM issue WHERE owner_id = request.user())`
    //   2. Left parameter clause, right subquery: `WHERE request.user_id() IN (SELECT * FROM user_id FROM users WHERE is_admin)`.
    //   3. Left parameter clause, right row data: `WHERE request.user() IN comments.tagged_users`.
    //   4. Left row clause, right parameter data: `WHERE id IN subscription_parameters.ids`.
    //   5. Left and right both row clauses, both parameter clauses, or mix or static and row/parameter clauses.
    const left = tools.compileClause(clause.left);
    const location = clause._location ?? null;
    if (isClauseError(left)) {
      return recoverErrorClause(tools);
    }

    if (clause.right.type == 'select') {
      if (!isScalarExpression(left)) {
        if (!isClauseError(left)) {
          tools.error(
            'This may contain values derived from the source row to sync or a value derived from stream parameters, but never both.',
            clause.left
          );
        }

        return recoverErrorClause(tools);
      }

      const subqueryResult = this.compileSubquery(clause.right);
      if (!subqueryResult) {
        return recoverErrorClause(tools);
      }
      const [subquery, subqueryTools] = subqueryResult;

      if (isStaticValueClause(left)) {
        tools.error(
          'For IN subqueries, the left operand must either depend on the row to sync or stream parameters.',
          clause.left
        );
        return recoverErrorClause(tools);
      }

      if (isParameterValueClause(left)) {
        // Case 2: We can't implement this as an actual IN operator because we need to use exact parameter lookups (so
        // we can't, for instance, resolve `SELECT * FROM users WHERE is_admin` via parameter data sets). Since the
        // left clause doesn't depend on row data however, we can push it down into the subquery where it would be
        // introduced as a parameter: `EXISTS (SELECT _ FROM users WHERE is_admin AND user_id = request.user_id())`.
        const additionalClause = subqueryTools.parameterMatchClause(subquery.column, left);
        subquery.addFilter(compiledClauseToFilter(subqueryTools, null, additionalClause));
        return new ExistsOperator(location, subquery);
      } else {
        // Case 1
        return new InOperator(location, left, subquery);
      }
    }

    const right = tools.compileClause(clause.right);

    // For cases 3-5, we can actually use SqlTools.compileClause. Case 3 and 4 are handled specially in there and return
    // a ParameterMatchClause, which we can translate via CompareRowValueWithStreamParameter. Case 5 is either a row-value
    // or a parameter-value clause which we can wrap in EvaluateSimpleCondition.
    const combined = tools.compileInClause(clause.left, left, clause.right, right);
    return compiledClauseToFilter(tools, location, combined);
  }

  private compileOverlapOperator(tools: SqlTools, clause: ExprBinary): FilterOperator {
    const left = tools.compileClause(clause.left);
    const location = clause._location ?? null;

    if (isClauseError(left)) {
      return recoverErrorClause(tools);
    }

    if (clause.right.type == 'select') {
      if (!isRowValueClause(left)) {
        if (!isClauseError(left)) {
          tools.error('The left-hand side of an && operator must be derived from the row to sync..', clause.left);
        }

        return recoverErrorClause(tools);
      }

      const subqueryResult = this.compileSubquery(clause.right);
      if (!subqueryResult) {
        return recoverErrorClause(tools);
      }
      const [subquery] = subqueryResult;
      return new OverlapOperator(location, left, subquery);
    }

    const right = tools.compileClause(clause.right);

    // For cases 3-5, we can actually uses SqlTools.compileClause. Case 3 and 4 are handled specially in there and return
    // a ParameterMatchClause, which we can translate via CompareRowValueWithStreamParameter. Case 5 is either a row-value
    // or a parameter-value clause which we can wrap in EvaluateSimpleCondition.
    const combined = tools.compileOverlapClause(clause.left, left, clause.right, right);
    return compiledClauseToFilter(tools, location, combined);
  }

  private compileSubquery(stmt: SelectStatement): [Subquery, SqlTools] | undefined {
    // A subquery is similar to a data query in legacy sync rules. Importantly, despite being an expression, subqueries
    // can't reference columns from the outer query! The syntax is always `SELECT <single column> FROM <table> WHERE
    // <compiled clause>`.
    let validated;
    try {
      validated = this.checkValidSelectStatement(stmt);
    } catch (e) {
      if (e instanceof SqlRuleError) {
        this.errors.push(e);
      }
      return undefined;
    }

    const { query, alias, querySchema, tableRef, sourceTable } = validated;
    // Create a new tools instance for this - the subquery does not have access to the outer one.
    const tools = new SqlTools({
      table: alias,
      parameterTables: [],
      valueTables: [alias],
      sql: this.sql,
      schema: querySchema,
      supportsParameterExpressions: true,
      parameterFunctions: STREAM_FUNCTIONS
    });
    tools.checkSpecificNameCase(tableRef);

    if (query.columns?.length != 1) {
      tools.error('This subquery must return exactly one column', query);
    }

    const column = tools.compileRowValueExtractor(query.columns?.[0]?.expr);
    if (isClauseError(column)) {
      return;
    }

    const where = tools.compileClause(query.where);

    this.errors.push(...tools.errors);
    return [new Subquery(sourceTable, column, compiledClauseToFilter(tools, query.where?._location, where)), tools];
  }

  private checkValidSelectStatement(stmt: Statement) {
    if (!isSelectStatement(stmt)) {
      throw new SqlRuleError('Only SELECT statements are supported', this.sql, stmt._location);
    }

    if (stmt.from == null || stmt.from.length != 1 || stmt.from[0].type != 'table') {
      throw new SqlRuleError('Must SELECT from a single table', this.sql, stmt);
    }

    this.checkUnsupportedFeatures(stmt);

    const tableRef = stmt.from?.[0].name;
    if (tableRef?.name == null) {
      throw new SqlRuleError('Must SELECT from a single table', this.sql, stmt.from?.[0]._location);
    }
    const alias: string = tableRef.alias ?? tableRef.name;

    const sourceTable = new TablePattern(tableRef.schema ?? this.options.defaultSchema, tableRef.name);
    let querySchema: QuerySchema | undefined = undefined;
    const schema = this.options.schema;
    if (schema) {
      const tables = schema.getTables(sourceTable);
      if (tables.length == 0) {
        const e = new SqlRuleError(
          `Table ${sourceTable.schema}.${sourceTable.tablePattern} not found`,
          this.sql,
          stmt.from?.[0]?._location
        );
        e.type = 'warning';

        this.errors.push(e);
      } else {
        querySchema = new TableQuerySchema(tables, alias);
      }
    }

    return {
      query: stmt,
      tableRef,
      alias,
      querySchema,
      sourceTable
    };
  }
}

function isScalarExpression(clause: CompiledClause): clause is ScalarExpression {
  return isRowValueClause(clause) || isStaticValueClause(clause) || isParameterValueClause(clause);
}

function recoverErrorClause(tools: SqlTools): EvaluateSimpleCondition {
  // An error has already been logged.
  return new EvaluateSimpleCondition(null, tools.compileClause(null) as StaticValueClause);
}

function compiledClauseToFilter(tools: SqlTools, location: NodeLocation | nil, regularClause: CompiledClause) {
  if (isScalarExpression(regularClause)) {
    return new EvaluateSimpleCondition(location ?? null, regularClause);
  } else if (isParameterMatchClause(regularClause)) {
    return new CompareRowValueWithStreamParameter(location ?? null, regularClause);
  } else if (isClauseError(regularClause)) {
    return recoverErrorClause(tools);
  } else {
    throw new Error('Unknown clause type');
  }
}
