import {
  cast,
  compare,
  CompatibilityContext,
  generateSqlFunctions,
  SQLITE_FALSE,
  SQLITE_TRUE,
  sqliteBool,
  sqliteNot
} from '../../index.js';
import { evaluateOperator, SqlFunction } from '../../sql_functions.js';
import { cartesianProduct } from '../../streams/utils.js';
import { generateTableValuedFunctions } from '../../TableValuedFunctions.js';
import { SqliteRow, SqliteValue } from '../../types.js';
import {
  ExternalData,
  UnaryExpression,
  BinaryExpression,
  BetweenExpression,
  ScalarInExpression,
  CaseWhenExpression,
  CastExpression,
  ScalarFunctionCallExpression,
  LiteralExpression,
  SqlExpression
} from '../expression.js';
import { ExpressionVisitor, visitExpr } from '../expression_visitor.js';
import {
  ScalarExpressionEngine,
  ScalarExpressionEvaluator,
  TableValuedFunction,
  TableValuedFunctionOutput
} from './scalar_expression_engine.js';

/**
 * Creates a {@link ScalarExpressionEngine} implemented by evaluating scalar expressions in JavaScript.
 */
export function javaScriptExpressionEngine(compatibility: CompatibilityContext): ScalarExpressionEngine {
  const tableValued = generateTableValuedFunctions(compatibility);
  const regularFunctions = generateSqlFunctions(compatibility);
  const compiler = new ExpressionToJavaScriptFunction({
    named: regularFunctions.named,
    jsonExtractJson: regularFunctions.operatorJsonExtractJson,
    jsonExtractSql: regularFunctions.operatorJsonExtractSql
  });

  return {
    close() {},
    prepareEvaluator({ outputs = [], filters = [], tableValuedFunctions = [] }): ScalarExpressionEvaluator {
      function compileScalar(expr: SqlExpression<number | TableValuedFunctionOutput>) {
        return compiler.compile(expr);
      }

      const resolvedTableValuedFunctions = tableValuedFunctions.map((fn) => {
        const found = tableValued[fn.name];
        if (found == null) {
          throw new Error(`Unknown table-valued function: ${fn.name}`);
        }

        const inputs = fn.inputs.map(compileScalar);
        return {
          original: fn,
          evaluate: (input: PendingStatementEvaluation) => {
            return found.call(inputs.map((f) => f(input)));
          }
        };
      });

      const columns = outputs.map(compileScalar);
      const compiledFilters = filters.map(compileScalar);

      return {
        evaluate(inputs) {
          // First, evaluate table-valued functions (if any).
          const perFunctionResults: [TableValuedFunction, SqliteRow][][] = [];

          for (const { original, evaluate } of resolvedTableValuedFunctions) {
            perFunctionResults.push(
              evaluate({ inputs }).map((row) => [original, row] satisfies [TableValuedFunction, SqliteRow])
            );
          }

          const rows: SqliteValue[][] = [];
          // We're doing an inner join on all table-valued functions, which we implement as a cross join on which each
          // filter is evaluated. Having more than one table-valued function per statement would be very rare in
          // practice.
          row: for (const sourceRow of cartesianProduct(...perFunctionResults)) {
            const byFunction = new Map<TableValuedFunction, SqliteRow>();
            for (const [fn, output] of sourceRow) {
              byFunction.set(fn, output);
            }

            const input: PendingStatementEvaluation = { inputs, row: byFunction };

            for (const filter of compiledFilters) {
              if (!sqliteBool(filter(input))) {
                continue row;
              }
            }
            rows.push(columns.map((c) => c(input)));
          }

          return rows;
        }
      };
    }
  };
}

interface PendingStatementEvaluation {
  inputs: SqliteValue[];
  row?: Map<TableValuedFunction, SqliteRow>;
}

type ExpressionImplementation = (input: PendingStatementEvaluation) => SqliteValue;

interface KnownFunctions {
  named: Record<string, SqlFunction>;
  jsonExtractJson: SqlFunction; // -> operator
  jsonExtractSql: SqlFunction; // ->> operator
}

class ExpressionToJavaScriptFunction
  implements ExpressionVisitor<number | TableValuedFunctionOutput, ExpressionImplementation, null>
{
  constructor(readonly functions: KnownFunctions) {}

  compile(expr: SqlExpression<number | TableValuedFunctionOutput>): ExpressionImplementation {
    return visitExpr(this, expr, null);
  }

  visitExternalData(expr: ExternalData<number | TableValuedFunctionOutput>, arg: null): ExpressionImplementation {
    if (typeof expr.source === 'number') {
      const index = expr.source;
      //  -1 because variables in SQLite are 1-indexed.
      return ({ inputs }) => inputs[index - 1];
    } else {
      const { column, function: fn } = expr.source;

      return ({ row }) => {
        const result = row!.get(fn)!;
        return result[column];
      };
    }
  }

  visitUnaryExpression(expr: UnaryExpression<number | TableValuedFunctionOutput>): ExpressionImplementation {
    const operand = this.compile(expr.operand);

    switch (expr.operator) {
      case '+':
        return operand;
      case 'not':
        return (input) => sqliteNot(operand(input));
      // case '~':
      // case '-':
      //   throw new Error(`unary operator not supported: ${expr.operator}`);
    }
  }

  visitBinaryExpression(expr: BinaryExpression<number | TableValuedFunctionOutput>): ExpressionImplementation {
    const left = this.compile(expr.left);
    const right = this.compile(expr.right);
    const operator = expr.operator.toUpperCase();

    return (input) => evaluateOperator(operator, left(input), right(input));
  }

  visitBetweenExpression(expr: BetweenExpression<number | TableValuedFunctionOutput>): ExpressionImplementation {
    const low = this.compile(expr.low);
    const high = this.compile(expr.high);
    const value = this.compile(expr.value);

    return (input) => {
      const evaluatedValue = value(input);

      const geqLow = evaluateOperator('>=', evaluatedValue, low(input));
      const leqHigh = evaluateOperator('<=', evaluatedValue, high(input));
      if (geqLow == null || leqHigh == null) {
        return null;
      }

      return sqliteBool(geqLow) && sqliteBool(leqHigh);
    };
  }

  visitScalarInExpression(expr: ScalarInExpression<number | TableValuedFunctionOutput>): ExpressionImplementation {
    const target = this.compile(expr.target);
    const inQuery = expr.in.map((q) => this.compile(q));

    return (input) => {
      const evaluatedTarget = target(input);
      if (evaluatedTarget == null) {
        return null;
      }

      let hasNullQuery = false;
      for (const q of inQuery) {
        const evaluated = q(input);
        if (evaluated == null) {
          hasNullQuery = true;
          continue;
        }

        if (compare(evaluatedTarget, evaluated) == 0) {
          return SQLITE_TRUE;
        }
      }

      return hasNullQuery ? null : SQLITE_FALSE;
    };
  }

  visitCaseWhenExpression(expr: CaseWhenExpression<number | TableValuedFunctionOutput>): ExpressionImplementation {
    const compiledWhens = expr.whens.map((w) => ({ when: this.compile(w.when), then: this.compile(w.then) }));
    const compiledElse = expr.else && this.compile(expr.else);

    if (expr.operand) {
      const operand = this.compile(expr.operand);
      return (input) => {
        const evaluatedOperand = operand(input);

        for (const { when, then } of compiledWhens) {
          if (evaluateOperator('=', evaluatedOperand, when(input))) {
            return then(input);
          }
        }

        return compiledElse ? compiledElse(input) : null;
      };
    } else {
      return (input) => {
        for (const { when, then } of compiledWhens) {
          if (sqliteBool(when(input))) {
            return then(input);
          }
        }

        return compiledElse ? compiledElse(input) : null;
      };
    }
  }

  visitCastExpression(expr: CastExpression<number | TableValuedFunctionOutput>): ExpressionImplementation {
    const operand = this.compile(expr.operand);
    return (input) => {
      return cast(operand(input), expr.cast_as);
    };
  }

  visitScalarFunctionCallExpression(
    expr: ScalarFunctionCallExpression<number | TableValuedFunctionOutput>
  ): ExpressionImplementation {
    let fnImpl: SqlFunction;
    if (expr.function === '->') {
      fnImpl = this.functions.jsonExtractJson;
    } else if (expr.function === '->>') {
      fnImpl = this.functions.jsonExtractSql;
    } else {
      fnImpl = this.functions.named[expr.function];
      if (!fnImpl) {
        throw new Error(`Function not implemented: ${expr.function}`);
      }
    }

    const args = expr.parameters.map((p) => this.compile(p));
    return (input) => {
      return fnImpl.call(...args.map((f) => f(input)));
    };
  }

  visitLiteralExpression(expr: LiteralExpression): ExpressionImplementation {
    switch (expr.type) {
      case 'lit_null':
        return () => null;
      case 'lit_double':
        return () => expr.value;
      case 'lit_int':
        return () => BigInt(expr.base10);
      case 'lit_string':
        return () => expr.value;
    }
  }
}
