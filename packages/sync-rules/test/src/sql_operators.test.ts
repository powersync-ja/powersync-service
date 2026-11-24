import { JSONBig } from '@powersync/service-jsonbig';
import { describe, expect, test } from 'vitest';
import {
  CompatibilityContext,
  DEFAULT_HYDRATION_STATE,
  evaluateOperator,
  ImplicitSchemaTablePattern,
  PrecompiledSyncConfig,
  SqliteRow,
  SqliteValue,
  StreamDataSource,
  TableProcessorData
} from '../../src/index.js';
import { BinaryOperator, SqlExpression } from '../../src/sync_plan/expression.js';
import { createRustEvaluator, syncTest, TEST_TABLE } from './sync_plan/evaluator/utils.js';

type Expression = SqlExpression<TableProcessorData>;

const operatorTest = syncTest.extend('compileOperator', ({ rust }) => {
  return function (jsOperator: string, buildExpr: (a: Expression, b: Expression) => Expression) {
    const dataSource: StreamDataSource = {
      columns: [
        {
          alias: 'output',
          expr: buildExpr({ type: 'data', source: { column: 'a' } }, { type: 'data', source: { column: 'b' } })
        }
      ],
      hashCode: 0,
      sourceTable: new ImplicitSchemaTablePattern(null, 'tbl'),
      filters: [],
      parameters: [],
      tableValuedFunctions: []
    };
    const config = new PrecompiledSyncConfig(
      {
        dataSources: [dataSource],
        buckets: [
          {
            hashCode: 0,
            uniqueName: 'stream',
            sources: [dataSource]
          }
        ],
        parameterIndexes: [],
        streams: [],
        events: []
      },
      CompatibilityContext.FULL_BACKWARDS_COMPATIBILITY,
      { defaultSchema: 'test_schema', sourceText: '' }
    );

    const evaluator = createRustEvaluator(
      rust,
      config.hydrate({
        sqlite: null,
        hydrationState: DEFAULT_HYDRATION_STATE
      })
    )!;

    function evaluateWithRust(a: SqliteValue, b: SqliteValue) {
      const { rows: rustRows, errors: rustErrors } = evaluator.evaluateSerialized(TEST_TABLE, { a, b });

      expect(rustErrors).toStrictEqual([]);
      return (JSONBig.parse(rustRows[0].data) as SqliteRow).output;
    }

    return function (a: SqliteValue, b: SqliteValue, allowDivergenceBetweenJavaScriptAndRust = false) {
      const rust = evaluateWithRust(a, b);
      const js = evaluateOperator(jsOperator, a, b);

      if (!allowDivergenceBetweenJavaScriptAndRust) expect(rust).toStrictEqual(js);
      return rust;
    };
  };
});

function buildBinaryExpression(op: BinaryOperator) {
  return (a: Expression, b: Expression) => ({ type: 'binary', left: a, right: b, operator: op }) satisfies Expression;
}

function negate(inner: (a: Expression, b: Expression) => Expression) {
  return (a: Expression, b: Expression) =>
    ({ type: 'unary', operator: 'not', operand: inner(a, b) }) satisfies Expression;
}

describe('SQL operators', () => {
  syncTest('truthiness', ({ sync }) => {
    const plan = sync.prepareSyncStreams(`
config:
  edition: 3
streams:
  a:
    query: SELECT value AS id FROM tbl WHERE value
`);

    function isTruthy(value: SqliteValue) {
      // prepareSyncStreams injects a check to ensure the Rust and JS evaluator agree.
      const rows = plan.evaluateRow({
        sourceTable: { connectionTag: 'default', schema: 'test_schema', name: 'tbl' },
        record: { value }
      });
      return rows.length != 0;
    }

    expect(isTruthy(null)).toStrictEqual(false);
    expect(isTruthy(1)).toStrictEqual(true);
    expect(isTruthy(-1n)).toStrictEqual(true);
    expect(isTruthy(0n)).toStrictEqual(false);
    expect(isTruthy(0.5)).toStrictEqual(true);
    expect(isTruthy(0.0)).toStrictEqual(false);

    // Text is converted to a number by parsing its numeric prefix.
    for (const truthy of ['1', ' 1abc', '-2', '1e3', '5.', '+3']) {
      expect(isTruthy(truthy), truthy).toStrictEqual(true);
    }
    for (const falsy of ['', 'abc', '0', '0.0', '-', '.', 'e5', '0e9', ' ']) {
      expect(isTruthy(falsy), falsy).toStrictEqual(false);
    }

    expect(isTruthy(Uint8Array.of(0xff))).toStrictEqual(false);

    // Known mismatches: Rust follows SQLite for reals in text (e.g. '0.1' or '.5') and blobs, treating them as truthy
    // if their numeric prefix is non-zero. JavaScript uses parseInt and treats all blobs as false.
  });

  operatorTest('=', ({ compileOperator }) => {
    const evaluate = compileOperator('=', buildBinaryExpression('='));

    expect(evaluate(1, 1n)).toStrictEqual(1n);
    expect(evaluate(1.1, 1n)).toStrictEqual(0n);
    expect(evaluate('1', 1n)).toStrictEqual(0n);
    expect(evaluate('', 0n)).toStrictEqual(0n);
    expect(evaluate(1, null)).toStrictEqual(null);
    expect(evaluate(0, null)).toStrictEqual(null);
    expect(evaluate(null, '')).toStrictEqual(null);
    expect(evaluate(null, null)).toStrictEqual(null);
  });

  operatorTest('!=', ({ compileOperator }) => {
    const evaluate = compileOperator('!=', negate(buildBinaryExpression('=')));

    expect(evaluate(1, 1n)).toStrictEqual(0n);
    expect(evaluate(1.1, 1n)).toStrictEqual(1n);
    expect(evaluate('1', 1n)).toStrictEqual(1n);
    expect(evaluate('', 0n)).toStrictEqual(1n);
    expect(evaluate(1, null)).toStrictEqual(null);
    expect(evaluate(0, null)).toStrictEqual(null);
    expect(evaluate(null, '')).toStrictEqual(null);
    expect(evaluate(null, null)).toStrictEqual(null);
  });

  operatorTest('IS', ({ compileOperator }) => {
    const evaluate = compileOperator('IS', buildBinaryExpression('is'));

    expect(evaluate(1, 1n)).toStrictEqual(1n);
    expect(evaluate(1.1, 1n)).toStrictEqual(0n);
    expect(evaluate('1', 1n)).toStrictEqual(0n);
    expect(evaluate('', 0n)).toStrictEqual(0n);
    expect(evaluate(1, null)).toStrictEqual(0n);
    expect(evaluate(0, null)).toStrictEqual(0n);
    expect(evaluate(null, '')).toStrictEqual(0n);
    expect(evaluate(null, null)).toStrictEqual(1n);
  });

  operatorTest('IS NOT', ({ compileOperator }) => {
    const evaluate = compileOperator('IS NOT', negate(buildBinaryExpression('is')));

    expect(evaluate(1, 1n)).toStrictEqual(0n);
    expect(evaluate(1.1, 1n)).toStrictEqual(1n);
    expect(evaluate('1', 1n)).toStrictEqual(1n);
    expect(evaluate('', 0n)).toStrictEqual(1n);
    expect(evaluate(1, null)).toStrictEqual(1n);
    expect(evaluate(0, null)).toStrictEqual(1n);
    expect(evaluate(null, '')).toStrictEqual(1n);
    expect(evaluate(null, null)).toStrictEqual(0n);
  });

  operatorTest('>', ({ compileOperator }) => {
    const evaluate = compileOperator('>', buildBinaryExpression('>'));

    expect(evaluate(1, 1n)).toStrictEqual(0n);
    expect(evaluate(2, 1n)).toStrictEqual(1n);
    expect(evaluate(2n, 1)).toStrictEqual(1n);
    expect(evaluate('text', 1)).toStrictEqual(1n);
    expect(evaluate(1, 'text')).toStrictEqual(0n);
    expect(evaluate(1, null)).toStrictEqual(null);
    expect(evaluate(null, null)).toStrictEqual(null);
  });

  operatorTest('>=', ({ compileOperator }) => {
    const evaluate = compileOperator('>=', buildBinaryExpression('>='));

    expect(evaluate(1, 1n)).toStrictEqual(1n);
    expect(evaluate(2, 1n)).toStrictEqual(1n);
    expect(evaluate(2n, 1)).toStrictEqual(1n);
    expect(evaluate('text', 1)).toStrictEqual(1n);
    expect(evaluate(1, 'text')).toStrictEqual(0n);
    expect(evaluate(1, null)).toStrictEqual(null);
    expect(evaluate(null, null)).toStrictEqual(null);
  });

  operatorTest('<', ({ compileOperator }) => {
    const evaluate = compileOperator('<', buildBinaryExpression('<'));

    expect(evaluate(1, 1n)).toStrictEqual(0n);
    expect(evaluate(1n, 2)).toStrictEqual(1n);
    expect(evaluate(1, 2n)).toStrictEqual(1n);
    expect(evaluate('text', 1)).toStrictEqual(0n);
    expect(evaluate(1, 'text')).toStrictEqual(1n);
    expect(evaluate(1, null)).toStrictEqual(null);
    expect(evaluate(null, null)).toStrictEqual(null);
  });

  operatorTest('<=', ({ compileOperator }) => {
    const evaluate = compileOperator('<=', buildBinaryExpression('<='));

    expect(evaluate(1, 1n)).toStrictEqual(1n);
    expect(evaluate(1n, 2)).toStrictEqual(1n);
    expect(evaluate(1, 2n)).toStrictEqual(1n);
    expect(evaluate('text', 1)).toStrictEqual(0n);
    expect(evaluate(1, 'text')).toStrictEqual(1n);
    expect(evaluate(1, null)).toStrictEqual(null);
    expect(evaluate(null, null)).toStrictEqual(null);
  });

  operatorTest('||', ({ compileOperator }) => {
    const evaluate = compileOperator('||', buildBinaryExpression('||'));

    expect(evaluate('test', null)).toStrictEqual(null);
    expect(evaluate('a', 31n)).toStrictEqual('a31');
    expect(evaluate(3.1, 'a')).toStrictEqual('3.1a');
  });

  operatorTest('+', ({ compileOperator }) => {
    const evaluate = compileOperator('+', buildBinaryExpression('+'));

    expect(evaluate(1n, null)).toStrictEqual(null);
    expect(evaluate(1n, 2)).toStrictEqual(3);
    expect(evaluate(1n, 2n)).toStrictEqual(3n);
    expect(evaluate(1, 2)).toStrictEqual(3);
    expect(evaluate(1n, '2')).toStrictEqual(3n);
    expect(evaluate(9223372036854775807n, -1n)).toStrictEqual(9223372036854775806n);

    // i64 overflow, should use floats (not implemented correctly in JS).
    expect(evaluate(9223372036854775807n, 1n, true)).toStrictEqual(9.223372036854776e18);
  });

  operatorTest('/', ({ compileOperator }) => {
    const evaluate = compileOperator('/', buildBinaryExpression('/'));

    expect(evaluate(1n, null)).toStrictEqual(null);
    expect(evaluate(5n, 2)).toStrictEqual(2.5);
    expect(evaluate(5n, 2n)).toStrictEqual(2n);
    expect(evaluate(5, 2)).toStrictEqual(2.5);
    expect(evaluate(5n, '2')).toStrictEqual(2n);
    expect(evaluate(5n, '2.0')).toStrictEqual(2.5);
  });

  operatorTest('division by zero follows SQLite null semantics', ({ compileOperator }) => {
    const evaluate = compileOperator('/', buildBinaryExpression('/'));

    expect(evaluate(5n, 0n)).toStrictEqual(null);
    expect(evaluate(5n, 0)).toStrictEqual(null);
    expect(evaluate(5, 0)).toStrictEqual(null);
    expect(evaluate(-5n, '0')).toStrictEqual(null);
    expect(evaluate(0n, 0n)).toStrictEqual(null);
  });

  operatorTest('*', ({ compileOperator }) => {
    const evaluate = compileOperator('*', buildBinaryExpression('*'));

    expect(evaluate(3n, null)).toStrictEqual(null);
    expect(evaluate(3n, 2)).toStrictEqual(6);
    expect(evaluate(3n, 2n)).toStrictEqual(6n);
    expect(evaluate(3, 2)).toStrictEqual(6);
    expect(evaluate(3n, '2')).toStrictEqual(6n);
    expect(evaluate(3n, '2.0')).toStrictEqual(6.0);
    expect(evaluate('3', '2.0')).toStrictEqual(6.0);
  });

  operatorTest('-', ({ compileOperator }) => {
    const evaluate = compileOperator('-', buildBinaryExpression('-'));

    expect(evaluate(3n, null)).toStrictEqual(null);
    expect(evaluate(3n, 2)).toStrictEqual(1);
    expect(evaluate(3n, 2n)).toStrictEqual(1n);
    expect(evaluate(3, 2)).toStrictEqual(1);
    expect(evaluate(3n, '2')).toStrictEqual(1n);
    expect(evaluate(3n, '2.0')).toStrictEqual(1.0);
    expect(evaluate('3', '2.0')).toStrictEqual(1.0);
  });

  test('&&', () => {
    // This is the overlap operator, which is implemented differently for Sync Streams.

    // null short-circuit
    expect(evaluateOperator('&&', null, '["foo"]')).toStrictEqual(null);
    expect(evaluateOperator('&&', '["foo"]', null)).toStrictEqual(null);

    // disjoint arrays
    expect(evaluateOperator('&&', '["bar"]', '["foo"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '[]', '["foo"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '["foo"]', '[]')).toStrictEqual(0n);

    // overlapping arrays
    expect(evaluateOperator('&&', '["foo"]', '["foo"]')).toStrictEqual(1n);
    expect(evaluateOperator('&&', '["bar","foo"]', '["foo"]')).toStrictEqual(1n);
    expect(evaluateOperator('&&', '["foo"]', '["bar","foo"]')).toStrictEqual(1n);

    // regression: index-like string on the left must not satisfy any right-hand array.
    // The previous implementation iterated indexes of the left array, so "0" would
    // overlap any non-empty right-hand array.
    expect(evaluateOperator('&&', '["0"]', '["foo"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '["0","1"]', '["foo","bar"]')).toStrictEqual(0n);
    expect(evaluateOperator('&&', '["0"]', '["0"]')).toStrictEqual(1n);

    // numeric affinity: bigint on one side, number on the other should still match.
    expect(evaluateOperator('&&', '[1]', '[1]')).toStrictEqual(1n);
    expect(evaluateOperator('&&', '[1,2,3]', '[4,3]')).toStrictEqual(1n);
  });
});
