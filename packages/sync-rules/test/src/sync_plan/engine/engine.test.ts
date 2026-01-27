import * as sqlite from 'node:sqlite';

import { describe, expect, onTestFinished, test } from 'vitest';
import {
  CompatibilityContext,
  CompatibilityEdition,
  javaScriptExpressionEngine,
  nodeSqliteExpressionEngine,
  SqliteValue
} from '../../../../src/index.js';
import {
  ScalarExpressionEngine,
  ScalarStatement,
  TableValuedFunction
} from '../../../../src/sync_plan/engine/scalar_expression_engine.js';
import { BinaryOperator } from '../../../../src/sync_plan/expression.js';

describe('sqlite', () => {
  defineEngineTests(false, () => nodeSqliteExpressionEngine(sqlite));
});

describe('javascript', () => {
  defineEngineTests(true, () =>
    javaScriptExpressionEngine(new CompatibilityContext({ edition: CompatibilityEdition.SYNC_STREAMS }))
  );
});

function defineEngineTests(isJavaScript: boolean, createEngine: () => ScalarExpressionEngine) {
  function prepare(stmt: ScalarStatement) {
    const engine = createEngine();
    onTestFinished(() => engine.close());
    return engine.prepareEvaluator(stmt);
  }

  function expectBinary(left: SqliteValue, op: BinaryOperator, right: SqliteValue, output: SqliteValue) {
    const stmt = prepare({
      outputs: [
        {
          type: 'binary',
          left: { type: 'data', source: 1 },
          right: { type: 'data', source: 2 },
          operator: op
        }
      ]
    });

    expect(stmt.evaluate([left, right])).toStrictEqual([[output]]);
  }

  test('literal null', () => {
    expect(prepare({ outputs: [{ type: 'lit_null' }] }).evaluate([])).toStrictEqual([[null]]);
  });

  test('literal double', () => {
    expect(prepare({ outputs: [{ type: 'lit_double', value: 3 }] }).evaluate([])).toStrictEqual([[3]]);
  });

  test('literal int', () => {
    expect(prepare({ outputs: [{ type: 'lit_int', base10: '3' }] }).evaluate([])).toStrictEqual([[3n]]);
  });

  test('literal string', () => {
    expect(prepare({ outputs: [{ type: 'lit_string', value: 'hello world' }] }).evaluate([])).toStrictEqual([
      ['hello world']
    ]);
  });

  test('length(?)', () => {
    const stmt = prepare({
      outputs: [{ type: 'function', function: 'length', parameters: [{ type: 'data', source: 1 }] }]
    });

    expect(stmt.evaluate(['foo'])).toStrictEqual([[3n]]);
  });

  test('cast', () => {
    const stmt = prepare({
      outputs: [{ type: 'cast', operand: { type: 'data', source: 1 }, cast_as: 'text' }]
    });

    expect(stmt.evaluate(['foo'])).toStrictEqual([['foo']]);
    expect(stmt.evaluate([3])).toStrictEqual([[isJavaScript ? '3' : '3.0']]);
  });

  test('unary not', () => {
    const stmt = prepare({ outputs: [{ type: 'unary', operator: 'not', operand: { type: 'data', source: 1 } }] });

    expect(stmt.evaluate([1n])).toStrictEqual([[0n]]);
    expect(stmt.evaluate([0n])).toStrictEqual([[1n]]);
  });

  test('binary', () => {
    expectBinary(1n, '=', 1, 1n);
    expectBinary(3, '+', 4, 7);
    expectBinary(4, 'is', null, 0n);
    expectBinary(null, 'is', null, 1n);

    expectBinary(1, 'or', null, 1n);
    expectBinary(1, 'or', 0n, 1n);
    expectBinary(1, 'and', 0n, 0n);
  });

  test('?1 between ?2 and ?3', () => {
    const stmt = prepare({
      outputs: [
        {
          type: 'between',
          value: { type: 'data', source: 1 },
          low: { type: 'data', source: 2 },
          high: { type: 'data', source: 3 }
        }
      ]
    });

    expect(stmt.evaluate([1, 0, 2])).toStrictEqual([[1n]]);
    expect(stmt.evaluate([1, 2, 3])).toStrictEqual([[0n]]);
    expect(stmt.evaluate([4, 2, 3])).toStrictEqual([[0n]]);
    expect(stmt.evaluate([1, 1, 1])).toStrictEqual([[1n]]);
  });

  test('scalar in', () => {
    // SELECT ?1 IN (?2, ?3)
    const stmt = prepare({
      outputs: [
        {
          type: 'scalar_in',
          target: { type: 'data', source: 1 },
          in: [
            { type: 'data', source: 2 },
            { type: 'data', source: 3 }
          ]
        }
      ]
    });

    expect(stmt.evaluate([1, 2, 3])).toStrictEqual([[0n]]);
    expect(stmt.evaluate([2, 2, 3])).toStrictEqual([[1n]]);
    expect(stmt.evaluate([3, 2, 3])).toStrictEqual([[1n]]);

    expect(stmt.evaluate([null, 2, 3])).toStrictEqual([[null]]);
    expect(stmt.evaluate([1, null, 3])).toStrictEqual([[null]]);
    expect(stmt.evaluate([1, null, null])).toStrictEqual([[null]]);
  });

  test('when without operand', () => {
    // SELECT CASE WHEN ?1 THEN 'a' WHEN ?2 THEN 'b' ELSE 'c' END
    const stmt = prepare({
      outputs: [
        {
          type: 'case_when',
          whens: [
            { when: { type: 'data', source: 1 }, then: { type: 'lit_string', value: 'a' } },
            { when: { type: 'data', source: 2 }, then: { type: 'lit_string', value: 'b' } }
          ],
          else: { type: 'lit_string', value: 'c' }
        }
      ]
    });

    expect(stmt.evaluate([1, 0])).toStrictEqual([['a']]);
    expect(stmt.evaluate([0, 1])).toStrictEqual([['b']]);
    expect(stmt.evaluate([0, 0])).toStrictEqual([['c']]);
    expect(stmt.evaluate([null, 'foo'])).toStrictEqual([['c']]);
  });

  test('when with operand', () => {
    // SELECT CASE ?1 WHEN ?2 THEN 'match' ELSE 'else' END
    const stmt = prepare({
      outputs: [
        {
          type: 'case_when',
          operand: { type: 'data', source: 1 },
          whens: [{ when: { type: 'data', source: 2 }, then: { type: 'lit_string', value: 'match' } }],
          else: { type: 'lit_string', value: 'else' }
        }
      ]
    });

    expect(stmt.evaluate([1, 1])).toStrictEqual([['match']]);
    expect(stmt.evaluate(['foo', 'foo'])).toStrictEqual([['match']]);
    expect(stmt.evaluate(['foo', 'bar'])).toStrictEqual([['else']]);
    expect(stmt.evaluate([null, null])).toStrictEqual([['else']]);
  });

  test('SELECT value FROM json_each(?)', () => {
    const fn: TableValuedFunction = {
      name: 'json_each',
      inputs: [{ type: 'data', source: 1 }]
    };

    const stmt = prepare({
      outputs: [{ type: 'data', source: { function: fn, column: 'value' } }],
      tableValuedFunctions: [fn]
    });
    expect(stmt.evaluate([JSON.stringify([1, 'two', 3.2])])).toStrictEqual([
      [
        // The JavaScript json_each implementation uses JSON.parse. It probably should be using JSONBig, but it's too
        // late to fix that now.
        isJavaScript ? 1 : 1n
      ],
      ['two'],
      [3.2]
    ]);
  });

  test('filters and multiple sources', () => {
    // SELECT a.value, b.value FROM json_each(?1) a, json_each(?2) b where length(a.value || b.value) > 5
    const a: TableValuedFunction = {
      name: 'json_each',
      inputs: [{ type: 'data', source: 1 }]
    };
    const b: TableValuedFunction = {
      name: 'json_each',
      inputs: [{ type: 'data', source: 2 }]
    };

    const stmt = prepare({
      outputs: [
        { type: 'data', source: { function: a, column: 'value' } },
        { type: 'data', source: { function: b, column: 'value' } }
      ],
      tableValuedFunctions: [a, b],
      filters: [
        {
          type: 'binary',
          operator: '>',
          left: {
            type: 'function',
            function: 'length',
            parameters: [
              {
                type: 'binary',
                operator: '||',
                left: { type: 'data', source: { function: a, column: 'value' } },
                right: { type: 'data', source: { function: b, column: 'value' } }
              }
            ]
          },
          right: { type: 'lit_int', base10: '5' }
        }
      ]
    });

    expect(stmt.evaluate([JSON.stringify([]), JSON.stringify(['aaaaaa'])])).toStrictEqual([]);
    expect(stmt.evaluate([JSON.stringify(['x', 'y']), JSON.stringify(['a', 'aaaaa'])])).toStrictEqual([
      ['x', 'aaaaa'],
      ['y', 'aaaaa']
    ]);
  });
}
