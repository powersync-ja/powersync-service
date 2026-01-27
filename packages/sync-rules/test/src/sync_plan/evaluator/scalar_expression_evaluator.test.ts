import * as sqlite from 'node:sqlite';
import { describe, expect, onTestFinished, test } from 'vitest';
import {
  nodeSqliteExpressionEngine,
  scalarStatementToSql,
  TableValuedFunction
} from '../../../../src/sync_plan/evaluator/scalar_expression_evaluator.js';

describe('scalarStatementToSql', () => {
  test('empty', () => {
    expect(scalarStatementToSql({})).toStrictEqual('SELECT 1');
  });

  test('outputs', () => {
    expect(
      scalarStatementToSql({
        outputs: [
          {
            type: 'function',
            function: 'foo',
            parameters: [
              { type: 'data', source: 1 },
              { type: 'lit_string', value: 'hello' }
            ]
          }
        ]
      })
    ).toStrictEqual(`SELECT "foo"(?1, 'hello')`);
  });

  test('filters', () => {
    expect(
      scalarStatementToSql({
        filters: [
          {
            type: 'lit_int',
            base10: '1'
          }
        ]
      })
    ).toStrictEqual(`SELECT 1 WHERE 1`);
  });

  test('output and filters', () => {
    expect(
      scalarStatementToSql({
        outputs: [
          {
            type: 'lit_string',
            value: 'foo'
          }
        ],
        filters: [
          {
            type: 'function',
            function: 'foo',
            parameters: []
          }
        ]
      })
    ).toStrictEqual(`SELECT 'foo' WHERE "foo"()`);
  });

  test('table-valued functions', () => {
    const fn: TableValuedFunction = {
      name: 'json_each',
      inputs: [{ type: 'data', source: 1 }]
    };

    expect(
      scalarStatementToSql({
        outputs: [
          {
            type: 'data',
            source: { function: fn, column: 'value' }
          }
        ],
        filters: [
          {
            type: 'function',
            function: 'foo',
            parameters: [
              {
                type: 'data',
                source: { function: fn, column: 'key' }
              }
            ]
          }
        ],
        tableValuedFunctions: [fn]
      })
    ).toStrictEqual(`SELECT "tbl_0"."value" FROM "json_each"(?1) AS "tbl_0" WHERE "foo"("tbl_0"."key")`);
  });
});

test('nodeSqliteExpressionEngine', () => {
  const engine = nodeSqliteExpressionEngine(sqlite);
  onTestFinished(() => engine.close());

  const fn: TableValuedFunction = {
    name: 'json_each',
    inputs: [{ type: 'data', source: 1 }]
  };

  const stmt = engine.prepareEvaluator({
    outputs: [{ type: 'data', source: { function: fn, column: 'value' } }],
    tableValuedFunctions: [fn]
  });

  expect(stmt.evaluate([JSON.stringify([1, 'two', 3.2])])).toStrictEqual([[1n], ['two'], [3.2]]);
});
