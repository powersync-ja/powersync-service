import { Expr, parse } from 'pgsql-ast-parser';
import { PostgresToSqlite } from '../../../src/compiler/sqlite.js';
import { describe, expect, test } from 'vitest';
import { getLocation } from '../../../src/errors.js';
import { ExpressionToSqlite } from '../../../src/sync_plan/expression_to_sql.js';
import { NodeLocations } from '../../../src/compiler/expression.js';
import { SqlScope } from '../../../src/compiler/scope.js';

describe('sqlite conversion', () => {
  test('literals', () => {
    expectNoErrors('null', 'NULL');
    expectNoErrors('true', '1');
    expectNoErrors('false', '0');

    expectNoErrors("''", "''");
    expectNoErrors("'hello world'", "'hello world'");
    expectNoErrors("'escaped''string'", "'escaped''string'");

    expectNoErrors('0', '0');
    expectNoErrors('-0', '-0');
    expectNoErrors('123.45', '123.45');
    expectNoErrors('9223372036854775808', '9223372036854775808');
  });

  test('substring', () => {
    expectNoErrors("substring('val' from 2 for 3)", `"substr"('val', 2, 3)`);
    expectNoErrors("substring('PowerSync' for 5)", `"substr"('PowerSync', 1, 5)`);
    expectNoErrors("substring('PowerSync' from 6)", `"substr"('PowerSync', 6)`);
  });

  test('in values', () => {
    expectNoErrors('1 IN ARRAY[1,2,3]', '1 IN (1, 2, 3)');
    expectNoErrors('1 IN ARRAY[]', 'FALSE');

    expectNoErrors('1 NOT IN ROW(1, 2, 3)', 'not 1 IN (1, 2, 3)');
  });

  test('precedence', () => {
    expectNoErrors('(2+3) * 4', '(2 + 3) * 4');
  });

  test('cast', () => {
    expectNoErrors('CAST(123 AS REAL)', 'CAST(123 AS real)');
  });

  test('ternary', () => {
    expectNoErrors('3 BETWEEN 4 AND 5', '3 BETWEEN 4 AND 5');
  });

  test('case', () => {
    expectNoErrors('CASE WHEN 1 THEN 2 ELSE 3 END', 'CASE WHEN 1 THEN 2 ELSE 3 END');
    expectNoErrors('CASE 1 WHEN 2 THEN 3 ELSE 4 END', 'CASE 1 WHEN 2 THEN 3 ELSE 4 END');

    expectNoErrors('CASE WHEN 1 THEN 2 END', 'CASE WHEN 1 THEN 2 END');
    expectNoErrors('CASE 1 WHEN 2 THEN 3 END', 'CASE 1 WHEN 2 THEN 3 END');
  });

  test('extract', () => {
    expectNoErrors('1 ->> 2', '"->>"(1, 2)');
    expectNoErrors("1 -> '$.foo'", `"->"(1, '$.foo')`);
  });

  test('unary', () => {
    expectNoErrors('1 IS NOT NULL', 'not 1 is NULL');
    expectNoErrors('NOT 1', 'not 1');
  });

  describe('errors', () => {
    describe('function', () => {
      test('too few args', () => {
        expect(translate('coalesce(123)')[1]).toStrictEqual([
          {
            message: 'Expected at least 2 arguments',
            source: 'coalesce(123)'
          }
        ]);
      });

      test('too many args', () => {
        expect(translate('round(1.23, 4, 5)')[1]).toStrictEqual([
          {
            message: 'Expected at most 2 arguments',
            source: 'round(1.23, 4, 5)'
          }
        ]);
      });

      test('must be even', () => {
        expect(translate("json_object('foo')")[1]).toStrictEqual([
          {
            message: 'Expected an even amount of arguments',
            source: "json_object('foo')"
          }
        ]);
      });

      test('must be odd', () => {
        expect(translate("json_insert('foo', 'path', 'val', 'path2')")[1]).toStrictEqual([
          {
            message: 'Expected an odd amount of arguments',
            source: "json_insert('foo', 'path', 'val', 'path2')"
          }
        ]);
      });

      test('unsupported feature', () => {
        expect(translate('upper(DISTINCT 1)')[1]).toStrictEqual([
          {
            message: 'DISTINCT, ORDER BY, FILTER and OVER clauses are not supported',
            source: 'upper'
          }
        ]);
      });

      test('unknown function', () => {
        expect(translate('unknown_function()')[1]).toStrictEqual([
          {
            message: 'Unknown function',
            source: 'unknown_function'
          }
        ]);
      });

      test('explicitly forbidden function', () => {
        expect(translate('random()')[1]).toStrictEqual([
          {
            message: 'Forbidden call: Sync definitions must be deterministic.',
            source: 'random'
          }
        ]);
      });
    });

    test('binary', () => {
      expect(translate("'foo' @> 'bar'")[1]).toStrictEqual([
        {
          message: 'Unsupported binary operator',
          source: "'foo' @> 'bar'"
        }
      ]);
    });

    test('subquery', () => {
      expect(translate("'foo' = (SELECT bar FROM baz)")[1]).toStrictEqual([
        {
          message: 'Invalid position for subqueries. Subqueries are only supported in WHERE clauses.',
          source: 'SELECT bar FROM baz'
        }
      ]);
    });

    test('cast', () => {
      expect(translate("CAST('foo' AS invalidtype)")[1]).toStrictEqual([
        {
          message: 'Invalid SQLite cast',
          source: 'invalidtype'
        }
      ]);
    });

    test('unknown expression', () => {
      expect(translate('ARRAY[1,2,3]')[1]).toStrictEqual([
        {
          message: 'This expression is not supported by PowerSync',
          source: 'ARRAY[1,2,3]'
        }
      ]);
    });
  });
});

interface TranslationError {
  message: string;
  source: string;
}

function expectNoErrors(source: string, expected: string) {
  expect(translateNoErrors(source)).toStrictEqual(expected);
}

function translateNoErrors(source: string): string {
  const [translated, errors] = translate(source);
  expect(errors).toStrictEqual([]);
  return translated;
}

function translate(source: string): [string, TranslationError[]] {
  const expr = parse(source, { entry: 'expr', locationTracking: true })[0] as Expr;
  const errors: TranslationError[] = [];

  const translator = new PostgresToSqlite({
    originalText: source,
    scope: new SqlScope({}),
    errors: {
      report: (message, location) => {
        const resolved = getLocation(location);

        errors.push({ message, source: source.substring(resolved?.start ?? 0, resolved?.end) });
      }
    },
    locations: new NodeLocations(),
    resolveTableName() {
      throw new Error('unsupported in tests');
    },
    generateTableAlias() {
      throw new Error('unsupported in tests');
    },
    joinSubqueryExpression(expr) {
      return null;
    }
  });

  const expression = translator.translateExpression(expr);
  const toSql = ExpressionToSqlite.toSqlite(expression.node);

  return [toSql, errors];
}
