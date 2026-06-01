import { describe, expect } from 'vitest';
import { SqliteJsonValue, SqliteValue } from '../../../../src/types.js';
import { requestParameters, TestSourceTable } from '../../util.js';
import { syncTest } from './utils.js';

describe('operators match SQLite', () => {
  syncTest('division by zero', ({ sync }) => {
    // Regression test for https://github.com/powersync-ja/powersync-service/pull/646.
    const streams = sync.prepareSyncStreams(`
config:
  edition: 3
  unstable_sqlite_expression_engine: true

streams:
  a:
    query: 'SELECT id, a / b AS result FROM tbl'
`);

    const table = new TestSourceTable('tbl');

    function evaluate(a: SqliteValue, b: SqliteValue): SqliteJsonValue {
      const [row] = streams.evaluateRow({ sourceTable: table, record: { id: 'ignored', a, b } });
      return row.data['result'];
    }

    expect(evaluate(5n, 0n)).toStrictEqual(null);
    expect(evaluate(5n, 0)).toStrictEqual(null);
    expect(evaluate(5, 0)).toStrictEqual(null);
    expect(evaluate(-5n, '0')).toStrictEqual(null);
    expect(evaluate(0n, 0n)).toStrictEqual(null);
  });

  syncTest('cast', ({ sync }) => {
    // Regression test for https://github.com/powersync-ja/powersync-service/pull/645.
    const streams = sync.prepareSyncStreams(`
config:
  edition: 3
  unstable_sqlite_expression_engine: true

streams:
  as_int:
    query: 'SELECT id, CAST(r AS integer) AS result FROM tbl_1'
  as_numeric:
    query: 'SELECT id, CAST(r AS numeric) AS result FROM tbl_2'
`);

    const tableAsInt = new TestSourceTable('tbl_1');
    const tableAsNumeric = new TestSourceTable('tbl_2');

    function evaluate(sourceTable: TestSourceTable, r: SqliteValue): SqliteJsonValue {
      const [row] = streams.evaluateRow({ sourceTable, record: { id: 'ignored', r } });
      return row.data['result'];
    }

    expect(evaluate(tableAsInt, '-12abc')).toStrictEqual(-12n);
    expect(evaluate(tableAsInt, '+12abc')).toStrictEqual(12n);
    expect(evaluate(tableAsInt, '-12.9abc')).toStrictEqual(-12n);

    expect(evaluate(tableAsNumeric, '-12.5abc')).toStrictEqual(-12.5);
    expect(evaluate(tableAsNumeric, '-.5abc')).toStrictEqual(-0.5);
    expect(evaluate(tableAsNumeric, '+1.2e2abc')).toStrictEqual(120n);
    expect(evaluate(tableAsNumeric, '-1.2e2abc')).toStrictEqual(-120n);
  });

  syncTest('json_each', ({ sync }) => {
    // Regression test for https://github.com/powersync-ja/powersync-service/pull/647.
    const streams = sync.prepareSyncStreams(`
config:
  edition: 3
  unstable_sqlite_expression_engine: true

streams:
  stream:
    auto_subscribe: true
    query: |
        SELECT tbl.* FROM tbl, json_each(auth.parameter('r')) AS json
            WHERE tbl.key = json.key AND tbl.value = json.value
`);

    function evaluate(value: any) {
      const { querier, errors } = streams.getBucketParameterQuerier({
        globalParameters: requestParameters({ sub: 'user', r: value }),
        hasDefaultStreams: true,
        streams: {}
      });
      expect(errors).toHaveLength(0);
      return querier.staticBuckets;
    }

    expect(evaluate([JSON.stringify({ a: 1, b: true, c: null })])).toStrictEqual([
      ['a', 1n],
      ['b', 1n],
      ['c', null]
    ]);
    expect(evaluate(['42'])).toStrictEqual([[null, 42n]]);
  });

  describe('SQLite NULL semantics for NOT in sync stream filters', () => {
    // Regression tests for https://github.com/powersync-ja/powersync-service/pull/643.
    const DOCS = new TestSourceTable('docs');

    syncTest('NOT over a NULL scalar expression should not admit the row', ({ sync }) => {
      const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  unstable_sqlite_expression_engine: true

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE NOT nullable_flag
`);

      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', nullable_flag: null } })).toHaveLength(0);
      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'false-row', nullable_flag: 0n } })).toHaveLength(1);
      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'true-row', nullable_flag: 1n } })).toHaveLength(0);
    });

    syncTest('NOT IN over a NULL left operand should not admit the row', ({ sync }) => {
      const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  unstable_sqlite_expression_engine: true

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE state NOT IN '["deleted", "archived"]'
`);

      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', state: null } })).toHaveLength(0);
      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'public-row', state: 'public' } })).toHaveLength(1);
      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'deleted-row', state: 'deleted' } })).toHaveLength(0);
    });

    syncTest('NOT over OR with a NULL operand should not admit the row', ({ sync }) => {
      const desc = sync.prepareSyncStreams(`
config:
  edition: 3
  unstable_sqlite_expression_engine: true

streams:
  stream:
    auto_subscribe: true
    query: SELECT * FROM docs WHERE NOT (nullable_flag OR 0)
`);

      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'null-row', nullable_flag: null } })).toHaveLength(0);
      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'false-row', nullable_flag: 0n } })).toHaveLength(1);
      expect(desc.evaluateRow({ sourceTable: DOCS, record: { id: 'true-row', nullable_flag: 1n } })).toHaveLength(0);
    });
  });
});
