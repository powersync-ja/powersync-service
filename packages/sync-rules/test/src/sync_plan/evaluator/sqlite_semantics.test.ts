import { describe, expect } from 'vitest';
import { SqliteJsonValue, SqliteValue } from '../../../../src/types.js';
import { requestParameters, TestSourceTable } from '../../util.js';
import { syncTest } from './utils.js';

describe('operators match SQLite', () => {
  syncTest('upper / lower use ASCII-only semantics (matches SQLite default)', ({ sync }) => {
    // SQLite's default upper()/lower() only handles a-z / A-Z; non-ASCII
    // letters pass through unchanged. JavaScript's toUpperCase/toLowerCase
    // are Unicode-aware and length-changing (ß -> SS, fi -> FI). When the
    // evaluator and the client disagree on the result of upper(), bucket
    // keys silently diverge and rows are routed to the wrong bucket.
    const streams = sync.prepareSyncStreams(`
config:
  edition: 3

streams:
  a:
    query: 'SELECT id, UPPER(name) AS upper, LOWER(name) AS lower FROM tbl'
`);

    const table = new TestSourceTable('tbl');

    function evaluate(name: SqliteValue) {
      const [row] = streams.evaluateRow({ sourceTable: table, record: { id: 'ignored', name } });
      return { upper: row.data['upper'], lower: row.data['lower'] };
    }

    // ASCII works exactly as before.
    expect(evaluate('hello')).toStrictEqual({ upper: 'HELLO', lower: 'hello' });
    expect(evaluate('Hello World')).toStrictEqual({ upper: 'HELLO WORLD', lower: 'hello world' });

    // Non-ASCII letters now pass through unchanged (matching SQLite),
    // instead of being length-changed by JS Unicode folding.
    expect(evaluate('straße')).toStrictEqual({ upper: 'STRAßE', lower: 'straße' });
    expect(evaluate('ﬁle')).toStrictEqual({ upper: 'ﬁLE', lower: 'ﬁle' });

    // Length is preserved (was previously length-changed by JS folds).
    expect(evaluate('straße').upper).toHaveLength('straße'.length);
    expect(evaluate('ﬁle').upper).toHaveLength('ﬁle'.length);
  });

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
      return querier.staticBuckets.map((b) => b.bucket);
    }

    expect(evaluate(JSON.stringify({ a: 1, b: true, c: null }))).toStrictEqual([`stream|0[1,"a"]`, `stream|0[1,"b"]`]);
    expect(evaluate([42])).toStrictEqual([`stream|0[42,0]`]);
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
