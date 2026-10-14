import { JSONBig } from '@powersync/service-jsonbig';
import {
  PrecompiledSyncConfig,
  SqlSyncRules,
  versionedHydrationState,
  type SqliteValue
} from '@powersync/service-sync-rules';
import { createRequire } from 'node:module';
import { setImmediate } from 'node:timers/promises';
import { describe, expect, test } from 'vitest';
import { RustSourceEvaluator, rustSqliteVersion } from '../src/index.js';
import { compile, prepare, table } from './helpers.js';

const values: SqliteValue[] = [
  null,
  0n,
  1n,
  -1n,
  9007199254740993n,
  9223372036854775807n,
  -9223372036854775808n,
  0,
  -0,
  1,
  1.25,
  -1.5,
  1e-8,
  1e21,
  '',
  'abc',
  '1',
  '-12abc',
  'a😀bc',
  'Straße',
  new Uint8Array([65, 66])
];

describe('SQLite expression parity', () => {
  test.each([
    'a',
    'typeof(a)',
    'a + 1',
    'a - 2',
    'a * 3',
    'a / 2',
    'a / 0',
    'a % 2',
    'a || a',
    'CAST(a AS text)',
    'CAST(a AS integer)',
    'CAST(a AS real)',
    'CAST(a AS numeric)',
    'CAST(a AS blob)',
    'a IS NULL',
    'a IS NOT NULL',
    'NOT a',
    'a > 1',
    'a = 1',
    'a != 1',
    'a >= 1',
    'a <= 1',
    'a BETWEEN 0 AND 10',
    "NOT (a NOT IN '[0,1,2]')",
    "a NOT IN '[0,1,2]'",
    'CASE WHEN a IS NULL THEN 1 ELSE 2 END',
    'ifnull(a, 7)',
    'length(a)',
    'substring(a, 1, 2)',
    'upper(a)',
    'lower(a)',
    'hex(a)'
  ])('%s', (expression) => {
    const { rust, expected } = prepare([`SELECT id, ${expression} AS result FROM docs`]);
    const rows = values.map((a, i) => ({ id: String(i), a }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test.each(["json_extract(a, '$.x')", "a ->> '$.x'", "a -> '$.x'", 'json_array_length(a)'])('%s', (expression) => {
    const { rust, expected } = prepare([`SELECT id, ${expression} AS result FROM docs`]);
    const rows = ['null', '[]', '[1,true,null]', '{"x":1}', '{"x":9007199254740993}', '{"x":{"a":true}}'].map((a) => ({
      id: a,
      a
    }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });

  test('seeded numeric combinations and boolean filters', () => {
    const { rust, expected } = prepare([
      'SELECT id, a + b AS sum, a / b AS quotient, a = b AS equal FROM docs WHERE NOT (a < b AND b IS NULL)',
      'SELECT * FROM docs WHERE a > b OR a IS NULL'
    ]);
    let state = 42;
    const random = () => {
      state = (Math.imul(state, 1664525) + 1013904223) >>> 0;
      return state;
    };
    const rows = Array.from({ length: 2000 }, (_, i) => ({
      id: String(i),
      a: values[random() % values.length],
      b: values[random() % values.length]
    }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });
});

describe('source-row processing', () => {
  test('preserves hydrated source identities on data and parameter results', () => {
    const queries = ['SELECT docs.* FROM docs WHERE owner IN (SELECT id FROM users WHERE team = auth.user_id())'];
    for (const sourceTable of [table, { ...table, name: 'users' }]) {
      const { rust, expected } = prepare(queries, sourceTable);
      const rows = [{ id: 'x', owner: 'u', team: 't' }];
      const actual = rust.evaluate(rows)[0];
      const reference = expected(rows)[0];
      actual.data.results.forEach((result, i) => expect(result.source).toBe(reference.data.results[i].source));
      actual.parameters.results.forEach((result, i) =>
        expect(result.lookup.source).toBe(reference.parameters.results[i].lookup.source)
      );
    }
  });
  test('compiled plans may omit an output table override', () => {
    const config = compile(['SELECT id FROM docs']);
    if (!(config instanceof PrecompiledSyncConfig)) throw new Error('Expected compiled plan');
    delete config.plan.buckets[0].sources[0].outputTableName;
    expect(new RustSourceEvaluator(config, table).evaluate([{ id: 'x' }])[0].data.results[0].table).toBe('docs');
  });
  test('result field ordering and numeric types preserve downstream JSON encoding', () => {
    const { rust, expected } = prepare(['SELECT *, a AS id, 1.5 AS ratio FROM docs']);
    const rows = [{ id: 'old', a: 9007199254740993n, '2': 2n, '1': 1, text: 'a\\n"😀', blob: new Uint8Array([1]) }];
    expect(JSONBig.stringify(rust.evaluate(rows))).toEqual(JSONBig.stringify(expected(rows)));
  });
  test('serialized payloads preserve numeric aliases, escaping and integer/real spelling', async () => {
    const { rust, expected } = prepare([
      'SELECT id, a AS "10", b AS "2", c AS "01", d AS "4294967295", e AS "0", a AS "quote\"\"key" FROM docs'
    ]);
    const rows = [{ id: 'x', a: 1.0, b: 1n, c: -0, d: 1e21, e: '"\\\n\u0000😀' }];
    const reference = expected(rows);
    expect(rust.evaluate(rows)).toEqual(reference);
    expect(await rust.evaluateAsync(rows)).toEqual(reference);
    expect(typeof reference[0].data.results[0].data).toBe('string');
    expect(reference[0].data.results[0].data).toContain('"2":1,"10":1.0');
  });
  test('stars, aliases, binary omission, null IDs and property order', () => {
    const { rust, expected } = prepare(['SELECT *, a AS id, 42 AS added FROM docs', 'SELECT a, docs.* FROM docs']);
    const rows = values.map((a) => ({ id: 'old', a, bytes: new Uint8Array([1]), '2': 2n, '1': 1n }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
    expect(rust.evaluate([])).toEqual([]);
  });
  test('bucket partition identity, validity and hydration scopes', () => {
    const { rust, expected } = prepare(
      ["SELECT * FROM docs WHERE a = subscription.parameter('a')"],
      table,
      versionedHydrationState(42)
    );
    const rows = values.map((a) => ({ id: 'x', a }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });
  test('source table mismatch', () => {
    const { rust } = prepare(['SELECT * FROM docs'], { ...table, name: 'unrelated' });
    expect(rust.evaluate([{ id: 'x' }])).toEqual([
      { data: { results: [], errors: [] }, parameters: { results: [], errors: [] } }
    ]);
  });
  test('wildcard table metadata', () => {
    const { rust, expected } = prepare(
      ['SELECT id, d.schema() AS schema, d.table_name() AS name, d.table_suffix() AS suffix FROM "doc%" d'],
      { ...table, name: 'docs_2026' }
    );
    expect(rust.evaluate([{ id: 'x' }])).toEqual(expected([{ id: 'x' }]));
  });
  test('table-valued expansion preserves multiplicity', () => {
    const { rust, expected } = prepare([
      "SELECT docs.* FROM docs, json_each(docs.tags) AS tag WHERE tag.value = subscription.parameter('tag')"
    ]);
    const rows = ['[]', '[1,1,2]', '[null,true,"a"]', '{"a":1,"b":2}'].map((tags) => ({ id: tags, tags }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });
  test('parameter-index construction and grouping', () => {
    const queries = ['SELECT docs.* FROM docs WHERE owner IN (SELECT id FROM users WHERE team = auth.user_id())'];
    const { rust, expected } = prepare(queries, { ...table, name: 'users' });
    const rows = values.map((team, i) => ({ id: BigInt(i), team }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });
  test('parameter table-valued expansion', () => {
    const { rust, expected } = prepare(
      [
        'SELECT docs.* FROM docs WHERE owner IN (SELECT users.id FROM users, json_each(users.teams) t WHERE t.value = auth.user_id())'
      ],
      { ...table, name: 'users' }
    );
    const rows = ['[1,1,2]', '[null,"a","a"]', '[]'].map((teams) => ({ id: 'u', teams }));
    expect(rust.evaluate(rows)).toEqual(expected(rows));
  });
  test('per-query errors preserve other successful queries and later rows', () => {
    const { rust, expected } = prepare(["SELECT id, json_extract(a, '$.x') AS x FROM docs", 'SELECT id FROM docs']);
    const rows = [
      { id: 'bad', a: '{' },
      { id: 'good', a: '{"x":1}' }
    ];
    const actual = rust.evaluate(rows);
    expect(actual.map((r) => r.data.results)).toEqual(expected(rows).map((r) => r.data.results));
    expect(actual[0].data.errors).toHaveLength(1);
    expect(actual[1].data.errors).toEqual([]);
  });
  test('missing referenced fields report an error without poisoning later calls', () => {
    const { rust } = prepare(['SELECT id, missing FROM docs']);
    expect(rust.evaluate([{ id: 'x' }])[0].data.errors).toHaveLength(1);
    expect(rust.evaluate([{ id: 'x', missing: null }])[0].data.errors).toEqual([]);
  });
  test('rejects unsupported configuration engines and legacy plans', () => {
    expect(() => new RustSourceEvaluator(compile(['SELECT * FROM docs'], false), table)).toThrow(
      'unstable_sqlite_expression_engine'
    );
    const legacy = SqlSyncRules.fromYaml('bucket_definitions:\n  global:\n    data:\n      - SELECT * FROM docs', {
      defaultSchema: 'public'
    }).config;
    expect(() => new RustSourceEvaluator(legacy, table)).toThrow('edition 3');
  });
  test('out-of-range input is rejected before native execution', () => {
    const { rust } = prepare(['SELECT * FROM docs']);
    expect(() => rust.evaluate([{ id: 1n << 80n }])).toThrow('64-bit');
    expect(rust.evaluate([{ id: 0n }])[0].data.results[0].id).toBe('0');
  });
  test('deduplicates hydrated bucket and parameter scopes', () => {
    const queries = [
      'SELECT id FROM docs WHERE a > 1',
      'SELECT id FROM docs WHERE a > 2',
      'SELECT docs.* FROM docs WHERE owner IN (SELECT id FROM users WHERE team = auth.user_id())',
      'SELECT docs.* FROM docs WHERE owner IN (SELECT id FROM users WHERE region = auth.user_id())'
    ];
    const hydrationState = {
      getBucketSourceScope: (source: any) => ({ bucketPrefix: 'shared', source }),
      getParameterIndexLookupScope: (source: any) => ({ lookupName: 'shared', queryId: '', source })
    };
    for (const sourceTable of [table, { ...table, name: 'users' }]) {
      const { rust, expected } = prepare(queries, sourceTable, hydrationState);
      const rows = [{ id: 'x', a: 3n, owner: 'u', team: 't', region: 'r' }];
      expect(rust.evaluate(rows)).toEqual(expected(rows));
    }
  });
  test('parameter query failure is reported separately from data errors', () => {
    const { rust } = prepare(
      [
        "SELECT docs.* FROM docs WHERE owner IN (SELECT id FROM users WHERE json_extract(team, '$.id') = auth.user_id())"
      ],
      { ...table, name: 'users' }
    );
    const result = rust.evaluate([{ id: 'x', team: '{' }])[0];
    expect(result.parameters.errors).toHaveLength(1);
    expect(result.data.errors).toEqual([]);
  });
  test('reports SQLite version', () => expect(rustSqliteVersion()).toMatch(/^3\.\d+\.\d+$/));
});

describe('native background execution', () => {
  test('same results, concurrent calls, and event-loop progress', async () => {
    const { rust, expected } = prepare(['SELECT id, upper(a) AS upper FROM docs']);
    const rows = Array.from({ length: 2000 }, (_, i) => ({ id: String(i), a: 'Straße😀' }));
    let yielded = false;
    const marker = setImmediate().then(() => {
      yielded = true;
    });
    const batches = await Promise.all([rust.evaluateAsync(rows), rust.evaluateAsync(rows.slice(0, 20))]);
    await marker;
    expect(yielded).toBe(true);
    expect(batches[0]).toEqual(expected(rows));
    expect(batches[1]).toEqual(expected(rows.slice(0, 20)));
    expect(await rust.evaluateAsync([])).toEqual([]);
  });
  test('owns a snapshot of input bytes', async () => {
    const { rust } = prepare(['SELECT id, hex(a) AS bytes FROM docs']);
    const a = new Uint8Array([1, 2, 3]);
    const task = rust.evaluateAsync([{ id: 'x', a }]);
    a.fill(255);
    expect(JSONBig.parse((await task)[0].data.results[0].data)).toMatchObject({ bytes: '010203' });
  });
});

describe('invalid native plans', () => {
  const { NativeEvaluator } = createRequire(import.meta.url)('../dist/evaluator.node');
  test('invalid JSON and SQL are rejected', () => {
    expect(() => new NativeEvaluator('{')).toThrow();
    expect(() => new NativeEvaluator('[{}]')).toThrow();
  });
  test('native-only diagnostic timings exclude crossings', () => {
    const native = new NativeEvaluator('[]');
    expect(native.measureExecution([], 1)).toMatchObject({ resultCount: 0 });
    expect(() => native.measureExecution([], 0)).toThrow('positive');
  });
  test.each([
    { sql: 'DELETE FROM sqlite_master', outputCount: 0, outputs: [] },
    { sql: 'SELECT 1', outputCount: 2, outputs: [] },
    { sql: 'SELECT 1', outputCount: 0, outputs: ['not-star'] },
    { sql: 'SELECT 1', outputCount: 1, outputs: [{ index: 1, alias: 'x' }] },
    { sql: 'SELECT ?1', outputCount: 1, outputs: [] }
  ])('rejects invalid plan layout %j', (overrides) => {
    const plan = {
      kind: 'data',
      source: 0,
      inputs: [],
      parameterCount: 0,
      table: 'docs',
      bucketPrefix: 'test',
      ...overrides
    };
    expect(() => new NativeEvaluator(JSON.stringify([plan]))).toThrow();
  });
  test('native measurement refuses erroneous results', () => {
    const plan = {
      kind: 'data',
      source: 0,
      sql: "SELECT json_extract('{', '$.x')",
      inputs: [],
      outputs: [],
      outputCount: 0,
      parameterCount: 0,
      table: 'docs',
      bucketPrefix: 'test'
    };
    expect(() => new NativeEvaluator(JSON.stringify([plan])).measureExecution([[]], 1)).toThrow('evaluation errors');
  });
});
