import { JSONBig } from '@powersync/service-jsonbig';
import { DEFAULT_HYDRATION_STATE, nodeSqlite, SqlSyncRules, withBucketSource } from '@powersync/service-sync-rules';
import assert from 'node:assert/strict';
import { writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import os from 'node:os';
import { performance } from 'node:perf_hooks';
import * as sqlite from 'node:sqlite';
import { RustSourceEvaluator, rustSqliteVersion } from '../dist/index.js';
import { compileSourcePlan } from '../dist/plan.js';
const { NativeEvaluator } = createRequire(import.meta.url)('../dist/evaluator.node');

const rowCount = Number(process.env.BENCHMARK_ROWS ?? 10000);
const iterations = Number(process.env.BENCHMARK_ITERATIONS ?? 7);
const batchSize = Number(process.env.BENCHMARK_BATCH_SIZE ?? 1000);
for (const [name, value] of Object.entries({ rowCount, iterations, batchSize })) {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${name} must be a positive integer`);
}
const table = { connectionTag: 'default', schema: 'public', name: 'docs' };
const baseRows = Array.from({ length: rowCount }, (_, i) => ({
  id: String(i),
  owner: `user${i % 100}`,
  a: (i % 100) + 0.25,
  b: 2.5,
  title: `Straße item ${i} 😀`,
  tags: '["one","two","three","four"]',
  json: `{"x":${i},"name":"example","flags":[true,false]}`,
  date: '2026-09-14T12:34:56.123Z'
}));

const scenarios = [
  { name: 'passthrough', queries: ['SELECT * FROM docs'] },
  { name: 'projection-filter', queries: ['SELECT id, a + b AS sum, a / b AS ratio FROM docs WHERE a > 25'] },
  {
    name: 'json',
    queries: ["SELECT id, json_extract(json, '$.x') AS x, json_extract(json, '$.flags') AS flags FROM docs"]
  },
  {
    name: 'native-functions',
    queries: ["SELECT id, upper(title) AS title, unixepoch(date, 'subsec') AS epoch, datetime(date) AS date FROM docs"]
  },
  {
    name: 'bucket-fanout',
    queries: ["SELECT docs.* FROM docs, json_each(docs.tags) t WHERE t.value = subscription.parameter('tag')"]
  },
  {
    name: 'many-queries',
    queries: Array.from({ length: 8 }, (_, i) => `SELECT id, a + ${i}.5 AS value FROM docs WHERE a > ${i * 10}`)
  },
  {
    name: 'parameter-index',
    queries: ['SELECT docs.* FROM docs WHERE owner IN (SELECT id FROM users WHERE owner = auth.user_id())'],
    table: { ...table, name: 'users' }
  },
  {
    name: 'parameter-expansion',
    queries: [
      "SELECT users.* FROM users INNER JOIN docs INNER JOIN json_each(docs.tags) tags WHERE users.id = tags.value AND docs.id = subscription.parameter('doc')"
    ]
  }
];

function compile(queries, useSqlite) {
  return SqlSyncRules.fromYaml(
    `config:\n  edition: 3\n  unstable_sqlite_expression_engine: ${useSqlite}\nstreams:\n` +
      queries
        .map(
          (q, i) => `  stream${i}:\n    accept_potentially_dangerous_queries: true\n    query: ${JSON.stringify(q)}\n`
        )
        .join(''),
    { defaultSchema: 'public', throwOnError: true }
  ).config;
}
function evaluateJs(config, sourceTable) {
  const hydrated = config.hydrate({ hydrationState: DEFAULT_HYDRATION_STATE, sqlite: nodeSqlite(sqlite) });
  return (rows) =>
    rows.map((record) => ({
      data: (() => {
        const { results, errors } = hydrated.evaluateRowWithErrors({ sourceTable, record });
        return {
          results: results.map((row) => withBucketSource({ ...row, data: JSONBig.stringify(row.data) }, row.source)),
          errors
        };
      })(),
      parameters: hydrated.evaluateParameterRowWithErrors(sourceTable, record)
    }));
}

let sink = 0;
function consume(result) {
  for (const row of result) {
    if (row.data.errors.length || row.parameters.errors.length)
      throw new Error('Benchmark generated evaluation errors');
    sink += row.data.results.length + row.parameters.results.length;
  }
}
const batches = [];
for (let i = 0; i < baseRows.length; i += batchSize) batches.push(baseRows.slice(i, i + batchSize));
const results = [];
const nativeExecution = [];

for (const scenario of scenarios) {
  const sourceTable = scenario.table ?? table;
  const sqliteConfig = compile(scenario.queries, true);
  const native = new RustSourceEvaluator(sqliteConfig, sourceTable);
  const implementations = [
    ['js-plain', evaluateJs(compile(scenario.queries, false), sourceTable)],
    ['js-sqlite', evaluateJs(sqliteConfig, sourceTable)],
    ['rust-sync', (rows) => native.evaluate(rows)],
    ['rust-async', (rows) => native.evaluateAsync(rows)]
  ];
  // Verify outputs on every input before timing. Only use workloads shared by the JS and SQLite engines.
  for (const rows of batches) {
    const expected = implementations[1][1](rows);
    consume(expected);
    for (const [name, run] of implementations)
      assert.deepStrictEqual(await run(rows), expected, `${scenario.name}: ${name}`);
  }
  for (let warmup = 0; warmup < 2; warmup++) {
    for (const [, run] of implementations) for (const batch of batches) consume(await run(batch));
  }
  const samples = new Map(implementations.map(([name]) => [name, []]));
  // Rotate implementation order to reduce systematic warmup/thermal ordering bias.
  for (let iteration = 0; iteration < iterations; iteration++) {
    for (let offset = 0; offset < implementations.length; offset++) {
      const [name, run] = implementations[(iteration + offset) % implementations.length];
      const start = performance.now();
      for (const batch of batches) consume(await run(batch));
      samples.get(name).push(performance.now() - start);
    }
  }
  for (const [name, times] of samples) {
    const sorted = [...times].sort((a, b) => a - b);
    const middle = Math.floor(sorted.length / 2);
    const medianMs = sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
    const result = {
      scenario: scenario.name,
      implementation: name,
      medianMs,
      rowsPerSecond: Math.round((rowCount * 1000) / medianMs),
      samplesMs: times
    };
    results.push(result);
    console.log(
      `${scenario.name.padEnd(21)} ${name.padEnd(11)} ${String(result.rowsPerSecond).padStart(9)} rows/s (${medianMs.toFixed(2)} ms)`
    );
  }
  const core = new NativeEvaluator(JSON.stringify(compileSourcePlan(sqliteConfig, sourceTable).processors));
  const fields = baseRows.map((row) => Object.entries(row).map(([name, value]) => ({ name, value })));
  core.measureExecution(fields, 2);
  const measured = core.measureExecution(fields, iterations);
  nativeExecution.push({
    scenario: scenario.name,
    ...measured,
    rowsPerSecond: Math.round((rowCount * iterations * 1000) / measured.executionMs)
  });
}
const report = {
  generatedAt: new Date().toISOString(),
  node: process.version,
  nodeSqlite: process.versions.sqlite,
  rustSqlite: rustSqliteVersion(),
  platform: process.platform,
  arch: process.arch,
  cpu: os.cpus()[0]?.model,
  cpuCount: os.availableParallelism(),
  rowCount,
  iterations,
  batchSize,
  scope:
    'Source row evaluation including Node/native boundary, projections, bucket names, parameter indexes and data payload JSON; excludes config compilation, source conversion, checksums and storage.',
  results,
  nativeExecution,
  sink
};
writeFileSync('benchmark-results.json', JSON.stringify(report, null, 2) + '\n');
