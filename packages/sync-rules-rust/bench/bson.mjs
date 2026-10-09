import { BSON, ObjectId } from 'bson';
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import { performance } from 'node:perf_hooks';
import { bufferToSqlite, getDateRenderMode } from '../../../modules/module-mongodb/dist/replication/bufferToSqlite.js';
import { RustSourceEvaluator, rustSqliteVersion } from '../dist/index.js';
import { compile, jsImplementation } from './evaluation.mjs';
import { JsPreparationWorker, completePreparation } from './worker-pool.mjs';

const workers = process.env.BENCHMARK_WORKERS === 'true';
const parallel = process.env.BENCHMARK_PARALLEL === 'true';
const concurrencyLevels = [1, 2, 4, 8];
const rowCount = Number(process.env.BENCHMARK_ROWS ?? (parallel || workers ? 40000 : 10000));
const iterations = Number(process.env.BENCHMARK_ITERATIONS ?? 7);
const batchSize = Number(process.env.BENCHMARK_BATCH_SIZE ?? 1000);
for (const [key, value] of Object.entries({ rowCount, iterations, batchSize })) {
  if (!Number.isSafeInteger(value) || value < 1) throw new Error(`${key} must be a positive integer`);
}
// Generated from the existing replication harness's anonymized shape generator.
// No source record contents are retained; fixture generation is outside timings.
const fixture = JSON.parse(readFileSync(new URL('./fixtures/sample-documents.json', import.meta.url), 'utf8'));
const table = { connectionTag: 'default', schema: 'public', name: 'docs' };
const nested = Array.from({ length: 40 }, (_, i) => ({
  i,
  label: `item ${i} 😀`,
  flags: [true, false],
  value: i + 0.25
}));
const scenarios = [
  { name: 'flat-passthrough', shape: 'flat', queries: ['SELECT *, _id AS id FROM docs'] },
  { name: 'sample-passthrough', shape: 'sample', queries: ['SELECT *, _id AS id FROM docs'] },
  { name: 'sample-projection', shape: 'sample', queries: ['SELECT _id AS id, a + b AS sum FROM docs WHERE a > 25'] },
  {
    name: 'nested-json',
    shape: 'nested',
    queries: ["SELECT _id AS id, json_extract(details, '$.value') AS value FROM docs"]
  },
  {
    name: 'sample-functions',
    shape: 'sample',
    queries: ["SELECT _id AS id, upper(title) AS title, unixepoch(date, 'subsec') AS epoch FROM docs"]
  },
  {
    name: 'sample-fanout',
    shape: 'sample',
    queries: [
      "SELECT docs.*, docs._id AS id FROM docs, json_each(docs.tags) t WHERE t.value = subscription.parameter('tag')"
    ]
  },
  {
    name: 'sample-many-queries',
    shape: 'sample',
    queries: Array.from({ length: 8 }, (_, i) => `SELECT _id AS id, a + ${i}.5 AS value FROM docs WHERE a > ${i * 10}`)
  },
  {
    name: 'sample-parameters',
    shape: 'sample',
    queries: ['SELECT others.* FROM others WHERE owner IN (SELECT _id FROM docs WHERE owner = auth.user_id())']
  }
];

let sink = 0;
function consume(results) {
  for (const row of results) {
    assert.equal(row.data.errors.length + row.parameters.errors.length, 0);
    for (const result of row.data.results) sink += result.data.length;
    sink += row.parameters.results.length;
  }
}
// Each slot owns an evaluator/SQLite connection. Keep at most concurrency batches
// outstanding and consume in source order, including head-of-line waiting in timings.
async function runParallel(batches, evaluators, accept) {
  const pending = evaluators.map((evaluator, i) =>
    i < batches.length ? evaluator.evaluateBsonAsync(batches[i]) : null
  );
  for (let i = 0; i < batches.length; i++) {
    const slot = i % evaluators.length;
    accept(await pending[slot], i);
    const next = i + evaluators.length;
    pending[slot] = next < batches.length ? evaluators[slot].evaluateBsonAsync(batches[next]) : null;
  }
}
function recordResult(scenario, implementation, inputBytes, samplesMs, extra = {}) {
  const sorted = [...samplesMs].sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  const medianMs = sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
  const result = {
    scenario,
    implementation,
    inputBytes,
    medianMs,
    rowsPerSecond: Math.round((rowCount * 1000) / medianMs),
    inputMiBPerSecond: (inputBytes * 1000) / medianMs / 2 ** 20,
    inputMBPerSecond: (inputBytes * 1000) / medianMs / 1e6,
    samplesMs,
    ...extra
  };
  results.push(result);
  console.log(
    `${(extra.scope ?? '').padEnd(workers ? 22 : 0)} ${scenario.padEnd(22)} ${implementation.padEnd(18)} ${String(result.rowsPerSecond).padStart(9)} rows/s  ${result.inputMBPerSecond.toFixed(1)} MB/s`
  );
}
const results = [];
for (const scenario of scenarios) {
  const documents = Array.from({ length: rowCount }, (_, i) =>
    BSON.serialize({
      ...(scenario.shape === 'sample' ? fixture[i % fixture.length] : {}),
      _id: new ObjectId(i.toString(16).padStart(24, '0')),
      owner: `user${i % 100}`,
      a: (i % 100) + 0.25,
      b: 2.5,
      title: `Straße ${i} 😀`,
      tags: ['one', 'two', 'three', 'four'],
      date: new Date('2026-09-14T12:34:56.123Z'),
      ...(scenario.shape === 'nested' ? { details: { value: 0.25, items: nested } } : {})
    })
  );
  const batches = [];
  for (let i = 0; i < documents.length; i += batchSize) batches.push(documents.slice(i, i + batchSize));
  const inputBytes = documents.reduce((sum, document) => sum + document.length, 0);
  const config = compile(scenario.queries, true);
  if (workers) {
    const variants = [];
    try {
      for (const full of [false, true]) {
        for (const kind of ['js-plain-workers', 'js-sqlite-workers', 'rust']) {
          const setupStart = performance.now();
          const evaluators =
            kind === 'rust'
              ? Array.from({ length: 4 }, () => {
                  const native = new RustSourceEvaluator(config, table);
                  return {
                    async evaluateBsonAsync(buffers) {
                      return full
                        ? native.prepareBsonAsync(buffers, '66e834cc91d805df11fa0ecb')
                        : native.evaluateBsonAsync(buffers);
                    }
                  };
                })
              : Array.from(
                  { length: 4 },
                  () => new JsPreparationWorker(scenario.queries, kind === 'js-sqlite-workers', config, table, full)
                );
          const variant = {
            implementation: `${kind}-4`,
            scope: full ? 'full-preparation' : 'conversion-evaluation',
            evaluators,
            samplesMs: [],
            setupMs: 0
          };
          variants.push(variant);
          await Promise.all(evaluators.map((e) => e.ready));
          variant.setupMs = performance.now() - setupStart;
          const coldStart = performance.now();
          await runParallel(batches.slice(0, 4), evaluators, consume);
          variant.firstWaveMs = performance.now() - coldStart;
        }
      }
      const reference = jsImplementation(config, table);
      for (const variant of variants) {
        await runParallel(batches, variant.evaluators, (rows, i) => {
          const expected = reference(batches[i]);
          assert.deepStrictEqual(
            rows,
            variant.scope === 'full-preparation' ? completePreparation(expected, batches[i]) : expected,
            `${scenario.name}: ${variant.implementation} batch ${i}`
          );
          // Data source references are deliberately non-enumerable, so deep equality
          // alone cannot verify that the transport restored the correct source.
          for (let row = 0; row < rows.length; row++) {
            rows[row].data.results.forEach((value, j) =>
              assert.strictEqual(value.source, expected[row].data.results[j].source)
            );
            rows[row].parameters.results.forEach((value, j) =>
              assert.strictEqual(value.lookup.source, expected[row].parameters.results[j].lookup.source)
            );
          }
        });
        for (let warmup = 0; warmup < 2; warmup++) await runParallel(batches, variant.evaluators, consume);
      }
      for (let iteration = 0; iteration < iterations; iteration++) {
        for (let offset = 0; offset < variants.length; offset++) {
          const variant = variants[(iteration + offset) % variants.length];
          const start = performance.now();
          await runParallel(batches, variant.evaluators, consume);
          variant.samplesMs.push(performance.now() - start);
        }
      }
      for (const v of variants)
        recordResult(scenario.name, v.implementation, inputBytes, v.samplesMs, {
          scope: v.scope,
          concurrency: 4,
          setupMs: v.setupMs,
          firstWaveMs: v.firstWaveMs
        });
    } finally {
      await Promise.all(variants.flatMap((v) => v.evaluators.map((e) => e.close?.())));
    }
    continue;
  }
  if (parallel) {
    const reference = jsImplementation(config, table);
    const variants = concurrencyLevels.map((concurrency) => ({
      concurrency,
      evaluators: Array.from({ length: concurrency }, () => new RustSourceEvaluator(config, table)),
      samplesMs: []
    }));
    for (const { evaluators } of variants) {
      await runParallel(batches, evaluators, (rows, i) =>
        assert.deepStrictEqual(rows, reference(batches[i]), `${scenario.name}: batch ${i}`)
      );
      for (let warmup = 0; warmup < 2; warmup++) await runParallel(batches, evaluators, consume);
    }
    for (let iteration = 0; iteration < iterations; iteration++) {
      for (let offset = 0; offset < variants.length; offset++) {
        const variant = variants[(iteration + offset) % variants.length];
        const start = performance.now();
        await runParallel(batches, variant.evaluators, consume);
        variant.samplesMs.push(performance.now() - start);
      }
    }
    for (const variant of variants)
      recordResult(scenario.name, `rust-parallel-${variant.concurrency}`, inputBytes, variant.samplesMs, {
        concurrency: variant.concurrency
      });
    continue;
  }
  const native = new RustSourceEvaluator(config, table);
  const mode = getDateRenderMode(config.compatibility);
  const implementations = [
    ['js-plain', jsImplementation(compile(scenario.queries, false), table)],
    ['js-sqlite', jsImplementation(config, table)],
    ['rust-fields-async', (buffers) => native.evaluateAsync(buffers.map((buffer) => bufferToSqlite(buffer, mode)))],
    ['rust-bson-sync', (buffers) => native.evaluateBson(buffers)],
    ['rust-bson-async', (buffers) => native.evaluateBsonAsync(buffers)]
  ];
  for (const batch of batches) {
    const reference = implementations[1][1](batch);
    for (const [name, run] of implementations)
      assert.deepStrictEqual(await run(batch), reference, `${scenario.name}: ${name}`);
  }
  for (let warmup = 0; warmup < 2; warmup++) {
    for (const [, run] of implementations) for (const batch of batches) consume(await run(batch));
  }
  const samples = new Map(implementations.map(([name]) => [name, []]));
  for (let iteration = 0; iteration < iterations; iteration++) {
    for (let offset = 0; offset < implementations.length; offset++) {
      const [name, run] = implementations[(iteration + offset) % implementations.length];
      const start = performance.now();
      for (const batch of batches) consume(await run(batch));
      samples.get(name).push(performance.now() - start);
    }
  }
  for (const [implementation, samplesMs] of samples) recordResult(scenario.name, implementation, inputBytes, samplesMs);
}
writeFileSync(
  workers
    ? 'benchmark-worker-comparison-results.json'
    : parallel
      ? 'benchmark-bson-parallel-results.json'
      : 'benchmark-bson-results.json',
  JSON.stringify(
    {
      generatedAt: new Date().toISOString(),
      node: process.version,
      nodeSqlite: process.versions.sqlite,
      rustSqlite: rustSqliteVersion(),
      cpu: os.cpus()[0]?.model,
      cpuCount: os.availableParallelism(),
      platform: process.platform,
      arch: process.arch,
      rowCount,
      iterations,
      batchSize,
      uvThreadpoolSize: Number(process.env.UV_THREADPOOL_SIZE ?? 4),
      concurrencyLevels: workers ? [4] : parallel ? concurrencyLevels : [1],
      scope: workers
        ? 'Four JS workers (plain and SQLite) versus four native evaluators, bounded ordered coordination. Cloned raw BSON input and serialized-result messaging for JS; buffer copy and N-API for Rust. Two scopes: conversion/evaluation and full preparation including replica IDs, subkeys and checksums, all on background threads for both implementations. Startup/first wave recorded separately; storage and source fetching excluded.'
        : parallel
          ? 'Raw BSON conversion, evaluation and serialization with bounded parallel native tasks on independent evaluators, consumed in source order. Includes buffer copies and output reconstruction; excludes source fetching, replica IDs, checksums, storage and config/fixture generation.'
          : 'Raw BSON conversion, source row evaluation and data JSON serialization, including boundary costs; excludes source fetching, replica identity extraction, checksums, storage, config compilation and BSON fixture generation. One sequentially awaited batch at a time; no JS worker-pool comparison.',
      results,
      sink
    },
    null,
    2
  ) + '\n'
);
