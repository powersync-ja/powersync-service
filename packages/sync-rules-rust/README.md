# Rust source-row evaluator

Experimental, private workspace package for evaluating compiled Sync Streams source rows with Rust and SQLite. It is not connected to production replication.

The TypeScript compiler generates SQL and execution metadata once per source table. Rust owns the in-memory SQLite connection, cached statements, input binding, filters, table-valued expansion, projections, row IDs, data-payload JSON serialization, bucket-name serialization and parameter-index grouping. All nine PowerSync SQLite overrides are implemented in Rust: `upper`, `lower`, `unixepoch`, `datetime`, `st_asgeojson`, `st_astext`, `st_x`, `st_y`, and `ps_json_contains`. There are no JavaScript callbacks from native execution.

This package accepts already-converted `SqliteRow` values. It does not implement BSON/source conversion, query-time evaluation, sync events, storage writes or checksums. Parsing, SQL generation and hydration identity assignment remain in the existing TypeScript package. Data results return `SerializedEvaluatedRow` with a JSON string in `data`; parameter results retain the existing TypeScript representation and number normalization. Storage accepts these payload strings directly, without parsing or reserializing them.

## Usage

Requires Node from the repository's `.nvmrc`, a C compiler and Rust. `rust-toolchain.toml` selects Rust 1.91.0; `native/Cargo.lock` pins native dependencies. Linux builds were verified locally. The build script also supports macOS and Windows, but these platforms have not been verified.

```sh
pnpm --filter @powersync/service-sync-rules-rust build
```

```ts
import { SqlSyncRules } from '@powersync/service-sync-rules';
import { RustSourceEvaluator } from '@powersync/service-sync-rules-rust';

const { config } = SqlSyncRules.fromYaml(
  `config:
  edition: 3
  unstable_sqlite_expression_engine: true
streams:
  notes:
    query: SELECT id, upper(title) AS title FROM notes
`,
  { defaultSchema: 'public', throwOnError: true }
);

const evaluator = new RustSourceEvaluator(config, {
  connectionTag: 'default',
  schema: 'public',
  name: 'notes'
});
const results = await evaluator.evaluateAsync([{ id: '1', title: 'Hello' }]);
// results[0].data.results[0].data is a serialized JSON string.
// Parameter results remain available in results[0].parameters.results.
// Each also has an errors array; one failed query does not discard other queries.
```

`evaluate(rows)` runs synchronously and can be used inside an existing JS worker. `evaluateAsync(rows)` submits the batch to libuv's native thread pool. Both use the same Rust engine. Each evaluator serializes access to its connection; different evaluators can execute independently. Input strings and binary values are owned by the task before dispatch, so subsequent mutation of JS buffers cannot race native reads. Results stay in input-row order. Callers should bound outstanding batches; this package does not implement the replication publication pipeline or its admission limits.

No native build is run during install or the general service build. Core CI explicitly builds the addon before package tests, so the experimental package does not add a Rust dependency to service container builds.

## Compatibility

The reference is the **SQLite** engine of compiled Sync Streams, not the plain JavaScript engine or legacy sync rules. Configuration without edition 3 / `unstable_sqlite_expression_engine` is rejected. One evaluator is tied to one immutable config, source table and optional `HydrationState`; create a new evaluator when these change. Multiple merged configs and event evaluation are not exposed by this initial API.

Known boundaries to audit before production use:

- Native SQLite is pinned by Cargo, while `node:sqlite` is pinned by Node. Both versions are recorded in benchmark reports. Version/build differences can affect SQLite expressions.
- Dates support ISO dates/times, explicit offsets, Julian days and Unix timestamps. V8's permissive non-ISO date parser, normalization of invalid calendar dates, and extreme date ranges are not fully reproduced. For example, `datetime('0000-01-01')` currently returns year 0000 in Rust and year 2000 in the JS reference; this difference has an explicit test. Invalid/unrecognized dates return null. `now` and unsupported modifiers remain disabled.
- Unicode casing uses Rust's Unicode tables; these may differ from the Node/ICU version at uncommon code points.
- Geometry uses geozero for WKB/EWKB parsing. XY points, lines, collections, endian variants and SRIDs have parity tests; extended dimensional and empty-geometry formatting need further compatibility work.
- JSON containment uses serde JSON, including exact signed-64-bit integers. Extremely large JSON integers, object key order and floating-point lexical forms need further parity work.
- Input integers outside SQLite's signed 64-bit range reject the batch. The JS evaluator can retain larger integers in unbound star-projected fields. Missing bound fields report a source-query error; error text is not guaranteed to match Node.
- Inputs and parameter results cross as JS strings, numbers, BigInts, binary arrays and null; data payloads cross as JSON strings. Payload serialization preserves JSONBig integer/real spelling and JavaScript key enumeration order. No promise of byte-for-byte compatibility is made for unusual JS strings (such as lone UTF-16 surrogates), non-finite numbers, or prototype-derived properties.

Ordinary expression semantics, scalar types, nulls, aliases, wildcard metadata, hydration scopes, bucket partition identity, parameter indexes and custom functions are checked differentially against `node:sqlite`. The plain JS engine is also checked on the common workloads used by the benchmark. Compatibility differences must not be confused with successful optimization.

## Validation

Run commands from this package:

```sh
pnpm build
pnpm build:tests
pnpm test
pnpm test:rust
pnpm test:coverage
pnpm test:coverage:native
pnpm exec cargo clippy --manifest-path native/Cargo.toml --all-targets -- -D warnings
```

The differential suite covers generated numeric/type combinations, JSON operations, all native custom functions, data and parameter processing, table-valued fanout, invalid input, query-error isolation, repeated use, concurrent async calls and copied binary input. V8 coverage measures the TypeScript wrapper only. Native coverage separately instruments the addon and combines those integration tests with Rust unit tests; reports are written under `coverage/native-*`.

Native coverage requires LLVM tools matching rustc's LLVM major version. Install Rust's `llvm-tools` component or provide matching `llvm-cov-<major>` and `llvm-profdata-<major>` executables. `LLVM_COV` and `LLVM_PROFDATA` can override discovery. The script restores the release addon even if integration tests fail. Do not run builds or benchmarks concurrently with native coverage, which temporarily replaces that addon.

## Benchmarks

```sh
pnpm build
BENCHMARK_ROWS=10000 BENCHMARK_ITERATIONS=7 BENCHMARK_BATCH_SIZE=1000 pnpm benchmark
```

The benchmark compares plain JS, JS + SQLite, Rust synchronous and Rust asynchronous evaluation. Workloads cover passthrough, projection/filtering, JSON, custom functions, bucket fanout, multiple queries, parameter indexes and expanded parameter outputs. Each implementation evaluates the same inputs and returns the same result shape. Every input is checked for result equality before timing. Two warmup passes precede rotated execution order across measured iterations.

Reported throughput includes input/output conversion across the native boundary, result reconstruction, data-payload serialization and result consumption. Both JavaScript implementations serialize payloads with JSONBig; Rust serializes during native evaluation (on the background thread for evaluateAsync). Config compilation, source conversion, checksums and storage are excluded. Native-only execution measurements are reported separately as a diagnostic, **not** as a comparable end-to-end speedup. `benchmark-results.json` records all timing samples, versions, machine details and settings. See [BENCHMARKS-SERIALIZED.md](BENCHMARKS-SERIALIZED.md) for the current run and [BENCHMARKS.md](BENCHMARKS.md) for the historical object-output baseline.
