# Rust source-row evaluator

Experimental, private workspace package for evaluating compiled Sync Streams source rows with Rust and SQLite. It is not connected to production replication.

The TypeScript compiler generates SQL and execution metadata once per source table. Rust owns the in-memory SQLite connection, cached statements, input binding, filters, table-valued expansion, projections, row IDs, data-payload JSON serialization, bucket-name serialization and parameter-index grouping. All nine PowerSync SQLite overrides are implemented in Rust: `upper`, `lower`, `unixepoch`, `datetime`, `st_asgeojson`, `st_astext`, `st_x`, `st_y`, and `ps_json_contains`. There are no JavaScript callbacks from native execution.

This package accepts already-converted `SqliteRow` values or raw MongoDB BSON documents. It does not implement query-time evaluation, sync events or storage writes. Its MongoDB preparation API also computes replica identities, subkeys and checksums. Parsing, SQL generation and hydration identity assignment remain in the existing TypeScript package. Data results return `SerializedEvaluatedRow` with a JSON string in `data`; parameter results retain the existing TypeScript representation and number normalization. Storage accepts these payload strings directly, without parsing or reserializing them.

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

### Combined MongoDB BSON path

```ts
// Raw BSON buffers from the MongoDB source driver, without a JS document decode.
const results = await evaluator.evaluateBsonAsync(documents);
// results[i].data.results[j].data is ready-to-store JSON.
```

`evaluateBsonAsync(Uint8Array[])` copies the input buffers into Rust-owned memory before dispatch. BSON parsing, conversion to SQLite values, data/parameter evaluation and data-payload serialization then run in one native background task. Conversion processes one document at a time; no JS source row or per-field N-API input objects are created. `evaluateBson` exposes the same path synchronously for comparisons. MongoDB date rendering is derived from the evaluator's config, including legacy timestamps and second precision.

The converter adapts `packages/mongo-after-record-rs/src/converter.rs` from **`rust-sync-plans` at `5773fd47b`**. It uses the `bson` crate's raw-document API for parsing and `serde_json` for string escaping, replacing the earlier Rust code's custom string escaper. It updates UUID, regex, timestamp, code/scope, symbol and date conversion to the current JS converter's behavior. Ordinary types are tested against `modules/module-mongodb/src/replication/bufferToSqlite.ts` through complete evaluation and exact payload comparisons.

Malformed BSON or conversion errors reject the whole batch; callers receive no partial results. SQL evaluation errors remain per-row/per-query results. Each call owns its buffers, so concurrent caller mutation cannot race background parsing. Concurrent calls on one evaluator serialize on its connection; separate evaluators are needed for parallel evaluation. Callers still need bounded admission and ordered publication.

For preparation with replica identity and checksums:

```ts
const prepared = await evaluator.prepareBsonAsync(documents, sourceTable.id.toHexString());
// Each row adds replicaIdBson, subkey and deleteChecksum.
// Every data result adds checksum alongside its serialized data.
```

`prepareBsonAsync` runs conversion, evaluation, serialization, replica identity extraction, subkey generation and checksums inside the same native task. The table ID must be the MongoDB storage source table's ObjectId as 24 hex characters. The original `_id` BSON bytes are preserved in `replicaIdBson`. Subkeys reproduce the existing UUID-v5 hash of BSON `{ table, id }`, including JS numeric/object-key normalization, and the historical UUID-ID special case. SHA-256 data/delete checksums use the first four digest bytes as an unsigned little-endian integer. Hashing uses `sha2` and `uuid` crates. The main thread copies input and reconstructs returned JS objects; no JS hashing or replica-ID parsing is needed.

Missing `_id`, invalid table IDs or unsupported replica IDs reject preparation of the whole batch. Undefined, regex, JavaScript code/scope and DBPointer replica IDs, and embedded `__proto__` keys, are explicitly rejected, including when nested in an ID. Unusual duplicate-key and deprecated-type normalization still need auditing. This API currently targets ObjectId table identifiers used by MongoDB storage, not string table identifiers from other storage implementations.

This is a standalone preparation API. Production replication is not switched to it. Event processing, current-row retention and storage coordination remain outside the API. The basic BSON benchmarks exclude identity/checksum operations equally for all implementations; the worker comparison also measures the broader preparation scope. Documents are fully converted even when only a few columns are referenced; selective conversion is a possible later optimization.

BSON compatibility gaps retained for this experiment:

- Deprecated DBPointer values reject the batch. Invalid UTF-8 is rejected by the BSON library, while the JS direct converter attempts replacement-character recovery.
- Top-level integral doubles and unsigned timestamps outside signed 64-bit range reject instead of retaining an unbounded JS bigint. Nested timestamps retain their full unsigned value in JSON text.
- Dates beyond chrono's supported range reject. Duplicate BSON field names and prototype-sensitive JS keys have not been made compatible.

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

For the complete raw-BSON comparison, run `pnpm benchmark:bson` after `pnpm build`. It builds the MongoDB module for the JS reference converter and compares JS plain, JS SQLite, JS conversion plus Rust field input, and combined Rust BSON sync/async paths. See [BENCHMARKS-BSON.md](BENCHMARKS-BSON.md) for results and scope.

For bounded parallel native batches, run `pnpm benchmark:bson:parallel`. This sets `UV_THREADPOOL_SIZE=8` before Node starts and tests 1, 2, 4 and 8 independent evaluators, with 40,000 documents by default. Each evaluator owns its SQLite connection and has at most one outstanding task. Results are consumed in input order through a bounded window; the benchmark includes any waiting behind earlier batches. Input-buffer copies and JS result construction remain included. See [BENCHMARKS-BSON-PARALLEL.md](BENCHMARKS-BSON-PARALLEL.md).

For four native threads versus four JS workers, run `pnpm benchmark:bson:workers`. This sets `UV_THREADPOOL_SIZE=4` and includes bounded ordered scheduling, message cloning or native input copying, and result reconstruction. It measures conversion/evaluation alone and preparation with replica identities, subkeys and checksums. Both implementations perform that preparation on background threads. See [BENCHMARKS-WORKERS.md](BENCHMARKS-WORKERS.md).
