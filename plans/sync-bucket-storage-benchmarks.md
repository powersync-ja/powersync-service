# Sync Bucket Storage Benchmark Suite Plan

## Goal

Add a reusable benchmark suite for comparing PowerSync sync bucket storage implementations. The suite should measure the two storage paths that matter most for sync throughput:

- Persisting a synthetic incoming dataset that touches both parameter lookups and bucket data writes.
- Draining all bucket data through the real sync stream API until `checkpoint_complete`.

The benchmark harness belongs in `packages/service-core-tests` so every storage module can register the same scenarios without duplicating data generation or stream-drain logic.

## Benchmark Model

Use a source-DB-free synthetic dataset written through the storage writer API:

- `lists` rows are parameter rows.
- `todos` rows are data rows.
- One bucket definition groups todos by list.
- One bucket definition groups todos by user.

This makes each todo row appear in two bucket families while list rows drive the dynamic parameter hot path.

Scenario fields are explicit test parameters, not environment variables:

```ts
interface StorageBenchmarkScenario {
  name: string;
  todo_row_count: number;
  list_row_count: number;
  user_count: number;
  flush_every: number;
  max_bucket_count?: number;
  timeout_ms?: number;
}
```

Default scenarios are the cross-product of these dimensions:

| Todo Rows | Timeout |
| --------: | ------: |
|     1,000 |   5 min |
|    10,000 |  10 min |
|   100,000 |  30 min |
| 1,000,000 |     3 h |

| Resolved Buckets | List Rows |  Users | Minimum Timeout |
| ---------------: | --------: | -----: | --------------: |
|              200 |       100 |    100 |               - |
|            1,000 |       500 |    500 |               - |
|           10,000 |     5,000 |  5,000 |          15 min |
|           20,000 |    10,000 | 10,000 |          20 min |

This produces 16 scenarios. Every row-count permutation includes the original 200- and 1,000-bucket shapes plus the
new 10,000- and 20,000-bucket shapes. Each scenario passes its own `max_bucket_count` into the sync context for both
bucket and parameter-result limits. All registered scenarios run both the write phase and sync stream drain phase.

## Harness Requirements

- Export `registerStorageBenchmarks()` from `@powersync/service-core-tests`.
- Export a benchmark summary printer that emits a markdown table after the suite.
- Accept a normal `TestStorageConfig` or `TestStorageFactory`.
- Accept storage name, storage version, scenario list, timeout, progress interval, and an optional shared result array.
- Write rows with `createWriter()` and flush every `flush_every` source rows.
- Commit once after all rows are written so the drain measures a single checkpoint.
- Drain via `sync.streamResponse()` with `raw_data: true` and stop only when `checkpoint_complete` is received.
- Do not benchmark a separate direct `getBucketDataBatch()` drain; the sync stream drain is the public path under test.
- Emit heartbeat progress logs during long sync drains showing lines, ops, bytes, and elapsed milliseconds.
- Emit write progress logs during long writes.

## Storage Module Registration

Each storage module should add a `test/src/storage_bench.test.ts` file and register all supported storage versions:

- `modules/module-postgres-storage`
- `modules/module-mongodb-storage`
- `modules/module-mikroorm-storage` for SQLite and MySQL

Storage setup must include migrations or schema setup before benchmark timing so indexes are present. Postgres and MikroORM use their existing test factory migration/schema paths. MongoDB benchmark factories should explicitly run migrations and use longer client socket timeouts so large drains are not killed by the normal fast-fail test timeout settings.

## Sample Commands

Run a single storage benchmark file with Vitest's `--run` mode:

```sh
pnpm --filter @powersync/service-module-postgres-storage test test/src/storage_bench.test.ts --run
```

```sh
pnpm --filter @powersync/service-module-mongodb-storage test test/src/storage_bench.test.ts --run
```

```sh
pnpm --filter @powersync/service-module-mikroorm-storage test test/src/storage_bench.test.ts --run -t "MikroORM SQLite"
```

```sh
MIKROORM_MYSQL_STORAGE_TEST_URI="mysql://repl_user:good_password@localhost:3306/powersync" \
  corepack pnpm --filter @powersync/service-module-mikroorm-storage test test/src/storage_bench.test.ts --run -t "MikroORM MySQL"
```

The storage modules use their normal test database environment variables when supplied:

- `PG_STORAGE_TEST_URL` for Postgres storage benchmarks.
- `MONGO_TEST_URL` for MongoDB storage benchmarks.
- `MIKROORM_MYSQL_STORAGE_TEST_URI` for MikroORM MySQL storage benchmarks.

## Output

The summary table should include:

- Storage
- Version
- Scenario
- Source Rows
- Buckets
- Write ms
- Write rows/s
- Write MiB/s, based on the logical JSON size of source rows
- Sync drain ms
- Ops
- Read MiB/s, based on bytes emitted by the sync stream

The output is intentionally plain markdown so benchmark results can be pasted into issues, PRs, or follow-up analysis notes.

## Comparing Storage Implementations

Use the root comparison CLI to select and run multiple storage implementations:

```sh
pnpm benchmark:storage
```

Missing storage selections are prompted for interactively. They can also be supplied directly:

```sh
pnpm benchmark:storage --storage drizzle-sqlite,mikroorm-sqlite
```

Use `--output <path>` to retain the combined JSON results. The CLI runs each selected module in a separate Vitest
process, collects its machine-readable output, and prints throughput relative to the fastest selected implementation
for each scenario.

The CLI also writes a self-contained grouped SVG chart to `storage-benchmark-comparison.svg`, comparing write and read
MiB/s for every storage/version/scenario result. Use `--chart <path>` to select a different destination.

A benchmark test can also write its results directly by setting `POWERSYNC_STORAGE_BENCHMARK_OUTPUT` to the desired
JSON path. Without that environment variable, benchmark tests keep their existing console-only behavior.
