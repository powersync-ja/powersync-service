# PowerSync Service Core Benchmarks

## Raw snapshot-batch benchmark (no source database)

Use `pnpm benchmark:snapshot` to isolate the snapshot writer path with pre-generated BSON
documents. It uses MongoDB storage v4 and S3 by default, with `storeCurrentData: false` and
`skipExistingRows: true`, matching the MongoDB snapshotter. Each page is flushed before
its snapshot cursor/progress is persisted. The timed interval ends after marking the table
snapshot complete and committing a simulated safe checkpoint.

```sh
pnpm build
pnpm benchmark:up:storage
BENCHMARK_ROWS=200000 BENCHMARK_BATCH_SIZE=6000 \
  BENCHMARK_S3_INLINE_THRESHOLD_BYTES=16000 BENCHMARK_PROFILE=timings \
  pnpm benchmark:snapshot
```

The default is 10,000 rows, 6,000-row pages, no warmup and one measured iteration.
Pages also have a simulated 16 MiB BSON limit. The same storage, S3, shape, user-count,
iteration and profiling settings as `benchmark:changes` apply. `BENCHMARK_MUTATIONS` is
ignored: snapshot input consists entirely of inserts. Rows are ordered by source `_id`.

Fixture generation, storage setup, verification and cleanup are outside the timed interval.
The benchmark does not connect to a source database, query a collection, run concurrent CDC,
or wait for a real source checkpoint marker. It starts with empty storage; it does not simulate
resumed snapshots with existing rows. Verification checks every row's identity/version/routing,
operation and bucket counts, the final checkpoint, table snapshot completion and S3 uploads.

Results use `replication.snapshot-batches.*` artifact names and the `snapshot_batches` timing
boundary, with the same JSON reports and CPU-profile files as the change-batch benchmark.

## Raw change-batch benchmark (no source database)

Use this focused benchmark when optimizing the code after MongoDB delivers change events:

```sh
# Once, from the repository root (also rebuild after changing production code):
pnpm --filter @powersync/service-core-benchmarks build
cd packages/service-core-benchmarks
pnpm benchmark:up:storage

# Defaults: 10,000 inserts, 100 users/buckets, sample shape, v4 + S3,
# no warmups, one measured iteration.
pnpm benchmark:changes
pnpm benchmark:report
# Open benchmark-artifacts/report/output.html in a browser.
```

Keep the services running between executions; `benchmark:up:storage` is only needed to
start them. It starts MongoDB storage, MinIO and the latency proxies, without a source DB.
`benchmark:up` also works if the full replication stack is already running.

The timer starts with pre-generated raw BSON change-event buffers in memory. It includes
the production `parseChangeDocument`, `DirectSourceRowConverter`, and shared `writeMongoChange`
path, sync-rule evaluation, bucket routing, serialization/compression, MongoDB v4 writes,
S3 uploads, per-page publication/resume-position persistence, and the final checkpoint commit.
MongoDB v4 queues page publication so processing continues while earlier pages upload; the
final commit waits for all queued progress. Preparation blocks can share a publication group.
The writer uses `storeCurrentData: false`, as the MongoDB connector does for complete postimages.
There is no source connection, snapshot, change-stream polling, or source checkpoint-marker round trip.
Collection discovery, transaction/split-event assembly, reconnect/recovery, and the outer connector
loop are outside this benchmark. Synthetic events belong to one already resolved collection and
contain complete, independent changes. One safe checkpoint marker is simulated after the backlog;
this is a focused processing benchmark, not an end-to-end latency measurement.

Fixture generation, storage setup, optional prefill, verification, and cleanup are outside the
timed interval. `run_wall_ms` reports the complete runner time (excluding Vitest startup), and
`fixture_generation_ms`, `setup_ms`, and `verification_ms` help explain overhead. Node CPU/memory
sampling covers the processing interval; the monitor's existing `load_generator` label refers
to that same process here. Database and MinIO CPU are not measured.

```sh
# More batches and a mix of inserts, updates and deletes:
BENCHMARK_ROWS=10000 BENCHMARK_USERS=100 BENCHMARK_BATCH_SIZE=1000 \
  BENCHMARK_MUTATIONS=mixed pnpm benchmark:changes

# Repeat for more stable comparisons:
BENCHMARK_WARMUPS=1 BENCHMARK_ITERATIONS=3 pnpm benchmark:changes

# Inject downstream response latency (milliseconds), then compare with baseline:
pnpm benchmark:latency storage 2
pnpm benchmark:latency minio 10
BENCHMARK_LABEL=storage-2ms-s3-10ms pnpm benchmark:changes
pnpm benchmark:latency storage 0
pnpm benchmark:latency minio 0
```

`BENCHMARK_ROWS` is the number of **measured change events**. `BENCHMARK_MUTATIONS` accepts
`insert` (default), `update`, `delete`, or `mixed` (round-robin insert/update/delete). Inserts
require no data prefill. Other modes prefill only the rows to update/delete directly into
storage using the same conversion/save path; the prefill is excluded from throughput and S3
upload counters. Verification checks all resulting operations, final row versions/deletions,
user routing, checkpoint position, and required S3 uploads. It reads output in bounded pages.

`BENCHMARK_BATCH_SIZE` defaults to 6,000 events, also capped at 64 MiB per batch; a final
partial batch is allowed. Events are generated once and reused across iterations, each with
fresh storage state. Memory grows with the total raw BSON fixture size, so start with 10,000
rows before increasing it. `raw_bson_bytes` and the throughput report's `logical_mib_per_second`
measure input BSON here, not output JSON or compressed S3 bytes. `put_payload_bytes_mean`
reports the actual output payload size (including prefill PUTs in mutation modes).

The existing data-shape controls below also apply: `BENCHMARK_SHAPE=sample|synthetic`,
`BENCHMARK_PAYLOAD_BYTES`, and `BENCHMARK_USERS`. Edit `createDocument` to customize the data.
`BENCHMARK_TIMEOUT_MS` defaults to 300,000. Existing `BENCHMARK_MONGODB_STORAGE_URL` and
`BENCHMARK_S3_*` endpoint/credential/inline-threshold settings apply. `BENCHMARK_S3=false`
disables object storage. Source settings and `BENCHMARK_MUTATION_COUNT` are unused.
This benchmark is opt-in via `benchmark:changes`; the existing quick suite skips it.

### Profiling change batches

To compare publication sizing, run `pnpm benchmark:publication-sizes`. This runs 6,000,
12,000, 24,000 and then 6,000 events per source page sequentially, with 200,000 changes,
two measured iterations and timings-only profiling by default. The repeated baseline helps
expose run drift. Existing storage, latency, workload and profiling environment settings are
retained; page size is overridden. Pass explicit sizes with
`pnpm benchmark:publication-sizes 6000 12000`. The command prints throughput/upload counts
and writes `benchmark-artifacts/publication-sizing-<timestamp>.json` linking the full results.
Do not run other change benchmarks concurrently with the sweep.

This tests larger source pages as a proxy for larger publication groups. Existing publication
byte limits still split oversized groups. It does not implement cross-page coalescing or test
low-load checkpoint latency. Inspect `publication.transaction.count` in the profile summaries
for actual group counts; page counts need not equal publication counts.

After rebuilding, run from this package with your usual storage and workload settings:

```sh
BENCHMARK_PROFILE=true BENCHMARK_ROWS=100000 BENCHMARK_ITERATIONS=2 pnpm benchmark:changes
# Stage timings without CPU sampling:
BENCHMARK_PROFILE=timings pnpm benchmark:changes
```

Each measured iteration writes a summary JSON and, in CPU mode, separate main-thread and
preparation-worker `.cpuprofile` files under `benchmark-artifacts/profiles/`. Override the
directory with `BENCHMARK_PROFILE_DIR`. Load CPU profiles in Chrome DevTools' Performance
panel. Summary frames report sampled **self time**, not inclusive call-tree time.

Timings cover preparation, message construction/delivery, membership reads, reconciliation,
packing, uploads, publication transactions and waits for pipeline capacity. They are bounded
aggregates (count, total and maximum), not per-row traces. Spans overlap and nest: do not sum
them as exclusive CPU time. Worker input delivery includes startup and profiler initialization;
output delivery includes cloning and event-loop scheduling. Neither measures pure serialization.
The first measured worker round trip is recorded separately to expose startup effects.

Publication internals appear under `transaction.*`: `fence`, `bucket_data`, `parameters`,
`current_data`, `bucket_states`, `clear_error`, `resume_lsn`, `persisted_op`, `commit` and
`abort`. Bucket-data subspans separate `insert`, `publish_uploads` (lifecycle marker removal)
and `usage`; current-data subspans separate `source_tables` and `membership_write`.
Parent spans include their subspans. All include client work, scheduling and database waits,
not just server execution. `callback.count` counts transaction attempts; retries repeat phase
measurements, including failed attempts. `commit` measures driver commit calls (including
internal retries), while `publication.transaction` covers the complete `withTransaction` call,
including retry handling. These timings cover pipeline publication, not the later checkpoint
commit. Empty phases may be absent or complete without a database request.

Warmups and prefill are excluded. Worker CPU capture starts on its first measured request and
ends after the final commit, so it includes trailing idle time during publication. Throughput
excludes profile collection/file writing, but process resource monitoring includes that overhead.
Profiling itself adds overhead; compare performance with profiling disabled (unset or `false`).
For an initial analysis, see [replication profiling findings](../../docs/storage/mongodb-replication-profiling.md).

## MongoDB replication throughput

The MongoDB throughput suite runs the real source connector against MongoDB storage v4,
with MinIO/S3 enabled by default. It is opt-in and does not enlarge the existing quick suite.

From the repository root, build the benchmark and its references:

```sh
pnpm --filter @powersync/service-core-benchmarks build
cd packages/service-core-benchmarks
pnpm benchmark:up
BENCHMARK_USERS=100 BENCHMARK_LABEL=baseline pnpm benchmark:replication
pnpm benchmark:report
pnpm benchmark:down
```

The Compose stack provides separate MongoDB replica sets for source and storage, MinIO,
and three independently configurable TCP proxies. Ports bind to localhost. These services
use disposable container storage; `benchmark:down` removes their data. Each run also uses
a fresh storage database, a fresh source database per iteration, and a unique S3 prefix.
Normal cleanup removes only those generated resources. Interrupted runs can leave them behind.
The source and storage URLs may specify authentication; their database names are not reused.

For a quick end-to-end check:

```sh
BENCHMARK_ROWS=378 BENCHMARK_BATCH_SIZE=42 BENCHMARK_USERS=7 \
  BENCHMARK_WARMUPS=0 BENCHMARK_ITERATIONS=1 pnpm benchmark:replication
```

### Data shape and user buckets

The default workload uses anonymized profiles derived from **the document payloads of 378
processed bucket-data operations** in `sample.bson`. The raw BSON is not required at runtime
and is not included in the benchmark. Only synthetic field names, structural/type descriptors,
and sizes are retained in [sample-op-shapes.json](src/fixtures/sample-op-shapes.json).
JSON-encoded objects and arrays in the output are reconstructed as native source values,
so replication encodes them back into operation payloads. Strings are generated from unrelated
deterministic seeds. Numbers and booleans are synthesized; nulls and empty values preserve
their types. Names inside nested structures are anonymized too.

The original example output payloads have 31–42 fields and a mean size of approximately 2.5 KB
(range 1.4–3.2 KB). The default sample generator now retains approximately 38% of each profile's
fields, targeting **1 KiB per output PUT payload**, including benchmark identity, routing and
marker fields. Field selection varies deterministically across rows and stays fixed across
revisions of the same row. Retained fields preserve their types, nested structure, and value sizes.
This models a smaller document with similar field shapes, not the original semantics or
compressibility. Sizes vary by row; the target excludes the operation envelope and does not apply
to REMOVE operations. Check `put_payload_bytes_mean` for the measured average (the full replication
suite also records min/max).

[createDocument](src/scenarios/mongodb-throughput-workload.ts) is the editing point for source
documents. Profiles cycle deterministically as row counts increase. Use `BENCHMARK_SHAPE=synthetic`
for the simpler hand-written payload generator; `BENCHMARK_PAYLOAD_BYTES` controls its padding.
For completely different scenarios, supply `scenario.createManifest` and `scenario.syncRule`.
Generated sequences must be deterministic and repeatable. JSON logical-byte counters are not BSON,
wire, or compressed S3 byte counts.

Documents are assigned round-robin across `BENCHMARK_USERS` users, each with a separate bucket.
The benchmark's verification parameters request all these buckets together. The permissive
parameter query is a benchmark fixture, not an example of production authorization rules.

To regenerate anonymized profiles from another BSON export (largest JSON object payload per op):

```sh
pnpm exec node src/scripts/import-op-shapes.ts ../../sample.bson src/fixtures/sample-op-shapes.json
pnpm exec prettier --write src/fixtures/sample-op-shapes.json
```

### Measurements

Each scenario has warmup and measured iterations, and each iteration gets a fresh source dataset.

| Phase     | Timed interval                                                                                     |
| --------- | -------------------------------------------------------------------------------------------------- |
| Snapshot  | Start replication after seeding → persisted checkpoint reaches the source target                   |
| Streaming | Start generating/submitting mutations → persisted checkpoint reaches the last target               |
| Catch-up  | Resume replication with mutations already committed → persisted checkpoint reaches the last target |

Catch-up first completes a snapshot, stops the service lifecycle, commits the workload, and
starts a fresh service lifecycle against the **same persisted stream**. It includes service
restart/cursor-resume overhead, so use sufficiently large workloads to measure sustained capacity.
Streaming includes producer generation, writes, transactions and marker overhead; catch-up
separates these from replication. Neither phase waits for checkpoints between transactions.

Timers use the parent's monotonic clock. Checkpoint polling has a 30 ms interval plus MongoDB/IPC
overhead. Full bucket-data verification runs **after timing and resource monitoring stop**,
in bounded pages, and sends only counts and size statistics to the parent. Seeding and generated
transactions are also bounded; lower the batch size for very large documents or transactions.
Post-images are automatically enabled. Update workloads issue real `$set` updates; deletes issue
deletes. Each transaction's last mutation inserts a target marker, including in delete mode.

Reports include rows/s, logical MiB/s, operation payload sizes, S3 uploads/bytes, and process
resource measurements. S3 counters exclude the setup snapshot for streaming/catch-up. With
inline threshold zero, verification fails if no S3 uploads occurred. Database resource
monitoring is still external: record CPU, RAM, disk and container settings alongside results.
Every replication run writes a unique JSON artifact so latency experiments do not overwrite
each other. Use `BENCHMARK_LABEL` to record the experiment; the git revision and Node version are recorded automatically.

### Configuration

| Variable                                              | Default                                            | Meaning                                                                                       |
| ----------------------------------------------------- | -------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `BENCHMARK_ROWS`                                      | `100000`                                           | Seeded documents                                                                              |
| `BENCHMARK_MUTATION_COUNT`                            | row count                                          | Streaming/catch-up mutations, including markers                                               |
| `BENCHMARK_BATCH_SIZE`                                | `1000`                                             | Mutations per transaction; must divide mutation count                                         |
| `BENCHMARK_USERS`                                     | `100`                                              | User buckets; positive and no greater than row count                                          |
| `BENCHMARK_MUTATIONS`                                 | `mixed`                                            | `insert`, `update`, `delete`, or `mixed`; non-insert modes require mutation count ≤ row count |
| `BENCHMARK_SHAPE`                                     | `sample`                                           | `sample` or `synthetic`                                                                       |
| `BENCHMARK_PAYLOAD_BYTES`                             | `1024`                                             | Synthetic padding; unused for sample profiles                                                 |
| `BENCHMARK_WARMUPS` / `BENCHMARK_ITERATIONS`          | `1` / `3`                                          | Iterations per phase                                                                          |
| `BENCHMARK_TIMEOUT_MS`                                | `3600000`                                          | Whole scenario timeout, including setup and verification                                      |
| `BENCHMARK_S3`                                        | `true`                                             | Set `false` for MongoDB v4 with inline storage only                                           |
| `BENCHMARK_S3_INLINE_THRESHOLD_BYTES`                 | `0`                                                | Force S3 uploads by default; set a larger value to study hybrid storage                       |
| `BENCHMARK_MONGODB_SOURCE_URL`                        | `mongodb://127.0.0.1:27117/?directConnection=true` | Proxied replicator connection                                                                 |
| `BENCHMARK_MONGODB_STORAGE_URL`                       | `mongodb://127.0.0.1:27118/?directConnection=true` | Proxied storage connection                                                                    |
| `BENCHMARK_MONGODB_WRITER_URL`                        | direct source on port `27217`                      | Producer connection; defaults to the source URL if you override it                            |
| `BENCHMARK_S3_ENDPOINT`                               | `http://127.0.0.1:19000`                           | Proxied MinIO endpoint                                                                        |
| `BENCHMARK_S3_BUCKET` / `BENCHMARK_S3_REGION`         | `powersync-benchmark` / `us-east-1`                | Existing bucket and signing region                                                            |
| `BENCHMARK_S3_ACCESS_KEY` / `BENCHMARK_S3_SECRET_KEY` | `minioadmin` / `minioadmin`                        | MinIO credentials                                                                             |
| `BENCHMARK_LABEL`                                     | `baseline`                                         | Free-form experiment label                                                                    |

For example, isolate a single phase with `pnpm benchmark:replication -t 'replication.catch-up'`.

### Artificial latency

Use the same proxy path for zero-latency and delayed baselines:

```sh
pnpm benchmark:latency source 20
BENCHMARK_LABEL=source-20ms pnpm benchmark:replication
pnpm benchmark:latency source 0
pnpm benchmark:latency storage 20
BENCHMARK_LABEL=storage-20ms pnpm benchmark:replication
pnpm benchmark:latency storage 0
pnpm benchmark:latency minio 50 5
BENCHMARK_LABEL=minio-50ms-jitter-5ms pnpm benchmark:replication
pnpm benchmark:latency minio 0
```

These commands add a [Toxiproxy downstream latency toxic](https://github.com/Shopify/toxiproxy#latency)
in milliseconds; the optional third argument is jitter. Zero removes the benchmark toxic.
Downstream delay adds response-path latency, not symmetric one-way delay on both directions.
Keep `directConnection=true` for these single-node replica sets: topology discovery must not
replace a proxy address with an advertised server address. The default producer connection
bypasses the source proxy, keeping source latency experiments focused on replication.
Set its URL to the proxy if you explicitly want to delay ingestion too.

Latency persists between runs until removed or the proxy container is recreated. Test each
dependency separately before combining delays. Very high latency can trigger driver timeouts
and retries; those runs measure degraded behavior rather than steady-state throughput.

Adds benchmarks for the areas of the PowerSync service.

This is generally used to test changes to different areas of the service, allowing for implementation comparisons that help improve the service as a whole.

## Usage

The benchmarks use environment variables to set the location of running instances of databases, which are used within the benchmarks:

| Environment Variable           | Usage                                                         | Default Value                                                      |
| ------------------------------ | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| PG_STORAGE_TEST_URL            | A Postgres URL for storage benchmarks                         | postgres://postgres:postgres@localhost:5432/powersync_storage_test |
| MONGO_TEST_URL                 | A MongoDB URL for storage benchmarks                          | mongodb://localhost:27017/powersync_test                           |
| BENCHMARK_POSTGRES_STORAGE_URL | A Postgres URL for **bucket storage** in replication tests    | NONE                                                               |
| BENCHMARK_POSTGRES_SOURCE_URL  | A Postgres URL for a **source** database in replication tests | NONE                                                               |
| BENCHMARK_MONGODB_SOURCE_URL   | A MongoDB URL for a **source** database in replication tests  | NONE                                                               |
| BENCHMARK_MONGODB_STORAGE_URL  | A MongoDB URL for **bucket storage** in replication tests     | NONE                                                               |

To run the tests call the following commands in the root of this repo:

```
pnpm install
pnpm build
pnpm benchmark:test
```

This installs and builds the service, then runs the entire suite of tests, which may take some time. To combat this, a subset can be selected using the [Vitest test tags](https://vitest.dev/guide/test-tags.html).

For example, running just the quick suite can be be done like so:

```
pnpm benchmark:test --tags-filter="quick"
```

A full list of available tags can be retrieved either from the [vitest config](./src/vitest.config.ts), or by running `pnpm benchmark:test --list-tags`.

After a run, `pnpm benchmark:report` prints a compact terminal summary and generates
`benchmark-artifacts/report/output.html`. Open that file directly in your browser—no server
or internet connection is needed. Each run has a summary card, searchable by scenario or
experiment label, with expandable timing statistics, counters, resource measurements and
diagnostics. Markdown (`output.md`) and JSON (`output.json`) summaries are also generated.

### Available commands

- `benchmark:test` - runs the test suite
- `benchmark:report` - generates the report for a previous benchmark run
