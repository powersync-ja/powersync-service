# PowerSync Service Core Benchmarks

## MongoDB replication throughput

The MongoDB throughput suite runs the real source connector against MongoDB storage v3,
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

The example output payloads have 31–42 fields and a mean size of approximately 2.5 KB
(range 1.4–3.2 KB). This models structure and approximate sizes, not the original semantics
or compressibility. Generated data includes additional benchmark identity, routing and marker
fields. Actual output sizes are recorded as `put_payload_bytes_min/mean/max` for comparison.

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
| `BENCHMARK_S3`                                        | `true`                                             | Set `false` for MongoDB v3 with inline storage only                                           |
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

After a successful run `pnpm benchmark:report` can be run to generate report, both in CLI, and saved to a `benchmark-artifacts` folder.

### Available commands

- `benchmark:test` - runs the test suite
- `benchmark:report` - generates the report for a previous benchmark run
