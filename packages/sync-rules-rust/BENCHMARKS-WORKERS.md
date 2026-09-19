# Four Rust threads versus four JS workers

Measured on 2026-09-14 using 40,000 raw BSON documents per workload, 1,000-document batches, two warmups and seven measured iterations. Rates use median wall time. Each cell below is **source rows/s · input MB/s**, where MB is decimal (1,000,000 bytes). Fanout counts source documents, not emitted operations.

## What is timed

All three implementations use the same bounded scheduler: four independent evaluators, one outstanding batch per evaluator, and consumption in source order. The next batch for a slot is dispatched after its preceding result is consumed. Waiting for earlier batches, Promise coordination, dispatch, input copying, output reconstruction and result consumption are included. There is no subtraction of coordination overhead or extrapolation from single-thread throughput.

- **JS plain / JS SQLite:** four Node worker threads. The main thread builds per-row transport metadata and posts raw BSON without a transfer list. Workers convert BSON using the current direct JS converter, evaluate with the selected engine, and serialize data with JSONBig. Results cross back using structured cloning, numeric source indexes and flattened parameter lookup values. The main thread reconstructs source references and lookup objects.
- **Rust:** four independent evaluators/SQLite connections, using `evaluateBsonAsync` with `UV_THREADPOOL_SIZE=4`. Input BSON is copied into native ownership on the main thread. Background tasks convert BSON, evaluate and serialize JSON. Native results are converted to JS and source/lookup objects reconstructed on the main thread.

Both have a coordinating JS main thread in addition to the four processing threads. Native pool threads are shared with other libuv work; the benchmark does not introduce competing libuv jobs.

The first scope measures conversion, evaluation and serialization. The second adds replica ID extraction, BSON replica ID output, replica subkeys, delete checksums and data checksums. **These extra operations run inside each JS worker or the same Rust background task**, using `prepareBsonAsync` for Rust. The native implementation uses SHA-256 and UUID-v5 crates; JS uses the existing service helpers. “Full preparation” below names this benchmark scope; source fetching, storage writes and replication publication are still excluded.

## Conversion, evaluation and serialization

| Workload            | JS plain workers | JS SQLite workers |            Rust | Rust / faster JS |
| ------------------- | ---------------: | ----------------: | --------------: | ---------------: |
| flat-passthrough    |   396,606 · 64.9 |    344,101 · 56.3 |  546,836 · 89.5 |            1.38× |
| sample-passthrough  |  120,133 · 114.0 |   124,624 · 118.2 | 318,610 · 302.3 |            2.56× |
| sample-projection   |  248,054 · 235.4 |   240,301 · 228.0 | 505,329 · 479.5 |            2.04× |
| nested-json         |   48,634 · 154.9 |    89,831 · 286.1 | 160,681 · 511.7 |            1.79× |
| sample-functions    |  230,943 · 219.1 |   215,763 · 204.7 | 470,886 · 446.8 |            2.04× |
| sample-fanout       |    45,234 · 42.9 |     45,803 · 43.5 | 128,727 · 122.1 |            2.81× |
| sample-many-queries |  120,931 · 114.7 |   119,156 · 113.1 | 196,844 · 186.8 |            1.63× |
| sample-parameters   |  230,541 · 218.7 |   230,714 · 218.9 | 418,527 · 397.1 |            1.81× |

## Including replica identity, subkeys and checksums

| Workload            | JS plain workers | JS SQLite workers |            Rust | Rust / faster JS |
| ------------------- | ---------------: | ----------------: | --------------: | ---------------: |
| flat-passthrough    |   207,990 · 34.0 |    200,710 · 32.8 |  285,794 · 46.8 |            1.37× |
| sample-passthrough  |    88,895 · 84.3 |     89,093 · 84.5 | 217,643 · 206.5 |            2.44× |
| sample-projection   |  170,102 · 161.4 |   162,998 · 154.7 | 311,466 · 295.5 |            1.83× |
| nested-json         |   44,434 · 141.5 |    78,187 · 249.0 | 137,969 · 439.4 |            1.76× |
| sample-functions    |  159,017 · 150.9 |   147,255 · 139.7 | 265,255 · 251.7 |            1.67× |
| sample-fanout       |    33,668 · 31.9 |     33,074 · 31.4 |  100,756 · 95.6 |            2.99× |
| sample-many-queries |    83,102 · 78.8 |     80,068 · 76.0 | 130,161 · 123.5 |            1.57× |
| sample-parameters   |  175,862 · 166.9 |   176,175 · 167.2 | 262,397 · 249.0 |            1.49× |

## Interpretation and limitations

With all preparation in background tasks, Rust is 1.37–2.99× faster than the faster JS worker variant across these workloads. Conversion/evaluation alone is 1.38–2.81× faster. The complete preparation result is the more relevant comparison for replacing the worker approach. Moving only BSON conversion and evaluation leaves substantial per-row work on the coordinating thread; the new API removes identity parsing and hashing from that thread, while retaining input copying, N-API object construction and JS source/lookup reconstruction.

The worker implementation is adapted from `optimize-replication` at `5b92e14f32b3a50bdd438b30ebdb4efd0fbdb039`, specifically `MongoRowPreparation.worker.ts` and `RowPreparation.ts`. It reproduces message cloning and output reconstruction using the evaluator APIs on this branch. It is not the entire original replication pipeline: the harness uses one fixed source table and all its relevant queries, omits per-row selection-cache routing and the diagnostic source-row `id` field, and adds identity/checksum results in a separate pass. Workers compile configs at startup instead of loading that branch's serialized hydrated config. These differences limit claims about exact production throughput.

The six variants rotate measurement order. They remain initialized throughout each workload: only four processing slots are active, but sixteen JS worker isolates are resident across both JS engines and scopes. This is not a memory-footprint comparison. Cores are not pinned, GC is not forced, and no event-loop-latency or CPU profile is collected. A single batch size and machine do not establish latency behavior or service-wide scaling.

Every batch from every variant is checked against JS SQLite before timing, including exact serialized payloads, parameter lookups, replica IDs and checksums where applicable. An additional validation run explicitly checks reconstructed source-reference identity, which ordinary deep equality omits for non-enumerable properties. The harness now includes that check in subsequent runs too. These workload checks do not remove the native compatibility gaps documented in the README.

Implementation validation: 157 integration tests and 10 Rust unit tests pass; TypeScript coverage is 100%, native line coverage is 96.49%, and the new preparation module has 97.58% line coverage. TypeScript builds, Clippy and formatting checks pass. The repository-wide project-reference validator still reports the unrelated untracked `packages/service-core-benchmarks` missing from root references.

The earlier prototype that computed identities/checksums on the JS main thread is retained in [the preceding raw run](bench/results/2026-09-14-workers-js-metadata.json). It demonstrated why these operations belong in the same background task: native conversion/evaluation throughput alone did not translate to full preparation throughput. That historical run is not mixed into the tables above.

## Startup and reproduction

Environment: AMD Ryzen 9 7900X 12-Core Processor, 24 available logical CPUs, linux x64, Node v24.18.1, Node SQLite 3.53.1, native SQLite 3.51.3; release addon with LTO.

Startup is excluded from steady-state throughput and recorded separately. Values below are median milliseconds (min–max) over the eight workloads and two scopes. Setup constructs and readies four evaluators/workers; the first wave processes four batches (4,000 documents) before correctness checks and warmup. These are repeated initializations within one process, not fresh-process launch measurements. Parent config compilation is excluded; worker config compilation is included, unlike the original branch's serialized-config hydration.

| Implementation      |            Setup ms |     First wave ms |
| ------------------- | ------------------: | ----------------: |
| js-plain-workers-4  | 434.6 (415.1–472.0) | 60.0 (25.3–161.6) |
| js-sqlite-workers-4 | 438.7 (423.4–478.6) | 56.1 (28.2–159.5) |
| rust-4              |       0.4 (0.3–0.8) |  19.5 (11.1–55.9) |

From this package with the repository Node version active:

```sh
pnpm build
pnpm benchmark:bson:workers
```

`BENCHMARK_ROWS`, `BENCHMARK_ITERATIONS` and `BENCHMARK_BATCH_SIZE` override defaults. The script writes `benchmark-worker-comparison-results.json`. Fixture creation, config setup and correctness assertions are outside steady-state timing.

[Raw samples and environment details](bench/results/2026-09-14-workers.json). Benchmark entry point: [bench/bson.mjs](bench/bson.mjs); shared evaluation: [bench/evaluation.mjs](bench/evaluation.mjs); transport: [bench/worker-pool.mjs](bench/worker-pool.mjs); worker: [bench/preparation.worker.mjs](bench/preparation.worker.mjs).
