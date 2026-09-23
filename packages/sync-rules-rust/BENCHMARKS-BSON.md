# Combined BSON conversion and evaluation benchmarks

Measured on 2026-09-14, AMD Ryzen 9 7900X, Linux x64, Node 24.18.1, Node SQLite 3.53.1 and Rust SQLite 3.51.3. Release Rust build with LTO. 10,000 source documents, batches of 1,000, two warmup passes and seven measured iterations, rotating implementation order. Every implementation is checked against JS SQLite for exact output equality on every input before timing.

All paths start with raw BSON buffers and finish with evaluated data JSON plus parameter results. `rust-fields-async` is the previous API preceded by the current JS direct BSON converter. The combined native async API includes the input-buffer copy and output reconstruction. These are sequential batch preparation benchmarks, not a JS worker-pool comparison or production replication throughput.

| Workload            | Input bytes/row | Plain JS | JS + SQLite | Rust fields async | Rust BSON sync | Rust BSON async | Async / JS SQLite |
| ------------------- | --------------: | -------: | ----------: | ----------------: | -------------: | --------------: | ----------------: |
| flat-passthrough    |             163 |  192,831 |     179,423 |           124,374 |        241,858 |         217,631 |             1.21× |
| sample-passthrough  |             948 |   42,480 |      41,626 |            38,116 |         93,889 |          89,656 |             2.15× |
| sample-projection   |             948 |   92,346 |      90,030 |            80,520 |        158,295 |         147,273 |             1.64× |
| nested-json         |            3184 |   13,940 |      27,301 |            25,194 |         44,690 |          43,149 |             1.58× |
| sample-functions    |             948 |   84,864 |      77,749 |            75,255 |        135,543 |         125,320 |             1.61× |
| sample-fanout       |             948 |   16,058 |      15,944 |            23,971 |         37,234 |          37,468 |             2.35× |
| sample-many-queries |             948 |   47,998 |      44,836 |            48,550 |         68,038 |          65,816 |             1.47× |
| sample-parameters   |             948 |   97,873 |      92,413 |            79,335 |        145,587 |         132,770 |             1.44× |

Rates are median source rows per second, higher is better. Combined async Rust is faster on all eight workloads in this run: about **1.2–2.4× vs JS SQLite** and **1.1–3.1× vs plain JS**. The sample-shaped passthrough gains about **2.2×**; fanout gains about **2.4×**. Input-side JS object creation and per-field N-API conversion were substantial costs in the standalone evaluator.

The result supports combining conversion and evaluation. It does **not** establish the gain over the earlier two-JS-worker preparation pipeline, nor account for membership reads/writes, checksums, storage publication, source fetching or replica-ID extraction. One evaluator serializes its native tasks; reproducing two-worker parallelism requires separate evaluator instances and bounded scheduling.

## Workloads and scope

The sample workloads use [64 synthetic documents](bench/fixtures/sample-documents.json) derived from the existing replication benchmark’s anonymized shape generator, plus BSON ObjectIds, routing fields, doubles, dates and arrays. The nested workload contains an object with 40 nested items. BSON creation, config compilation and correctness comparisons occur outside the timer. See [fixture provenance](bench/fixtures/README.md).

The nested JSON query uses a simple object path shared by both JS engines; the plain JS evaluator does not support the initially tried array-element path equivalently. No comparisons with differing output were timed in this saved run.

The current implementation fully converts every document, even for projections. It does not yet exploit selective BSON field conversion, reuse serialized projections across fanout, or compute checksums in Rust. No forced GC or CPU isolation is used; these are local exploratory results, not CI thresholds.

## Reproduction

From this package with the repository Node version active:

```sh
pnpm build
BENCHMARK_ROWS=10000 BENCHMARK_ITERATIONS=7 BENCHMARK_BATCH_SIZE=1000 pnpm benchmark:bson
```

[Raw samples, byte throughput and environment details](bench/results/2026-09-14-bson.json). Earlier [object-returning](BENCHMARKS.md) and [serialized-output](BENCHMARKS-SERIALIZED.md) evaluator-only benchmarks exclude BSON conversion and are not directly comparable.
