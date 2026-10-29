# Parallel Rust BSON preparation benchmarks

Measured on 2026-09-14, AMD Ryzen 9 7900X (12 physical cores), Linux x64, Node 24.18.1, native SQLite 3.51.3, release build with LTO. **40,000 documents, batches of 1,000, two warmups and seven measured iterations.** `UV_THREADPOOL_SIZE=8` for every concurrency level. These measurements use the same fixtures as the [sequential comparison](BENCHMARKS-BSON.md), with more batches to sustain parallel execution.

Each concurrency slot has an independent Rust evaluator and SQLite connection, with one outstanding async BSON batch. A bounded window consumes results in source order; waiting for earlier batches is included. Input copies, native conversion/evaluation/serialization and JS output construction/consumption are timed. Config compilation, fixture generation, source fetching, replica IDs, checksums and storage are excluded. Before timing, every batch at every concurrency level is compared exactly with the JS SQLite implementation.

Each cell is **source rows/s · input MB/s** (decimal MB). Rates are medians; fanout counts input documents, not emitted operations.

| Workload            |         1 batch |       2 batches |       4 batches |       8 batches | Best / 1 |
| ------------------- | --------------: | --------------: | --------------: | --------------: | -------: |
| flat-passthrough    |  217,644 · 35.6 |  402,773 · 65.9 |  529,186 · 86.6 |  550,785 · 90.1 |    2.53× |
| sample-passthrough  |   88,152 · 83.6 | 170,852 · 162.1 | 291,971 · 277.0 | 381,497 · 362.0 |    4.33× |
| sample-projection   | 143,272 · 135.9 | 280,967 · 266.6 | 517,070 · 490.6 | 651,181 · 617.8 |    4.55× |
| nested-json         |  43,010 · 137.0 |  82,908 · 264.0 | 154,081 · 490.7 | 250,086 · 796.4 |    5.81× |
| sample-functions    | 123,955 · 117.6 | 245,088 · 232.5 | 436,791 · 414.4 | 520,941 · 494.3 |    4.20× |
| sample-fanout       |   36,997 · 35.1 |   72,059 · 68.4 | 127,048 · 120.5 | 119,095 · 113.0 |    3.43× |
| sample-many-queries |   64,904 · 61.6 | 120,548 · 114.4 | 198,062 · 187.9 | 189,848 · 180.1 |    3.05× |
| sample-parameters   | 136,252 · 129.3 | 255,716 · 242.6 | 407,887 · 387.0 | 427,332 · 405.5 |    3.14× |

Two batches give roughly 1.9× the single-batch throughput on these workloads. Four batches give about 2.4–3.6×. Eight batches help nested JSON most (5.8×), but fanout and many-query workloads are slower at eight than at four. Flat-row and parameter workloads also show diminishing returns beyond four.

This supports bounded parallel evaluation, with four slots a useful starting point for further integration benchmarks. It does not identify the exact bottleneck at eight slots: JS output construction, allocation/GC, memory bandwidth and scheduling can all contribute; no CPU profile was collected in this run. Native thread-pool size is process-wide and shared with other libuv work. Multiple calls on one evaluator would serialize at its SQLite mutex and would not reproduce this test.

No production throughput or improvement over the previous JS worker pool is established here. There is no storage overlap or publication work in the benchmark. Measurements rotate concurrency order; they do not isolate cores or force GC.

## Reproduction

From this package with the repository Node version active:

```sh
pnpm build
pnpm benchmark:bson:parallel
```

The script sets the native pool size before launching Node. `BENCHMARK_ROWS`, `BENCHMARK_ITERATIONS` and `BENCHMARK_BATCH_SIZE` override defaults. Output is `benchmark-bson-parallel-results.json`.

[Raw samples and environment details](bench/results/2026-09-14-bson-parallel.json).
