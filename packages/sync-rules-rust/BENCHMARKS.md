# Initial evaluator benchmarks (object output)

Historical baseline before native payload serialization. See [serialized-output benchmarks](BENCHMARKS-SERIALIZED.md) for the current implementation.

Measured on 2026-09-14, AMD Ryzen 9 7900X, Linux x64, Node 24.18.1. Node bundles SQLite **3.53.1**; the Rust build bundles **3.51.3**. Release Rust build with LTO, 10,000 source rows, batches of 1,000, two warmup passes and seven measured iterations. The same workload inputs and output shape are used for every implementation, with full result equality checked before timing.

These are evaluator benchmarks, not replication-throughput measurements. They exclude config compilation, BSON conversion, final payload serialization/checksums, and storage. See [raw samples and environment details](bench/results/2026-09-14.json).

## Full evaluation path

Median source rows per second; higher is better. Rust measurements include input conversion, the native call, output conversion and TypeScript result reconstruction. The asynchronous variant additionally dispatches computation to a native background thread.

| Workload            | Plain JS | JS + SQLite | Rust sync | Rust async |
| ------------------- | -------: | ----------: | --------: | ---------: |
| Passthrough         |  850,437 |     731,078 |   120,114 |    121,114 |
| Projection + filter |  970,723 |     758,302 |   247,467 |    239,523 |
| JSON expressions    |  254,395 |     507,545 |   223,012 |    208,183 |
| Custom functions    |  398,748 |     281,907 |   174,668 |    171,767 |
| Bucket fanout       |  236,159 |     246,401 |    43,950 |     43,671 |
| Eight queries       |  148,210 |     126,706 |    68,711 |     67,958 |
| Parameter index     |  847,717 |     704,120 |   316,461 |    296,678 |
| Parameter expansion |  366,846 |     357,933 |   163,136 |    164,053 |

**This initial standalone evaluator is slower than both existing implementations on all eight full-path workloads.** It does not demonstrate a replication speedup. Moving computation off the event loop works, but does not by itself improve total throughput.

## Native execution diagnostic

The diagnostic converts input once, evaluates the already-native rows seven times and consumes native results without reconstructing JavaScript objects. It uses the same Rust engine but a single 10,000-row batch, so these figures are **not** directly comparable to the full-path implementations or a measured decomposition of their timings.

| Workload            | Native execution rows/s |
| ------------------- | ----------------------: |
| Passthrough         |               1,095,821 |
| Projection + filter |               1,263,759 |
| JSON expressions    |                 772,453 |
| Custom functions    |                 562,377 |
| Bucket fanout       |                 266,147 |
| Eight queries       |                 206,804 |
| Parameter index     |               1,556,191 |
| Parameter expansion |                 531,095 |

The gap between native-only and full-path throughput supports investigating the current value-by-value Node-API transport and result reconstruction before optimizing SQLite execution. Projection-only plans already avoid transferring unused source fields. Star projections and fanout still reconstruct many strings, BigInts and objects across the boundary.

A future integrated native conversion/evaluation/serialization path could avoid several of these intermediate representations. That potential is not established by these standalone results, and native-only timings must not be presented as the expected integrated speedup.

## Reproduction

From this package, with the repository's Node version active:

```sh
pnpm build
BENCHMARK_ROWS=10000 BENCHMARK_ITERATIONS=7 BENCHMARK_BATCH_SIZE=1000 pnpm benchmark
```

The benchmark writes `benchmark-results.json`. It rotates implementation order between iterations and retains all timing samples. It does not force GC or isolate CPU cores; these are local exploratory results, not a stable CI performance threshold.
