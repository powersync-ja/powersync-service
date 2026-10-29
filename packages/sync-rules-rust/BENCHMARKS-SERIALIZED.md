# Serialized-output evaluator benchmarks

Measured on 2026-09-14, AMD Ryzen 9 7900X, Linux x64, Node 24.18.1. Node SQLite 3.53.1; Rust SQLite 3.51.3. Release build with LTO, 10,000 source rows, batches of 1,000, two warmups and seven measured iterations.

All implementations return serialized data payloads. JavaScript uses JSONBig; Rust serializes inside native evaluation, on the background thread for the async API. Exact result equality is checked before timing. Parameter results retain their existing object representation.

These are evaluator measurements, including input/output conversion and data-payload serialization. They exclude config compilation, source conversion, checksums and storage. The [previous object-output run](BENCHMARKS.md) excluded payload serialization and is not a like-for-like throughput comparison.

| Workload            | Plain JS | JS + SQLite | Rust sync | Rust async |
| ------------------- | -------: | ----------: | --------: | ---------: |
| passthrough         |  281,691 |     257,490 |   166,367 |    151,289 |
| projection-filter   |  550,064 |     433,126 |   302,529 |    267,944 |
| json                |  182,769 |     352,635 |   277,922 |    257,626 |
| native-functions    |  254,224 |     193,659 |   214,790 |    199,587 |
| bucket-fanout       |   73,838 |      76,155 |    75,107 |     73,909 |
| many-queries        |   88,468 |      80,932 |    90,125 |     88,521 |
| parameter-index     |  767,430 |     686,888 |   291,834 |    287,460 |
| parameter-expansion |  340,739 |     346,936 |   161,772 |    154,781 |

Values are median source rows/second. Rust is near JS + SQLite on bucket fanout and modestly faster for native functions and many queries in this run; input conversion still outweighs execution gains on simpler data workloads. Parameter-only workloads are unchanged in design and remain slower. These local exploratory results do not establish a production replication speedup.

The native-only diagnostic in the raw report includes serialization but excludes boundary conversion and JS reconstruction. It uses one 10,000-row batch and must not be interpreted as full-path throughput.

Reproduce from this package using the repository Node version:

```sh
pnpm build
BENCHMARK_ROWS=10000 BENCHMARK_ITERATIONS=7 BENCHMARK_BATCH_SIZE=1000 pnpm benchmark
```

[Raw timing samples and environment details](bench/results/2026-09-14-serialized.json). Measurements rotate implementation order, without forced GC or CPU isolation.
