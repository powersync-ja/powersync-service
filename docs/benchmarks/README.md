# Benchmarking Docs

This folder contains documentation related to the PowerSync service benchmarking suite. This aims to cover:

- What the benchmarking suite is
- What the benchmarks cover
- How benchmarks run
- Benchmarking data and scenarios
- Adding new benchmarks
- Output handling and artifacts

## What is the benchmarking suite

This benchmark suite aims to provide more metrics that we can point to in how the PowerSync service performs, allowing for comparisons between different versions of the service, and with different storage and source databases.

The benchmarks are split into sections, with each section having its own `.bench.ts` file run by Vitest. Vitest handles running the benchmark tests and filtering them via [test tags](https://vitest.dev/guide/test-tags.html). Currently the suite has the following sections:

- API
- Replication
- Storage
- Combined

### API

The API benchmarks test the path from bucket storage to a client through the service's `/sync/stream` HTTP endpoint. They cover both initial sync and post-initial sync (streaming) from MongoDB and PostgreSQL bucket storage.

Each iteration seeds bucket storage directly and starts the API in an untimed setup step. The measured boundaries depend on the mode:

- **Initial sync:** measures from opening the HTTP request until the client receives `checkpoint_complete` for the seeded data.
- **Streaming:** measures from after writes are committed until the client receives the target checkpoint's `checkpoint_complete` on the existing connection.

Each iteration checks the HTTP status, content type, checkpoint completion, and expected buckets and operations before being marked as passed or failed.

### Replication

The replication benchmarks test the path from a source database to a visible checkpoint in bucket storage. They exercise the source connector and storage writer together, including evaluating rows against the sync config and writing the resulting bucket operations.

For snapshot cases, the source data is seeded during setup, and timing runs from releasing replication until the target checkpoint is visible. Streaming cases complete and verify the initial snapshot during setup, then measure from the first workload transaction's source commit until a checkpoint covering the final transaction is visible.

The current standalone replication cases cover MongoDB snapshots and streaming, and PostgreSQL snapshots, into either MongoDB or PostgreSQL bucket storage. Each iteration verifies snapshot completion, the checkpoint's source position, and the expected bucket contents.

### Storage

The storage benchmarks test the service's bucket storage interfaces directly. The current cases cover PostgreSQL storage versions 1 and 2, and MongoDB storage versions 1 through 4, in three modes:

- **Write:** measures saving generated rows through the storage writer, flushing batches, and committing an unblocked checkpoint.
- **Read:** seeds data during untimed setup, then measures reading the checkpoint, resolving buckets, fetching checksums, and downloading bucket operations.
- **Write-read:** measures both steps in sequence, recording separate write and read durations as well as their combined duration.

Each iteration verifies the checkpoint position, bucket counts, checksum operation counts, and expected bucket contents. Write-only cases read back the data during verification, outside the measured write boundary.

### Combined

Combined benchmarks test the entire flow from source database to client, covering replication, bucket storage writes and reads, and API streaming. The current cases use MongoDB or PostgreSQL sources with either storage implementation.

Initial sync cases seed source data during setup, then measure from requesting replication to start until the client completes its initial sync. The benchmark waits for the snapshot checkpoint to become visible before opening the client's HTTP request. Streaming cases complete replication and the client's initial sync during setup, then measure from starting the source workload commits until the existing client connection receives the target checkpoint's completion.

Both modes record replication and HTTP read durations alongside the overall duration. Verification checks the expected data in both storage and the client response, and confirms that the client completes the same checkpoint that was observed in storage.
