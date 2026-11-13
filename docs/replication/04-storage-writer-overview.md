# Storage Writer Overview

Source-specific streams write into bucket storage through shared storage writer interfaces. This page describes the concepts those interfaces represent; method-level behavior belongs in the JSDoc for `SyncRulesBucketStorage` and `BucketStorageBatch`.

## Storage

`SyncRulesBucketStorage` represents the persisted state for one replication stream.

Replication writes source changes, source table metadata, snapshot progress, resume positions, and checkpoint boundaries into that state. The sync API reads the same state as checkpoints, bucket data, parameter lookup rows, checksums, and checkpoint request acknowledgements.

A stream may contain one sync config or, in incremental storage, multiple sync configs. Replication-side writers evaluate the stream through the canonical parsed sync config set for that storage instance. Read-side storage serves one active sync config at a time.

## Writer Lifecycle

A source stream opens a writer for the unit of work that matches its source workflow. That may be a long-running streaming attempt, a table snapshot, a page of snapshot work, or a short setup step.

Writers batch source row changes and source metadata changes against the hydrated sync config state for the stream. Depending on the source, they may also retain raw current row state so later partial updates can be merged correctly. Snapshot writers can skip rows already copied by an earlier interrupted attempt.

The source connector is responsible for closing each writer cleanly so pending work is either persisted at the right boundary or discarded with the failed attempt.

## MongoDB Replication Batch Sizes

MongoDB storage uses one profile to coordinate input buffering, transaction splitting,
and v3/v4 pipeline backpressure. Configure it in the service config:

```yaml
storage:
  type: mongodb
  uri: mongodb://localhost:27017/powersync
  replication_batch_size: medium
```

The default is `medium`. `large` preserves the limits used before profiles were added.
Larger profiles can improve throughput by amortizing round trips, at the cost of
more retained memory and longer individual transactions. Smaller profiles can
help reduce memory use or transaction duration.

| Profile            | Input rows or checkpoints | Estimated input bytes | Transaction entries per category | Estimated transaction bytes | v3/v4 pipeline accounted-work target |
| ------------------ | ------------------------: | --------------------: | -------------------------------: | --------------------------: | -----------------------------------: |
| `small`            |                     4,000 |                 2 MiB |                            4,000 |                       4 MiB |                               32 MiB |
| `medium` (default) |                     8,000 |                 4 MiB |                            8,000 |                       8 MiB |                               64 MiB |
| `large`            |                    16,000 |                 8 MiB |                           16,000 |                      16 MiB |                              128 MiB |

These are flush and backpressure targets, not hard memory or MongoDB transaction
limits. A whole row is evaluated before splitting; high fanout or one oversized
row can exceed a target. Transaction counts apply separately to bucket operations,
parameter entries, and source records, preserving the existing splitting rules.
The pipeline target includes 4x packing headroom per transaction target across two
slots; it excludes source buffers and raw writer input. All profiles keep two
pipeline slots and the same current-row lookup partitioning, payload chunk sizes,
and object-storage request concurrency. The profile applies per replication
stream, so total memory also depends on active streams and writers.

## MongoDB V3/V4 Publication Pipeline

Writers on one replication lease share a two-batch pipeline. Each writer buffers
one normal input block (8,000 rows/checkpoints or 4 MiB estimated input with the
default `medium` profile). Preparation
admits complete blocks, or partial blocks at explicit boundaries. Interleaved
snapshot and streaming `save()` calls do not flush each other's input.

Preparation reads source records, evaluates changes and seals writes. Once a
batch owns a pipeline slot, preparation finishes by packing payloads and uploading
offloaded chunks. The successor can upload while its predecessor is uploading or
publishing. Publishing commits the prepared MongoDB writes in FIFO order, after
that batch's uploads finish. There are at most two batches in flight in total;
both can be uploading, but only one can publish to MongoDB. They share the existing
object-storage request limiter, with no additional upload queue. Source connectors
retain their existing bounded buffering/prefetch. Evaluation remains on the main thread.

Preparation sees the predecessor's immutable source-record delta on top of
majority reads. The delta includes tombstones and remains alive through the
preparation block even if its predecessor commits during a read. Before another
block can enter preparation, that predecessor has finished publication. Confirmed
publication clocks advance the preparation session, so older batches need no
retained overlay or cache.

Ordinary saves append rows and their event checkpoints synchronously, without a
mutex or lock trace. A flush captures and detaches its writer's current input
before yielding, then queues preparation under the writer mutex. Saves arriving
during that work use a fresh buffer; split-flush remainders stay private to the
captured input. A save that fills its buffer still waits for preparation capacity.

Explicit flushes and checkpoints wait for the admitted prefix and their captured
input. The pipeline privately tracks live writers from construction to disposal.
Metadata operations call `enqueueBufferedInputs()` synchronously before queueing
their own mutex operation. This captures every writer's current rows/checkpoints
and orders them before the metadata change. Later rows stay in fresh buffers and
are submitted after the barrier. The metadata operation awaits publication before
changing table state. Registering a writer does not mean its input has entered the
two-slot publication pipeline.
Failed publication invalidates the lease and stops the successor; replay starts
from persisted source progress. Partial-row repair notifications retain their
synchronous durability barrier.

The two slots and the profile’s accounted-work target (64 MiB by default) bound pipeline admission, with
headroom estimates for packing. These are not RSS limits: a preparation block or
single oversized row can exceed the byte target. Source pages, captured inputs
waiting for the writer mutex, and one fresh input block per active writer are
additional bounded allowances, so total memory also scales with the number of
active readers and replication streams.

## Source Table Resolution

Before source rows can be evaluated, discovered table or collection metadata has to be matched to the stream's parsed sync config set and the persisted source table state. That resolution decides which `SourceTable` records should receive data, which persisted bucket or parameter definition ids they cover, and which outdated mappings should be removed.

In incremental storage, one physical source entity can map to multiple `SourceTable` records. Each record owns a disjoint set of bucket data definition ids, parameter index ids, and stream-assigned event definition ids. Multiple records may evaluate different events, but each event id belongs to at most one record per physical source entity. Evaluating one event may produce multiple payloads when several payload queries match the row. New definitions can therefore snapshot without forcing already-compatible definitions to be reprocessed. An event-only record owns event ids but no bucket or parameter memberships.

See [09-resolve-tables-flow.md](./09-resolve-tables-flow.md) for the detailed source table lifecycle.

## Row And Metadata Writes

Source changes are normalized into insert, update, delete, truncate, and drop concepts before storage evaluates them. Data-query results become bucket operations, parameter-query results become parameter lookup entries, and current-row state is updated when the source requires it.

Truncates and drops remove currently replicated data for affected source tables. A drop also removes the stored source table mapping, which is useful when the source module detects table drops, renames, replica identity changes, or superseded mappings.

Automatic schema change detection is source-specific. Modules that do not detect schema changes in the streaming loop can rely on a new sync config deploy to refresh metadata and create new initial replication work for affected tables or collections.

## Checkpoint Boundaries

Flushing pending writes is separate from making those writes visible as a client checkpoint. This lets a source stream persist partial work inside a large source transaction while still exposing that work atomically only after the source reaches a valid transaction, page, snapshot, or marker boundary.

Position-only events are also meaningful. A heartbeat or checkpoint request marker can advance the stored source position without adding user data, which allows idle sources to show progress and publish checkpoint request acknowledgements.

## Resume And Snapshot State

Some sources need PowerSync storage to remember a restart cursor separately from the last visible checkpoint. That resume position is for restart safety; it does not by itself make a source position visible to clients.

In incremental streams, restart progress can be stream-level while checkpoint state remains per sync config. A blocked processing config can lag behind the stream resume position until its required snapshots and consistency boundaries are satisfied.

Storage also tracks table-level snapshot progress and source positions that checkpoints must not precede. Checkpoints remain blocked until snapshot state and those source-position boundaries allow a consistent sync point.

## Read-Side State Created By Writes

Every writer decision must preserve the state the sync API needs to serve a consistent checkpoint: the committed source position, bucket operation data, bucket checksums, dynamic parameter lookup rows, and checkpoint request acknowledgement state.

Storage may also derive checkpoint-change results from persisted write state to help the sync API avoid unnecessary checksum and parameter work. Those results are best-effort optimizations: storage may include extra buckets or lookups, or invalidate data and parameter buckets instead of returning specific changes.
