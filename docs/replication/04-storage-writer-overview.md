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

## MongoDB V3/V4 Publication Pipeline

Writers on one replication lease share a two-batch pipeline. Each writer buffers
one normal input block (up to the existing row/size thresholds). Application
admits complete blocks, or partial blocks at explicit boundaries. Interleaved
snapshot and streaming `save()` calls do not flush each other's input.

One batch uploads its payloads and then publishes them to MongoDB while its
successor reads source records and evaluates changes. Uploads start only when a
batch enters the publication stage; there is no separate upload queue. Source
connectors can fetch their next page while publication runs, retaining their
existing bounded source buffering. Evaluation remains on the main thread.

Application sees the predecessor's immutable source-record delta on top of
majority reads. The delta includes tombstones and remains alive through the
application block even if its predecessor commits during a read. Before another
block can enter application, that predecessor has finished publication. Confirmed
publication clocks advance the application session, so older batches need no
retained overlay or cache.

Explicit flushes and checkpoints wait for the admitted prefix and their writer's
input. Metadata operations seal buffered writer inputs and drain publication
before changing table state. Failed publication invalidates the lease and stops
the successor; replay starts from persisted source progress. Partial-row repair
notifications retain their synchronous durability barrier.

When the replication stream is ACTIVE, snapshot writers (`skipExistingRows`)
yield to storage-side streaming backlog: full streaming input blocks waiting for
application, queued streaming publications, and outstanding streaming durability
or metadata operations. A lone streaming publication or partially buffered input
is not backlog. Snapshots may use both slots when streaming is idle, and can use
the successor slot alongside a streaming publication once the backlog clears.

Snapshot admission waits outside the writer mutex when capacity is full or
streaming work has priority, then rechecks after acquiring it. Large inputs
release admission between publication transactions. PROCESSING streams retain
normal FIFO admission. The policy is shared by existing writers after activation
and initialized from stream state when opening a writer.

This preference is non-preemptive: an admitted snapshot block can still delay
streaming. Publication remains FIFO, and mandatory metadata barriers still seal
buffered inputs and drain earlier work. Sustained streaming backlog can starve
snapshot work; no minimum background share is reserved. Unread source changes
and source-side replication lag are not measured by this storage policy.

The two slots and 64 MiB accounted-work target bound pipeline admission, with
headroom estimates for packing. These are not RSS limits: an application block or
single oversized row can exceed the byte target. Source pages and one input block
per active writer are additional bounded allowances, so total memory also scales
with the number of active readers and replication streams.

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
