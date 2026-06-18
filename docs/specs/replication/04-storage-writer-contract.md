# Storage Writer Contract

Source-specific streams started by replication jobs write through `SyncRulesBucketStorage` and `BucketStorageBatch`. This contract is implemented by bucket storage modules, currently Postgres and MongoDB storage.

## Per-Stream Storage

`SyncRulesBucketStorage` represents storage for one replication stream. A replicator obtains it from `BucketStorageFactory.getInstance(replicationStream)`.

The same interface is used by both sides of the system:

- Replication writes source changes and creates checkpoints.
- The sync API reads checkpoints, checksums, parameter sets, and bucket data.

This document focuses on the write side.

## Creating A Writer

Replication calls:

```ts
await using batch = await storage.createWriter(options);
```

Important writer options include:

- `zeroLSN`: the source-specific zero position.
- `storeCurrentData`: whether storage must retain raw current row state for partial update merging.
- `skipExistingRows`: snapshot mode behavior used when resuming snapshots.
- `markRecordUnavailable`: callback for edge cases where streaming receives a partial update before current data exists.
- `hooks` and `tracer`: testing, instrumentation, and diagnostics hooks.

The writer must be flushed or disposed when finished. `await using` is the preferred pattern.

## Resolving Tables

Before saving row changes, source modules call:

```ts
const result = await batch.resolveTables({ connection_id, source });
```

Storage compares the discovered `SourceEntityDescriptor` with persisted source table state and returns:

- `tables`: `SourceTable` records that should receive replicated data.
- `dropTables`: outdated `SourceTable` records that should be removed.

The source connector should apply `drop(result.dropTables)` when necessary, and snapshot returned tables that are relevant but not snapshot-complete.

See [resolve-tables-flow.md](../../replication/resolve-tables-flow.md) for the detailed lifecycle.

## Saving Row Changes

Source row changes are normalized into `SaveOptions`:

- `INSERT`: has `after` row data and `afterReplicaId`.
- `UPDATE`: has `after` data, `afterReplicaId`, and optionally `before`/`beforeReplicaId` when the replica identity changed.
- `DELETE`: has `beforeReplicaId` and optional `before` data.

The writer evaluates the row against the hydrated sync config:

- Data query results become bucket operations.
- Parameter query results become parameter lookup/index entries.
- Current row state may be updated for future partial update handling.

`save()` may flush automatically and return the last flushed operation id. This does not create a checkpoint by itself.

## Truncate And Drop

`truncate(sourceTables)` writes removals for all currently replicated rows in the given source tables.

`drop(sourceTables)` does the same logical removal and also removes the `SourceTable` metadata record. It is used for table drops, renames, replica identity changes, and superseded table mappings when a source module detects those changes during replication.

Automatic schema change detection is source-specific. Modules that do not detect schema changes in the streaming loop can rely on a new sync config deploy to refresh metadata and create new initial replication work for affected tables or collections.

## Flush Versus Commit

`flush()` durably writes pending operations, but does not create a checkpoint. This allows large source transactions to be flushed in pieces while still becoming visible atomically at the transaction commit boundary.

`commit(lsn)` flushes pending operations and advances the source checkpoint position. It is the normal way to finish a source transaction, source page, or snapshot batch that should become visible as one checkpoint boundary.

`commit()` must not advance the source position in the middle of a source transaction. A stream may call `flush()` inside a large transaction, but it should call `commit(lsn)` only once it is outside source transaction boundaries. One `commit()` may cover multiple complete source transactions when batching is useful, as long as none of those transactions are split across checkpoint boundaries.

`commit()` returns whether checkpointing is still blocked and whether a checkpoint was actually created.

## Keepalive

`keepalive(lsn)` advances the stored source position without associated row operations.

Use it when:

- The source stream emits a heartbeat or marker-only event.
- A write-checkpoint marker must become visible even though no user data changed.
- Replication needs to show progress through a source position while idle.

`keepalive()` must only be called outside a source transaction.

## Resume Position

`setResumeLsn(lsn)` persists where source replication should resume after restart without advancing the committed checkpoint position. It is for restart safety, not client visibility.

This is used by sources where PowerSync stores resume state itself, such as MongoDB, SQL Server, MySQL, and Convex. It is generally not needed when the source database owns replication progress, such as Postgres logical replication slots.

A stream should call `setResumeLsn(lsn)` only after it has safely processed enough source input that resuming from that token will not skip required changes. If the same source position should become visible to clients, the stream must also call `commit(lsn)` or `keepalive(lsn)` at a valid source boundary.

The writer exposes:

- `lastCheckpointLsn`: last position from `commit()` or `keepalive()`.
- `resumeFromLsn`: persisted resume position for source streams that need it.

## Snapshot State

Replication uses writer methods to persist snapshot progress:

- `updateTableProgress(table, progress)`: stores resumable per-table progress.
- `markTableSnapshotDone(tables, no_checkpoint_before_lsn)`: marks table snapshots complete and advances the consistency boundary.
- `markTableSnapshotRequired(table)`: marks a table as needing a snapshot again.
- `markSnapshotDone(no_checkpoint_before_lsn)`: marks the whole stream snapshot complete after validating table snapshot state.
- `markAllSnapshotDone(no_checkpoint_before_lsn)`: test/setup-oriented variant that skips validation.

Checkpoints remain blocked until snapshot state and `no_checkpoint_before_lsn` constraints allow a consistent sync point.

## Read-Side Outputs Created By Writes

The write contract must maintain enough state for the read side to provide:

- `getCheckpoint()`: latest checkpoint and source position.
- `watchCheckpointChanges()`: checkpoint updates plus write checkpoint acknowledgement state.
- `getCheckpointChanges()`: best-effort bucket and parameter diff hints between checkpoints.
- `getBucketDataBatch()`: chunked bucket operation data for a checkpoint.
- `getChecksums()`: bucket checksums at a checkpoint.
- `ReplicationCheckpoint.getParameterSets()`: parameter lookup rows for dynamic bucket evaluation.
