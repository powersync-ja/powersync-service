# Snapshots And Streaming

Replication has two coupled responsibilities:

1. Copy existing source data that should be present for a sync config.
2. Continue applying later source changes in order.

The system only exposes a checkpoint to clients when storage can prove that bucket and parameter data are consistent at that checkpoint.

## Initial Snapshot

A new replication stream usually starts in `PROCESSING`. Incremental storage can also append a `PROCESSING` sync config to an existing active stream. If the selected config state has no completed snapshot, the source connector must snapshot the source table records storage marks incomplete for that config. In incremental storage, that may be only the new bucket or parameter memberships while compatible mappings continue serving the active config.

Initial snapshot work happens through a storage writer returned by `SyncRulesBucketStorage`, either from `createWriter()` or the older `startBatch()` helper. The source-specific stream passes that `BucketStorageBatch` through its snapshot helpers, so row copies, table progress, snapshot completion markers, and the final checkpoint commit are persisted through the same storage writer workflow.

The common flow is:

1. Create or enter a storage batch for the snapshot attempt.
2. Determine the source position that bounds or starts the snapshot.
3. Discover configured source entities.
4. Resolve each entity into `SourceTable` records through the batch.
5. Copy rows in chunks or pages.
6. Call `save()` on the batch for each row as an insert-like replicated row.
7. Persist table progress through the batch between chunks.
8. Mark each table snapshot complete with a source position that streaming must reach before checkpoints are valid.
9. Mark the overall snapshot complete.
10. Commit or keep alive the relevant source position through the batch.

Different sources choose different snapshot boundaries. Postgres uses logical replication slots and WAL LSNs. MongoDB uses a snapshotter and change stream resume state. MySQL uses GTID/binlog state. SQL Server uses CDC LSNs. Convex pins a global snapshot cursor.

Streaming is paired with the snapshot boundary in one of two common ways:

- Streaming starts immediately when the initial snapshot starts, using the recorded starting position while snapshot work runs. MongoDB incremental reprocessing follows this shape so the active config can continue streaming while the processing config catches up.
- Streaming starts after the snapshot completes, but from the source position recorded before snapshotting began.

Both approaches rely on recording the source position before or at snapshot start. The stream then replays all source changes from that position, ensuring writes that happen during the snapshot are not missed. This requires the source stream to contain every change relevant to the sync config inside the chosen boundary, even when the position is a source-specific cursor or synthetic marker rather than a native database LSN.

For example, a snapshotter may copy rows in primary-key order and already have copied every row with `id <= 10000`. If the source then runs `UPDATE ... SET id = 1 WHERE id = 20000`, later snapshot pages would miss that row. Because streaming starts from the recorded source position, the update is still replayed and the final replicated state converges correctly.

Some sources provide stronger or different primitives. Postgres can read multiple queries from one database snapshot, although long-running snapshots have operational costs and are not always restart-friendly. Convex can snapshot the database and then start streaming from the same point. A source with a complete ordered operation log could avoid a separate snapshot path and rebuild state from the log.

## Resumable Table Snapshots

Long snapshots can fail or be interrupted. Storage tracks table-level progress with:

- Estimated total rows.
- Replicated row count.
- Last key or source-specific page cursor.

On restart, the connector can skip already-replicated rows or resume from a persisted cursor. A repeated row is safe to ignore in snapshot mode because later streaming changes will converge the final state.

This is why snapshot writers can use `skipExistingRows`.

## Consistency Boundary

Table snapshots do not necessarily read every row at one global source snapshot. A table copy may observe a version of a row that is older or newer than adjacent source changes.

To avoid exposing inconsistent data, table completion records a `no_checkpoint_before_lsn` or equivalent source position. Checkpoints stay blocked until streaming has caught up to that boundary.

The important invariant is:

Clients may only receive a completed checkpoint after all snapshot data needed by the active sync config has been copied and all relevant source changes up to the snapshot completion boundary have been processed. A deploying config in an incremental stream follows the same rule before it can become active.

## Ongoing Streaming

After or alongside initial snapshot work, the connector consumes source changes in source order.

Streaming changes are also applied inside a storage batch returned by `SyncRulesBucketStorage`. The current source streams usually open one long-running batch for the duration of the streaming attempt, then repeatedly call `commit(lsn)` or `keepalive(lsn)` as source transaction, page, checkpoint-marker, or heartbeat boundaries are reached.

That long-running-batch shape is used by the main streaming loops for Postgres WAL, MongoDB change streams, MySQL binlog, SQL Server CDC polling, and Convex document deltas. The batch is a storage writer, not a single source transaction: it can flush many times, commit many checkpoints, and keep storage resume state current while the source stream continues.

For transactional consistency, `commit(lsn)` must not be called between the beginning and end of a source transaction. The stream can `flush()` partial work inside a large transaction, but it should only advance the source checkpoint position after the transaction boundary is reached. A single PowerSync commit may include multiple complete source transactions when batching improves throughput.

Shorter-lived batches are also valid when they match the source workflow. Existing examples include initial snapshot setup, table-cache warmup, queued or per-table snapshot work, and any future source where a page-scoped or polling-iteration writer is simpler. The important requirement is that source transaction boundaries and resume positions are still reflected with the correct `flush()`, `commit(lsn)`, `keepalive(lsn)`, and `setResumeLsn(lsn)` calls.

For each source transaction, page, or event batch, the stream helper generally:

1. Uses the active storage writer for the streaming attempt, or opens a shorter-lived writer when the source workflow calls for it.
2. Parses source relation/table metadata.
3. Calls `resolveTables()` for discovered entities.
4. Drops or truncates outdated source table mappings.
5. Saves inserts, updates, and deletes.
6. Flushes during large transactions if needed.
7. Calls `commit(lsn)` only after the source transaction or transaction-safe page is complete.

When the source emits an event that advances replication position but contains no user data, the helper calls `keepalive(lsn)`.

## Schema Changes During Streaming

Automatic source schema change handling is not required for every replication module. If streaming discovers a source table that is selected by the sync config but not yet snapshot-complete, the connector must decide whether to snapshot it inline, queue it, or mark it complete based on source capabilities.

For relational sources, new or changed table metadata often triggers:

- `resolveTables()`.
- `drop()` for old mappings.
- `truncate()` before re-snapshotting interrupted or changed tables.
- `markTableSnapshotDone()` when the table reaches a consistent boundary.

For Convex, deltas contain complete documents and table schema behavior differs, so newly discovered tables can be handled differently.

It is also acceptable for a source module not to detect new tables, column changes, replica identity changes, or other schema metadata changes in the streaming loop. In that case, operators must deploy a new sync config after making source schema changes that affect replication. The new processing work causes the source connector to rediscover configured source entities and snapshot any source-table mappings that are incomplete for the new config. Non-incremental storage usually represents this as a separate processing stream; incremental storage can instead append a processing config to the active stream and snapshot only the new or changed memberships.

## Restarting From Missing Source History

If the source cannot continue from persisted state, replication must not silently skip changes.

The source connector should call `restartReplication(replicationStreamId)` when source history is missing, expired, or invalidated. The storage factory then creates a fresh replication stream from the current sync config, causing a new snapshot and preserving correctness.

## Switching To Active

Once initial processing has produced a consistent checkpoint, storage can transition the stream or config to `ACTIVE`. The sync API then reads from that active state while the replication job keeps applying new changes.

An older active stream can continue serving clients while a newer stream is still processing. This allows sync config changes to be deployed without exposing partially snapshotted state.

With incremental storage, the older active config and the newer processing config can share the same replication stream. Activation atomically replaces the active config served to clients. Stopped embedded config state can then be cleaned up without dropping bucket or parameter definitions still used by the newly active config.
