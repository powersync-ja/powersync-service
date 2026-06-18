# Checkpoints

Checkpoints are the boundary between replication and sync. Replication creates and advances checkpoints; the sync API watches them and turns them into client stream messages.

## Storage Checkpoint

A storage checkpoint contains:

- `checkpoint`: internal operation id.
- `lsn`: latest source replication position included by the checkpoint.
- A method for resolving parameter lookup rows at that checkpoint.

The `lsn` field is nullable in the storage interface. In normal active replication, once a source stream has produced an unblocked `commit(lsn)` or `keepalive(lsn)`, checkpoints should have a non-null source position. `null` is mainly used for empty/default checkpoint objects before any checkpointable source position has been persisted, for example:

- No active snapshot-complete checkpoint is available yet, so storage returns the empty checkpoint `0/null`.
- A newly created or restarted replication stream has a storage row, but `last_checkpoint_lsn` is still `null` because initial snapshot work is not checkpointable yet.
- Tests or manually constructed storage state have operations/checkpoint ids without a corresponding source position.

The operation id is storage-wide for the replication stream. It orders bucket operations, parameter lookup records, and write checkpoint visibility.

The source position is different from the checkpoint operation id. The checkpoint id says where the PowerSync bucket history is; the source position says how far the source replication stream has been observed. That source position is what lets initial snapshots wait for stream catch-up and lets managed write checkpoints know when a client write has been replicated back.

## Creating Checkpoints

Replication creates checkpoints through the writer:

- `commit(lsn)`: flushes pending row operations and commits a source position.
- `keepalive(lsn)`: commits a source position without row operations.

`commit()` should be called only at a source transaction or page boundary where it is valid for clients to observe all flushed changes together.

Checkpoint creation can be blocked even after `commit()` or `keepalive()` when:

- Initial snapshot is still in progress.
- A table snapshot set a `no_checkpoint_before_lsn` boundary that streaming has not reached.
- Replication restarted from a lower source position and has not caught up.

## Checkpoint Changes

Bucket storage exposes `getCheckpointChanges(last, next)` as a best-effort optimization for the sync API. It can return:

- Specific updated data buckets.
- Specific updated parameter lookups.
- A request to invalidate all data buckets.
- A request to invalidate all parameter buckets.

The sync API must tolerate false positives and invalidation. The optimization only reduces checksum and dynamic bucket work; it is not the correctness boundary.

## Bucket Checksums

For a checkpoint, the sync API computes bucket checksums with `getChecksums(checkpoint, buckets)`. It uses those checksums to send either:

- A full `checkpoint` line for a new connection or reset.
- A `checkpoint_diff` line for an existing connection.

Clients fetch bucket data only for buckets whose checksums changed or whose previous download was interrupted.

The semantics of bucket operations and checksums are described in [sync-protocol.md](../sync-protocol.md) and [bucket-properties.md](../../storage/bucket-properties.md).

## Parameter Lookups

Dynamic buckets require parameter queries to be evaluated at the same checkpoint as bucket data.

The sync API uses `ReplicationCheckpoint.getParameterSets()` to resolve lookup rows for dynamic bucket evaluation. Storage must return parameter data valid at that checkpoint and enforce configured result limits.

Parameter lookup storage and compaction are covered in [parameter-lookups.md](../../storage/parameter-lookups.md).

## Checkpoint Complete

The sync API sends `checkpoint_complete` after all required bucket data for the latest checkpoint has been sent and the checkpoint has not been invalidated during the download.

The API may omit `checkpoint_complete` for a checkpoint. This happens when a newer checkpoint supersedes it or storage reports data that invalidates the checkpoint, such as a compacted operation with a future target op. The client should then continue to the next checkpoint.

Priority buckets can emit `partial_checkpoint_complete` before all lower-priority buckets are sent.

## Managed Write Checkpoints

Managed write checkpoints let a client wait for its own backend write to round-trip through replication.

The flow is:

1. The client calls `/write-checkpoint2.json`.
2. The route asks the source `RouteAPI` to create a replication head.
3. The source reads the current head and passes it to a callback.
4. The callback stores a managed write checkpoint mapping in active bucket storage.
5. The source adapter forces a later observable source event when needed.
6. Replication eventually commits or keeps alive at or beyond that head.
7. `watchCheckpointChanges()` returns a checkpoint update with the applicable write checkpoint id.
8. The sync API includes `write_checkpoint` in the next checkpoint line for that user/client.

The source marker does not need to be replicated as user data. It only needs to make the source stream advance after the mapping exists.

Managed write checkpoints require a comparable source head. The source adapter reads a position that should include the client's write, stores it with the generated write checkpoint id, and then ensures replication observes that position or a later one. Postgres uses `pg_current_wal_lsn()` and emits a logical message; MongoDB uses cluster time and writes to `_powersync_checkpoints`; other sources use their equivalent source position and marker behavior.

### Source-Side Checkpoint Markers

A source-side checkpoint marker is a source-visible event whose job is to make the replication stream advance after PowerSync has stored a managed write checkpoint mapping. It may be a logical replication message, a write to a `_powersync_checkpoints` or `powersync_checkpoints` table or collection, or another source-specific no-op event.

Use a marker table or collection when the connector cannot otherwise guarantee that a later ordered event will appear in the replication stream after `createReplicationHead()` calls its callback. This is common when:

- The source stream only advances when source data changes, so idle databases can stop producing new positions.
- The source stream is filtered and a valid database head may include writes that are not visible to the stream.
- The source has no logical keepalive or barrier message that appears in the replication stream.
- Custom write checkpoints are implemented by writing backend-owned checkpoint ids into the source.

Do not add a marker table solely because PowerSync needs to replicate marker rows as user data. Marker rows should usually be ignored by row replication and should instead produce `keepalive(lsn)` or an equivalent checkpoint-position advance. A marker table is also unnecessary when the source already provides a reliable ordered barrier, heartbeat, or logical message that the replication stream observes after the managed write checkpoint mapping exists.

The stored managed write checkpoint head is usually the position read before the marker is written. The marker's role is to create a later observable stream position at or beyond that stored head. Writing the marker before the callback stores the mapping can recreate the same idle-source race because replication may process the marker before the sync API knows about the write checkpoint.

See [convex-write-checkpoints.md](../../modules/convex/convex-write-checkpoints.md) for a detailed example of why Convex uses a `powersync_checkpoints` collection, and why the collection is about observable stream progress rather than syncing marker documents to clients.

## Custom Write Checkpoints

Custom write checkpoint mode lets a source or integration provide its own increasing write checkpoint value. Replication queues these through `addCustomWriteCheckpoint()` on the batch and storage exposes the latest matching checkpoint for the user.

Custom write checkpoints are useful when the source write is asynchronous or when an integration wants to own the acknowledgement id. In that mode, a custom backend writes an increasing checkpoint id into the source stream, and PowerSync forwards it when replication observes it. This avoids needing a database-specific comparable source position for acknowledgement, but it requires a custom source-side marker table or equivalent write path.

Managed mode is the default.

## Idle Sources

Idle sources are a correctness concern for write checkpoints. If a write checkpoint mapping is stored after replication has already emitted the checkpoint for that head, and no later source event occurs, the connected client may never see the acknowledgement.

For that reason, source adapters should create a source-visible marker or logical message after the managed write checkpoint callback completes.
