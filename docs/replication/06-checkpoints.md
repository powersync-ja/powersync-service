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

The operation id is storage-wide for the replication stream. It orders bucket operations, parameter lookup records, and checkpoint request visibility.

The source position is different from the checkpoint operation id. The checkpoint id says where the PowerSync bucket history is; the source position says how far the source replication stream has been observed. That source position is what lets initial snapshots wait for stream catch-up and lets checkpoint requests know when a client write has been replicated back.

Incremental storage can separate stream-level resume progress from per-sync-config checkpoint state. For example, MongoDB storage v3 stores a stream `resume_lsn` used to restart the change stream, while each embedded sync config has its own checkpoint source position used as a consistency marker. The sync API serves checkpoints from one active config; a processing config in the same stream does not affect client checkpoints until storage activates it.

## Creating Checkpoints

Replication creates checkpoints through the writer:

- `commit(lsn)`: flushes pending row operations and commits source progress at a checkpoint-safe boundary.
- `keepalive(lsn)`: commits source progress without row operations.

`commit()` should be called only at a source transaction or page boundary where it is valid for clients to observe all flushed changes together.

Checkpoint visibility can be blocked even after `commit()` or `keepalive()` when:

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

The semantics of bucket operations and checksums are described in [sync-protocol.md](../specs/sync-protocol.md) and [bucket-properties.md](../storage/bucket-properties.md).

## Parameter Lookups

Dynamic buckets require parameter queries to be evaluated at the same checkpoint as bucket data.

The sync API uses `ReplicationCheckpoint.getParameterSets()` to resolve lookup rows for dynamic bucket evaluation. Storage must return parameter data valid at that checkpoint and enforce configured result limits.

Parameter lookup storage and compaction are covered in [parameter-lookups.md](../storage/parameter-lookups.md).

## Checkpoint Complete

The sync API sends `checkpoint_complete` after all required bucket data for the latest checkpoint has been sent and the checkpoint has not been invalidated during the download.

The API may omit `checkpoint_complete` for a checkpoint. This happens when a newer checkpoint supersedes it or storage reports data that invalidates the checkpoint, such as a compacted operation with a future target op. The client should then continue to the next checkpoint.

Priority buckets can emit `partial_checkpoint_complete` before all lower-priority buckets are sent.

## Checkpoint Requests

Checkpoint requests let a client wait for its own backend write to round-trip through replication.

The feature was previously called a managed write checkpoint. That term remains in code and storage interfaces, and the sync stream still uses the `write_checkpoint` field for backwards compatibility. In this section, "managed write checkpoint" refers to the internal stored mapping that backs a checkpoint request.

The flow is:

1. The client calls `/sync/checkpoint-request` with a client-supplied `checkpoint_request_id`. The legacy `/write-checkpoint2.json` route creates the same kind of internal mapping with a generated id.
2. The route asks the source `RouteAPI` to create a replication head.
3. The source reads the current head and passes it to a callback.
4. The callback stores the managed write checkpoint mapping in active bucket storage.
5. The source adapter forces a later observable source event when needed.
6. Replication eventually commits or keeps alive at or beyond that head.
7. `watchCheckpointChanges()` returns a checkpoint update with the applicable checkpoint request id.
8. The sync API includes that id in the legacy `write_checkpoint` field in the next checkpoint line for that user/client.

The source marker does not need to be replicated as user data. It only needs to make the source stream advance after the mapping exists.

`/sync/checkpoint-request` accepts a client-supplied `checkpoint_request_id`. Storage treats this id as monotonic for the full user/client id: it only updates the managed write checkpoint mapping when the supplied value is greater than the stored value. The response `checkpoint_request_id` is always the request id storage is actually at after handling the request. If a previous request already advanced storage to a larger value, the stale request does not update storage and the response returns that larger stored value so the client can detect that its request id was stale.

Stale requests are normal behaviour for this mode, not an error. A client that lost track of its last request id - for example after clearing local state - simply starts again at `1`. When storage already holds a larger checkpoint for that user/client, the request is a no-op and the response returns the stored value, so the client resynchronizes its counter from the response and resumes with the next id. No special recovery request is needed.

Client-requested checkpoint mappings are temporary. Compact jobs may remove them after the configured retention period. Retrying the current checkpoint request refreshes its retention timestamp without replacing its original source head, giving replication more time to reach that head. Stale requests with a lower id do not refresh the timestamp. MongoDB storage additionally retains managed mappings until `processed_at_lsn` is set, preventing cleanup during replication lag; Postgres storage uses timestamp-based cleanup.

Checkpoint requests require a comparable source head. The source adapter reads a position that should include the client's write, stores it with the checkpoint request id, and then ensures replication observes that position or a later one. Postgres uses `pg_current_wal_lsn()` and emits a logical message; MongoDB uses cluster time and writes to `_powersync_checkpoints`; other sources use their equivalent source position and marker behavior.

### Source-Side Checkpoint Markers

A source-side checkpoint marker is a source-visible event whose job is to make the replication stream advance after PowerSync has stored a checkpoint request mapping. It may be a logical replication message, a write to a `_powersync_checkpoints` or `powersync_checkpoints` table or collection, or another source-specific no-op event.

Use a marker table or collection when the connector cannot otherwise guarantee that a later ordered event will appear in the replication stream after storage persists a checkpoint request for a source head. This is common when:

- The source stream only advances when source data changes, so idle databases can stop producing new positions.
- The source stream is filtered and a valid database head may include writes that are not visible to the stream.
- The source has no logical keepalive or barrier message that appears in the replication stream.
- Custom checkpoint requests are implemented by writing backend-owned checkpoint ids into the source.

Do not add a marker table solely because PowerSync needs to replicate marker rows as user data. Marker rows should usually be ignored by row replication and should instead produce `keepalive(lsn)` or an equivalent checkpoint-position advance. A marker table is also unnecessary when the source already provides a reliable ordered barrier, heartbeat, or logical message that the replication stream observes after the checkpoint request mapping exists.

The stored checkpoint request head is usually the position read before the marker is written. The marker's role is to create a later observable stream position at or beyond that stored head. Writing the marker before the callback stores the mapping can recreate the same idle-source race because replication may process the marker before the sync API knows about the checkpoint request.

See [convex-write-checkpoints.md](../modules/convex/convex-write-checkpoints.md) for a detailed example of why Convex uses a `powersync_checkpoints` collection, and why the collection is about observable stream progress rather than syncing marker documents to clients.

## Custom Checkpoint Requests

Custom checkpoint request mode lets a source or integration provide its own increasing acknowledgement value. This is still named custom write checkpoint mode in storage APIs for backwards compatibility. Replication queues these through `addCustomWriteCheckpoint()` on the batch and storage exposes the latest matching checkpoint for the user.

Custom checkpoint requests are useful when the source write is asynchronous or when an integration wants to own the acknowledgement id. In that mode, a custom backend writes an increasing checkpoint id into the source stream, and PowerSync forwards it when replication observes it. This avoids needing a database-specific comparable source position for acknowledgement, but it requires a custom source-side marker table or equivalent write path.

Custom checkpoint request ids continue to use `checkpoint` as the supplied id in the legacy storage API. Callers that queue request-derived custom checkpoints should pass `checkpoint_requested_at` to `addCustomWriteCheckpoint()`. Storage keeps that nullable timestamp on custom checkpoint rows, just like managed checkpoint requests, so compact jobs can remove expired request-derived rows without deleting persistent custom checkpoint request rows. Omit or clear `checkpoint_requested_at` for persistent custom checkpoints owned by the source or integration.

Managed mode is the default.

## Idle Sources

Idle sources are a correctness concern for checkpoint requests. If a checkpoint request mapping is stored after replication has already emitted the checkpoint for that head, and no later source event occurs, the connected client may never see the acknowledgement.

For that reason, source adapters should create a source-visible marker or logical message after the checkpoint request callback completes.
