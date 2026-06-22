# DocumentDB LSN, Sentinel Checkpoint, and Write Checkpoint Notes

These notes summarize findings from testing Azure DocumentDB for MongoDB vCore change streams on the DocumentDB support branch. These are internal implementation notes; for user-affecting limitations see [documentdb-limitations.md](./documentdb-limitations.md).

## Summary

DocumentDB change streams currently require different checkpointing assumptions from standard MongoDB:

- Change events do not expose a usable `clusterTime`, so the oplog timestamp cannot be the LSN coordinate.
- Instead, a single shared counter document (`_sentinel_checkpoint`) supplies a monotonic, comparable LSN coordinate. It is advanced by writing to it and observed back through the change stream.
- Resume tokens are usable for `resumeAfter`, but should be treated as opaque and not relied on for ordering.
- The same document doubles as an explicit stream barrier: each write stamps a `stream_id` field, so a stream can recognise its own writes by content instead of timestamp ordering.

These two checkpointing strategies are encapsulated behind a single interface — see [Checkpoint Implementations](#checkpoint-implementations) below.

The practical model for DocumentDB is:

```text
sentinel counter (_sentinel_checkpoint.i)
  monotonic, comparable LSN coordinate, shared across all streams

resume token
  opaque position accepted by resumeAfter

stream_id stamp (_sentinel_checkpoint.stream_id)
  marks a write as a given stream's private barrier, or null for a
  shared standalone checkpoint observed by every stream

storage checkpoint op id
  the sync checkpoint clients consume
```

Note: `wallTime` only has second precision in our conversion, so it is unsafe as an LSN equality boundary — which is why the sentinel counter is the coordinate. `wallTime` is read only for the replication-lag metric, not as part of the DocumentDB LSN.

## Change ordering

The Azure DocumentDB team has confirmed that vCore change streams deliver **all committed operations as change events in commit order** — a single cross-document order, the same property standard MongoDB provides via `clusterTime`. (The "per shard key" ordering documented for the separate RU-based API does not apply to vCore.)

The sentinel checkpoint and write-checkpoint designs below rely on this. A checkpoint sentinel is written to a different document from the data being replicated, but because changes arrive in commit order, the sentinel's change event is delivered only after the data writes that preceded it. So committing at the sentinel is a consistent cut, and a write-checkpoint head is reached only after its data has been replicated — the same correctness argument as the standard MongoDB path, with the sentinel counter standing in for `clusterTime`.

## Checkpoint Implementations

The checkpointing strategy is selected at runtime and lives behind a single interface, `CheckpointImplementation` (`CheckpointImplementation.ts`). `ChangeStream` and `MongoRouteAPIAdapter` delegate every checkpoint-specific decision to it, instead of branching on `isDocumentDb` throughout the replication loop.

There are two implementations:

```text
TimestampCheckpointImplementation   standard MongoDB
  LSN coordinate is the oplog clusterTime: unique per operation,
  monotonic, parseable from resume tokens. Barriers and event LSNs are
  plain comparable MongoLSN strings; startAtOperationTime is available as
  a resume fallback.

SentinelCheckpointImplementation    sources without a usable clusterTime (DocumentDB)
  LSN coordinate is the shared _sentinel_checkpoint counter, observed
  through the change stream and encoded as a SentinelLSN.
```

Selection is currently by `isDocumentDb`: DocumentDB uses the sentinel implementation, standard MongoDB uses the timestamp one (`createCheckpointImplementation`). The sentinel strategy does not persist anything across restarts and is in principle usable on any MongoDB, but we do not select it there — standard MongoDB has a usable `clusterTime`, so the timestamp implementation is preferred. Everything else that genuinely depends on the platform — post-image support, `fullDocument` mode, cluster- vs database-level watch — also stays behind `isDocumentDb` in `ChangeStream`. What lives behind the interface is _how checkpoints are produced and interpreted_, so the replication loop does not branch on `isDocumentDb` throughout.

The interface covers the full checkpoint lifecycle:

```text
resume         parseResumePosition, seedPosition, logResume,
               lsnFromResumeToken
create         createBatchCheckpoint, createFirstBarrier,
               createStandaloneCheckpoint, createReplicationHead, keepalive
interpret      event.observe, event.lsn, event.resolvesBarrier,
               event.describe, checkDescendingLsn
maintenance    zeroLsn, hasPosition, checkpointClearFilter
```

The per-event interpretation methods are grouped under the `event` sub-API (`CheckpointEventApi`); each takes only the raw `ProjectedChangeStreamDocument`. The one ordering contract is that `event.observe` must be called before `event.lsn` for a given event, so the sentinel implementation's coordinate is current.

## Standard MongoDB vs DocumentDB

In standard MongoDB change streams, event `clusterTime` is a logical database timestamp. It is a BSON `Timestamp` with seconds and an increment. Multiple operations in the same second can still have different increments:

```text
{ seconds: 100, increment: 1 }
{ seconds: 100, increment: 2 }
{ seconds: 100, increment: 3 }
```

That makes it suitable as the timestamp part of an LSN and as a boundary for duplicate filtering.

DocumentDB does not expose a usable `clusterTime` on the tested change events. The only time-like field is `wallTime`, which can be converted to a Timestamp:

```ts
mongo.Timestamp.fromBits(0, Math.floor(wallTime.getTime() / 1000));
```

An observed DocumentDB checkpoint update event had this shape:

```json
{
  "_id": {
    "_data": "MzowOjM4OTczODYxMDgwOjA6Mzg5NzM4NjEwODA6MTowOg=="
  },
  "operationType": "update",
  "wallTime": "2026-06-11T07:04:01.756Z",
  "documentKey": {
    "_id": "6a2a5ddeede88f2681cfc603"
  },
  "ns": {
    "db": "powersync_test",
    "coll": "_powersync_checkpoints"
  }
}
```

The event includes `wallTime`, but no `clusterTime` and no `operationTime`.

That gives every event in the same second the same timestamp:

```text
{ seconds: 100, increment: 0 }
{ seconds: 100, increment: 0 }
{ seconds: 100, increment: 0 }
```

This is acceptable as a coarse timestamp but not as a precise stream position — every event in the same second collides. That is why the DocumentDB LSN coordinate is the sentinel counter (above), **not** a `wallTime`-derived timestamp. `wallTime` is read only for the replication-lag metric, not as part of the LSN.

## Resume Tokens

The original reported issue was that DocumentDB returned different token shapes for event-level and batch-level resume tokens:

```text
changeDocument._id       {_data: ...}
postBatchResumeToken     {_token: ...}
```

The reported behavior was:

```text
resumeAfter: changeDocument._id       failed
resumeAfter: postBatchResumeToken     succeeded
```

On the latest tested Microsoft dev cluster, this mismatch no longer reproduced. Debugger inspection showed matching event and batch tokens:

```text
changeDocument._id
{ _data: 'MzowOjM1MjY4MTIwOTEyOjA6MzUyNjgxMjA5MTI6MTowOg==' }

batchResumeToken
{ _data: 'MzowOjM1MjY4MTIwOTEyOjA6MzUyNjgxMjA5MTI6MTowOg==' }

batchResumeToken._data == changeDocument._id._data
true
```

This means event-backed checkpoints can persist `changeDocument._id`, matching the standard MongoDB path. Empty-batch keepalive checkpoints still use `postBatchResumeToken`, because there is no change event document in an empty batch.

Decoded DocumentDB `_data` tokens appear to contain structured internal fields, for example:

```text
3:0:35299946376:0:35299946376:1:0:
```

This is useful for debugging, but the token remains opaque. We should not parse it or assume it is lexicographically sortable unless Microsoft documents that guarantee.

## Cluster-Scoped Streams and the Source Database Change Gap

DocumentDB only supports cluster-level change streams. Collection- and database-level streams are rejected with `NamespaceNotFound` ("Collection not found"). The implementation therefore always opens the stream through `client.db('admin')` with `allChangesForCluster: true` and filters namespaces in the pipeline.

This has a side effect on resume safety: a change in source database name is not reliably detected. DocumentDB resume tokens are cluster-scoped, so they stay valid regardless of which database the pipeline filters target — the token stays valid when only the database name changes, and the stream silently continues against the new (typically empty) database.

Note this is not unique to DocumentDB. Standard MongoDB previously raised `ChangeStreamInvalidatedError` when resuming against a different source database, but that behavior was never reliable — it depended on which resume token type happened to be used — and [#609](https://github.com/powersync-ja/powersync-service/pull/609) (raw change streams, `flush()` + `setResumeToken()` per batch) changed the token handling that it relied on. So source-database-change detection should not be assumed on either source.

The `resuming with a different source database` test in `resume.test.ts` is skipped on DocumentDB for this reason.

A possible future fix is to persist the source database name alongside the LSN and validate it on resume, raising `ChangeStreamInvalidatedError` on mismatch to force a resync — a mechanism that would cover both sources rather than relying on token-invalidation behavior.

## The `.lte()` Dedupe Guard Does Not Apply to the Sentinel Path

After a resume, the streaming loop can skip duplicate boundary events by comparing event timestamps against the resume point:

```ts
if (startAfter != null && getEventTimestamp(event).lte(startAfter)) {
  continue;
}
```

This guard only runs when `startAfter` is set, which only the timestamp implementation produces (from the legacy `startAtOperationTime` resume path). The sentinel implementation's `parseResumePosition` always returns `startAfter: null` and resumes exclusively via `resumeAfter`, which the server guarantees will not redeliver boundary events. So the guard is inert for the sentinel path by construction — it is not a special-case `isDocumentDb` check.

This is deliberate, because the guard would also be _unsafe_ if it did run with a `wallTime`-derived timestamp. `wallTime` has second precision, so a valid new event can carry the same derived timestamp as the last checkpoint:

```text
last checkpoint: { seconds: 100, increment: 0 }
new event:       { seconds: 100, increment: 0 }
```

`.lte()` would incorrectly drop that new event. So even if a future change introduced a `startAfter` for the sentinel path, the guard must not be applied to it.

## Sentinel Checkpoints

Because `wallTime` is not a reliable boundary, DocumentDB uses a single sentinel checkpoint document.

`createSentinelCheckpointLsn` advances the `i` field of one shared document (`SENTINEL_CHECKPOINT_ID` = `_sentinel_checkpoint`) and stamps a `stream_id` field on each write:

```text
_sentinel_checkpoint.i
  global monotonic counter — the LSN coordinate, shared across all
  streams and write checkpoints

_sentinel_checkpoint.stream_id
  the calling ChangeStream's checkpointStreamId for a private batch /
  keepalive barrier, or null for a standalone checkpoint (write
  checkpoint heads, snapshot markers)
```

`i` is intentionally shared and global. Write checkpoints and client-visible storage checkpoints must compare in one coordinate system that survives new `ChangeStream` instances and new sync rules; a per-stream counter would reset or become incomparable when a new stream starts.

`stream_id` makes a single counter double as a private barrier. A stream stamps its own id on its batch barriers so it can recognise them and ignore barriers written by other streams or processes, without one stream treating another's batching marker as its own commit boundary. Standalone bumps clear it to null, and every stream observes those as the global coordinate.

The sentinel flow for a batch barrier is:

1. `$inc i, $set stream_id = <checkpointStreamId>` on `_sentinel_checkpoint` (a **single** write).
2. Return the resulting counter as the barrier marker (a `SentinelLSN` comparable — counter only, no resume token).
3. Keep processing change stream events.
4. When the stream observes an event on `_sentinel_checkpoint` whose `stream_id` is its own and whose `i` is `>=` the marker, clear the pending barrier.
5. Commit at that event's LSN.
6. The next data event creates the next sentinel barrier.

The barrier match is by content — stream ownership and counter:

```text
fullDocument.stream_id == this stream's checkpointStreamId
fullDocument.i         >= marker counter
```

The barrier write's own event resolves the barrier (its `i` equals the marker). The `>=` makes resolution robust to a later own write also satisfying an earlier marker; because `i` is monotonic, no earlier event can match.

### Single write per barrier

A batch barrier is one write. The global coordinate and the private barrier are the same `$inc` on the same document, distinguished only by the `stream_id` stamp — there is no separate barrier document and no embedded copy of the coordinate, while the global incrementing sequence is still preserved.

This matters for latency: a barrier is created for every individual replicated write that is not already part of a batch, so an extra write per barrier would add per-write latency (negligible for large batches, but significant for many small writes).

### Commit triggers

Commits are not exclusively driven by a stream's own barrier events. There are two triggers, and both matter:

```text
own barrier event (stream_id == ours)
  normal batch flow; coordinate from the event's own i

standalone checkpoint event (stream_id == null; immediate commit when caught up)
  required for write checkpoint latency on an idle stream, and for the
  keepalive design (the keepalive bump is committed via this path);
  coordinate from the event's own i
```

Both event types are self-describing for the coordinate: it is `fullDocument.i` in both cases, because the two share the one global counter.

### Why tracked position state still exists

The in-memory position (`SentinelCheckpointImplementation.position`) is a monotonic merge point fed by three sources:

```text
resume seed        sentinel parsed from the stored LSN at startup
standalone events  fullDocument.i
own barrier events fullDocument.i
```

It exists because one LSN producer is not self-describing: the `setResumeLsn` path builds an LSN from a plain data event during long catch-up stretches (20k+ changes without a commit — the batch barrier is a current-time write, so it sits at the end of the backlog and no commit happens until the stream reaches it). Data events carry no sentinel, so the coordinate must come from tracked state.

During such a stretch the coordinate is _frozen_ — data events do not bump the "LSN" counter, so only the resume token half of the LSN advances. That is exactly what resumption needs (`setResumeLsn` writes `snapshot_lsn` unconditionally, with no comparison, and resume only uses the token). The tracked position is not advancing the coordinate here; it is carrying the current frozen coordinate forward so the resume LSN stays a well-formed, non-regressing LSN that re-seeds `position` on the next restart.

The resume seed also lets the monotonic guard absorb replayed standalone events after a restart (their `i` is below the resumed position) without treating them as ordering violations.

Because every barrier and standalone bump now carries the global coordinate in its own `i`, the tracked position is not load-bearing for cross-document ordering — it is only a cache of the highest coordinate proven so far, advancing the data-event (`setResumeLsn`) path between checkpoint events. Any single event being late or absent does not affect correctness.

## `fullDocument` Is the Write-Time Post-Image

Sentinel tracking reads the counter from `fullDocument.i` on update events. This is only safe if `fullDocument` reflects the document state at the time of the write, not at the time the event is read from the cursor.

The two semantics differ in an important way:

```text
write-time post-image
  each event carries the i value its own write produced

read-time lookup (standard MongoDB updateLookup semantics)
  the server fetches the document when the cursor reads the event;
  an old event can report a newer i
```

Read-time semantics would be unsafe for the sentinel design. A backlogged stream reading an old standalone checkpoint event could observe a future counter value, commit an LSN ahead of data events it has not processed yet, and allow a write checkpoint to resolve before the caller's write has replicated.

DocumentDB does not expose `updateDescription` on update events, so the oplog-style event-time delta is not available as an alternative. The implementation must rely on `fullDocument`.

Testing against a Microsoft dev cluster confirmed write-time semantics. With a change stream position captured, five rapid `$inc` writes were issued with no cursor reading, and the events were replayed afterwards:

```text
writes:         i = 4, 5, 6, 7, 8
replayed events i = 4, 5, 6, 7, 8
```

All writes completed before the replay cursor was opened, so read-time lookup would have reported `i = 8` on every event. Each event instead carried its own value. This also confirms DocumentDB does not coalesce rapid updates to the same document — five increments produced five discrete events — which sentinel barrier matching by exact `i` depends on.

Despite the driver option being named `updateLookup` (DocumentDB rejects `required` since it does not support `changeStreamPreAndPostImages`), the engine materializes the post-image into the change entry at write time.

This assumption is verified automatically by the `fullDocument on update events is the write-time post-image` test in `documentdb_mode.test.ts`. If a future DocumentDB change moves to read-time lookups or starts coalescing updates, that test fails.

Note: the sample update event earlier in this document shows no `fullDocument` field — that capture was taken without the `fullDocument` option enabled, not because DocumentDB cannot return it.

## DocumentDB LSN Ordering

The committed DocumentDB LSN uses this shape:

```text
<sentinel counter>|<resume token>
```

The sentinel counter is the comparable coordinate. The resume token is retained for `resumeAfter`, but remains opaque.

**Encoding.** The sentinel counter is serialized as a **16-hex-char value with the same shape as a MongoDB timestamp LSN** ({@link SentinelLSN} vs {@link MongoLSN}): the high 32 bits resemble epoch seconds, the low 32 an increment. This is deliberate — it makes a sentinel LSN string-comparable with a timestamp LSN, and because the counter is seeded in the `seconds << 32` range (see [Sentinel Counter Persistence](#sentinel-counter-persistence)), a fresh sentinel LSN always sorts **above** any real-timestamp LSN issued in the past. It only _resembles_ a timestamp — the value is a synthetic monotonic counter and is never used as one (the sentinel implementation resumes purely via the resume token, never `startAtOperationTime`). The `i` examples below (`100`, `101`, …) are illustrative; real values are large timestamp-shaped numbers.

The counter must be global/shared, not stream-local. If each `ChangeStream` instance kept its own `i` sequence:

```text
stream A: i = 25
stream B: i = 1
```

then using that stream-local value as the LSN coordinate would let a new stream or new sync rules deployment appear to move replication backwards from `25` to `1`, or at least into a different coordinate system — unsafe for clients, storage checkpoints, snapshot gates, and write checkpoint comparisons.

The single shared `_sentinel_checkpoint.i` avoids that reset:

```text
_sentinel_checkpoint.i = 100  (stream_id = A)   stream A's barrier
_sentinel_checkpoint.i = 101  (stream_id = null) standalone / write checkpoint
_sentinel_checkpoint.i = 102  (stream_id = B)   stream B's barrier
```

Because every stream and every write checkpoint advances the same counter, the sentinel prefix stays in one shared, monotonic comparison domain — regardless of which stream stamped any given write.

The `stream_id` stamp still matters, but only as a private commit barrier. It answers:

```text
Has this specific ChangeStream observed its own batching marker?
```

It does not affect the LSN value exposed to storage, clients, or write checkpoint resolution — that is always the shared `i`.

This gives us three separate concepts, all carried by one document plus the token:

```text
sentinel counter (i)        shared sortable progress coordinate
stream_id stamp             private per-stream commit barrier
resume token                opaque restart position
```

## Idle Keepalive

When a change stream batch is empty for longer than the keepalive interval, the stream persists a keepalive so that resuming later does not start from a very old token (which can cause connection timeouts). The two implementations keepalive differently, and the difference is forced by how their LSN coordinate relates to the resume token.

For the timestamp implementation, the keepalive timestamp is parsed directly from the resume token (`MongoLSN.fromResumeToken`). The ordered prefix and the token advance together, so persisting the keepalive LSN is always safe and strictly increasing.

For the sentinel implementation, a naive keepalive would persist `<sentinel N>|<refreshed token>` — same sentinel, new token. That is unsafe. Storage only accepts a new checkpoint when `last_checkpoint_lsn <= lsn` as a plain string comparison. With an unchanged sentinel prefix, the comparison falls entirely to the base64-encoded resume token suffix, which is **not** lexicographically meaningful:

- The BSON length prefix is little-endian, so a token one byte longer changes the leading bytes in a way unrelated to recency.
- Base64's alphabet is not ASCII-ordered.

So roughly half of plain token refreshes would sort _below_ the persisted LSN and be silently rejected, leaving the persisted resume token to go stale on an idle stream — exactly when keepalive matters. (The timestamp implementation avoids this for free, because its prefix is derived from the token.)

The fix is to advance the shared sentinel so the ordered prefix moves forward with every refreshed token:

```text
keepalive: $inc _sentinel_checkpoint.i   (no checkpoint persisted here)
```

Two details matter:

1. **Bump, do not persist.** The keepalive only advances the counter; it does not call `batch.keepalive` directly. The bump's own change event flows through the stream and is committed by the standalone-checkpoint handling (the immediate-commit path), which advances the prefix and refreshes the token using the event's own token.

2. **Why not persist immediately.** Writes that landed after the empty batch was read — including a write checkpoint head — would be covered by an immediately-persisted LSN before the stream has actually processed them. That would let write checkpoints resolve before their data has replicated. Committing only when the bump's event is observed preserves the "commit only what the stream has seen" property.

A consequence of bumping is that the bump's event arrives moments later carrying the _same_ sentinel as the just-committed LSN (differing only in the opaque token). That descending-by-suffix comparison is expected, not an ordering violation: `checkDescendingLsn` treats an equal-sentinel descent as tolerable and returns without throwing, so the commit simply no-ops in storage (`checkpointBlocked`) instead of restarting replication.

## Sentinel Counter Persistence

The `_sentinel_checkpoint` counter is the ordered component of every committed DocumentDB LSN, including write checkpoint heads. It must therefore never move backwards. Two cases threaten it.

### Startup cleanup

`ChangeStream` clears the `_powersync_checkpoints` collection on startup to keep it tidy. The sentinel implementation's `checkpointClearFilter` excludes `_sentinel_checkpoint` from that delete (`{ _id: { $ne: SENTINEL_CHECKPOINT_ID } }`). Without this, the counter would be deleted and re-seeded on the next upsert, and new commits could compare below the persisted `last_checkpoint_lsn` — failing the out-of-order commit guard in a restart loop, and letting new write checkpoint heads resolve against old, higher committed LSNs.

### Consumer deletion and timestamp seeding

The counter lives in a user-visible collection in the _source_ database, so a consumer can delete it. Dropping the whole collection is detected (the streaming loop invalidates on the collection drop event, forcing a resync). Deleting just the document is not reliably detected.

To make that case safe rather than silently corrupting, `createSentinelCheckpointLsn` seeds a newly-created counter at the current epoch **seconds shifted into the high 32 bits** (`seconds << 32`, the same shape as a MongoDB timestamp) instead of at `1`:

```text
counter exists:  $inc i
counter missing: $setOnInsert i = (epoch_seconds << 32), then retry the $inc
```

(`$inc` and `$setOnInsert` cannot touch the same field, so this is two writes with a small retry loop; the `$setOnInsert` is a no-op under concurrent creation.)

Seeding in the `seconds << 32` range gives two properties: a sentinel LSN sorts above any real-timestamp LSN issued in the past (its high bits are the current epoch seconds), and the seed advances by `2^32` every wall-clock second while checkpoints add `1` each — so a re-created counter always resumes _ahead_ of any previously issued coordinate, a harmless forward jump rather than a backwards reset, with no detection logic required. (The forward jump needs ≥1 second to have elapsed since the original seed; the document lives from initial sync onward, so re-creation always lands in a later second.)

Verified by `sentinel counter is seeded at a timestamp value on creation` in `documentdb_helpers.test.ts`.

## Write Checkpoint Observation

Managed write checkpoints associate a user/client write checkpoint with a replication head LSN. Later, sync resolves the write checkpoint when the current replication LSN reaches or passes that stored head.

For standard MongoDB, the replication head is based on `clusterTime` / `operationTime`, which is an ordered logical timestamp.

For DocumentDB, `createReplicationHead` cannot directly capture a precise `clusterTime`, so it uses the shared sentinel counter as the head. A write checkpoint head must represent the source database replication head at the time the checkpoint is requested — the source head _after_ the caller's write — not the latest source LSN already replicated into PowerSync storage. Those two positions can be separated by replication lag, and a head that points behind the caller's source write could let the write checkpoint resolve too early.

The intended DocumentDB ordering is:

```text
caller source write <= sentinel write <= future committed checkpoint
```

`createReplicationHead` increments `_sentinel_checkpoint` with a **null `stream_id`** (a standalone bump, observed by every stream), converts that `i` value into a DocumentDB LSN, and gives that LSN to the write checkpoint callback:

```text
caller source write <= sentinel counter N <= future committed checkpoint at or beyond N
```

Committed DocumentDB replication checkpoints use a `SentinelLSN` shape:

```text
<sentinel counter>|<resume token>
```

The sentinel counter is the comparable coordinate. The resume token is retained for `resumeAfter`, but should still be treated as opaque. A write checkpoint head can use only the counter portion because write checkpoints are never used to resume replication; they only need to compare against the current replicated position.

The head is a standalone (null `stream_id`) bump rather than one of the current stream's own barriers because write checkpoints and client-visible storage checkpoints must compare in a stable coordinate system that survives new streams and new sync rules, and every stream observes standalone bumps as the global coordinate. A per-stream barrier would only be tracked by its own stream.

The sentinel write response does not include `operationTime`; an observed `_powersync_checkpoints` write response was:

```json
{
  "_id": "_sentinel_checkpoint",
  "i": 4,
  "stream_id": null
}
```

So the head is the counter `i`, not a timestamp. The sentinel counter prefix is sortable; the resume-token suffix is opaque and is not used as an ordering signal.

The sentinel head is tied to a source-side write the change stream actually observes. This is correct because vCore delivers changes in commit order (see [Change ordering](#change-ordering)): the caller's data write precedes the sentinel write, so it is delivered — and replicated — before the sentinel event that resolves the write checkpoint. The sentinel counter plays the same role `clusterTime` plays on standard MongoDB.

## Known Limitations

### Initial snapshot vs. change feed retention

Initial replication is currently sequential: `getSnapshotLsn` captures a resume position, the snapshot then runs to completion, and only afterwards does streaming resume from the captured position. The change stream is **not** consumed during the snapshot.

DocumentDB retains only a limited amount of change feed history (in the order of a few hundred MB). For a large or busy source, the snapshot can take long enough that the captured resume position rolls out of the retention window before streaming resumes. The `resumeAfter` then fails, and replication restarts from scratch — potentially in a loop for sources where the snapshot consistently outlasts retention.

This is not specific to DocumentDB in principle — standard MongoDB has the same shape — but MongoDB's oplog retention is typically time-based and far larger, so the window is rarely hit. DocumentDB's smaller, size-bound retention makes it a practical concern.

Buffering the change stream in memory during the snapshot is **not** an acceptable fix: the buffer is unbounded with respect to source write volume and snapshot duration, so it would risk running the replicator out of memory.

The intended resolution is incremental reprocessing (see [powersync-ja/powersync-service discussion #349](https://github.com/orgs/powersync-ja/discussions/349)). A side effect of that design is that the change stream is consumed from the moment the snapshot begins, so the resume position is continuously advanced and never has the chance to age out — which addresses this limitation without an in-memory buffer.

Until then, treat DocumentDB initial replication as suited to datasets small enough to snapshot well within the change feed retention window.
