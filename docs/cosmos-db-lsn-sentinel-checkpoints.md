# Cosmos DB LSN, Sentinel Checkpoint, and Write Checkpoint Notes

These notes summarize findings from testing Azure Cosmos DB for MongoDB vCore change streams on the Cosmos DB support branch.

## Summary

Cosmos DB change streams currently require different checkpointing assumptions from standard MongoDB:

- Change events do not expose a usable `clusterTime`, so the oplog timestamp cannot be the LSN coordinate.
- Instead, a shared counter document (`_standalone_checkpoint`) supplies a monotonic, comparable LSN coordinate. It is advanced by writing to it and observed back through the change stream.
- Resume tokens are usable for `resumeAfter`, but should be treated as opaque and not relied on for ordering.
- Sentinel checkpoint documents provide explicit stream barriers that can be matched by content instead of timestamp ordering.

These two checkpointing strategies are encapsulated behind a single interface — see [Checkpoint Implementations](#checkpoint-implementations) below.

The practical model for Cosmos is:

```text
standalone sentinel counter
  monotonic, comparable LSN coordinate

resume token
  opaque position accepted by resumeAfter

sentinel document
  explicit barrier proving the change stream observed a specific write

storage checkpoint op id
  the sync checkpoint clients consume
```

Historical note: an earlier design used a `wallTime`-derived timestamp as the LSN coordinate. `wallTime` only has second precision in our conversion, so it was unsafe as an equality boundary; the sentinel counter replaced it. `wallTime` is still read for the replication-lag metric, but is no longer part of the Cosmos LSN.

## Checkpoint Implementations

The checkpointing strategy is selected at runtime and lives behind a single interface, `CheckpointImplementation` (`CheckpointImplementation.ts`). `ChangeStream` and `MongoRouteAPIAdapter` delegate every checkpoint-specific decision to it, instead of branching on `isCosmosDb` throughout the replication loop.

There are two implementations:

```text
TimestampCheckpointImplementation   standard MongoDB
  LSN coordinate is the oplog clusterTime: unique per operation,
  monotonic, parseable from resume tokens. Barriers and event LSNs are
  plain comparable MongoLSN strings; startAtOperationTime is available as
  a resume fallback.

SentinelCheckpointImplementation    sources without a usable clusterTime (Cosmos DB)
  LSN coordinate is the shared _standalone_checkpoint counter, observed
  through the change stream and encoded as CosmosDBLSN.
```

Selection is by capability, not by vendor name: detection picks the implementation, and the sentinel strategy is in principle usable on any MongoDB. Everything that genuinely depends on the platform — post-image support, `fullDocument` mode, cluster- vs database-level watch — stays behind `isCosmosDb` in `ChangeStream`. Everything about _how checkpoints are produced and interpreted_ lives in the implementation.

The interface covers the full checkpoint lifecycle:

```text
resume         parseResumePosition, seedPosition, logResume
create         createBatchCheckpoint, createStandaloneCheckpoint,
               createReplicationHead, keepalive
interpret      observeCheckpointEvent, eventLsn, barrierResolved,
               isTolerableDescendingLsn, describeEventPosition
maintenance    zeroLsn, hasPosition, checkpointClearFilter
```

Every event-facing method takes only the raw `ProjectedChangeStreamDocument`. The one ordering contract is that `observeCheckpointEvent` must be called before `eventLsn` for a given event, so the sentinel implementation's coordinate is current.

## Standard MongoDB vs Cosmos DB

In standard MongoDB change streams, event `clusterTime` is a logical database timestamp. It is a BSON `Timestamp` with seconds and an increment. Multiple operations in the same second can still have different increments:

```text
{ seconds: 100, increment: 1 }
{ seconds: 100, increment: 2 }
{ seconds: 100, increment: 3 }
```

That makes it suitable as the timestamp part of an LSN and as a boundary for duplicate filtering.

Cosmos DB does not expose a usable `clusterTime` on the tested change events. The branch therefore derives the event timestamp from `wallTime`:

```ts
mongo.Timestamp.fromBits(0, Math.floor(wallTime.getTime() / 1000));
```

An observed Cosmos checkpoint update event had this shape:

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

This is acceptable as a coarse timestamp prefix, but not as a precise stream position.

## Resume Tokens

The original reported issue was that Cosmos DB returned different token shapes for event-level and batch-level resume tokens:

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

Decoded Cosmos `_data` tokens appear to contain structured internal fields, for example:

```text
3:0:35299946376:0:35299946376:1:0:
```

This is useful for debugging, but the token remains opaque. We should not parse it or assume it is lexicographically sortable unless Microsoft documents that guarantee.

## Cluster-Scoped Streams and the Source Database Change Gap

Cosmos DB only supports cluster-level change streams. Collection- and database-level streams are rejected with `NamespaceNotFound` ("Collection not found"). The implementation therefore always opens the stream through `client.db('admin')` with `allChangesForCluster: true` and filters namespaces in the pipeline.

This has a side effect on resume safety. Cosmos resume tokens are cluster-scoped, so they stay valid regardless of which database the pipeline filters target:

```text
standard MongoDB
  database-scoped stream — resuming against a different source database
  invalidates the token and raises ChangeStreamInvalidatedError

Cosmos DB
  cluster-scoped stream — the token stays valid when only the database
  name changes; the stream silently continues, filtered to the new
  (typically empty) database
```

On standard MongoDB this acts as a safeguard: repointing a connection at a different source database without resyncing fails loudly and triggers a resync. On Cosmos that safeguard never fires — replication continues from the old token as if nothing changed, scoped to the new database.

This is a known detection gap, not a test environment limitation. The `resuming with a different source database` test in `resume.test.ts` is skipped on Cosmos for this reason.

A possible future fix is to persist the source database name alongside the LSN and validate it on resume, raising `ChangeStreamInvalidatedError` on mismatch to force a resync.

## The `.lte()` Dedupe Guard Does Not Apply to the Sentinel Path

After a resume, the streaming loop can skip duplicate boundary events by comparing event timestamps against the resume point:

```ts
if (startAfter != null && getEventTimestamp(event).lte(startAfter)) {
  continue;
}
```

This guard only runs when `startAfter` is set, which only the timestamp implementation produces (from the legacy `startAtOperationTime` resume path). The sentinel implementation's `parseResumePosition` always returns `startAfter: null` and resumes exclusively via `resumeAfter`, which the server guarantees will not redeliver boundary events. So the guard is inert for the sentinel path by construction — it is not a special-case `isCosmosDb` check.

This is deliberate, because the guard would also be _unsafe_ if it did run with a `wallTime`-derived timestamp. `wallTime` has second precision, so a valid new event can carry the same derived timestamp as the last checkpoint:

```text
last checkpoint: { seconds: 100, increment: 0 }
new event:       { seconds: 100, increment: 0 }
```

`.lte()` would incorrectly drop that new event. So even if a future change introduced a `startAfter` for the sentinel path, the guard must not be applied to it.

## Sentinel Checkpoints

Because `wallTime` is not a reliable boundary, Cosmos uses sentinel checkpoint documents.

`createCheckpoint` writes to `_powersync_checkpoints` using the supplied document id and increments that document's `i` field. There are two important ids:

```text
_standalone_checkpoint
  fixed shared id used for standalone checkpoints, especially write checkpoints

checkpointStreamId
  per-ChangeStream ObjectId used for that stream's internal batch barriers
```

`STANDALONE_CHECKPOINT_ID` is intentionally shared. A write checkpoint is not owned by one specific stream-local checkpoint id; it is a source-side marker that any active replication stream should be able to observe and use to advance write checkpoint resolution.

`checkpointStreamId` is intentionally private to a `ChangeStream` instance. It lets the stream create internal checkpoint barriers while ignoring internal checkpoint documents written by other streams or processes. This avoids one stream treating another stream's batching marker as its own commit boundary.

The sentinel flow is:

1. Write a known document in `_powersync_checkpoints`, incrementing an `i` field.
2. Return a marker like `sentinel:<checkpointStreamId>:<i>`.
3. Keep processing change stream events.
4. When the stream observes the matching checkpoint document and matching `i`, clear the pending barrier.
5. Commit at the sentinel event's LSN.
6. The next data event creates the next sentinel barrier.

The important match is both document identity and increment:

```text
documentKey._id == expected checkpoint id
fullDocument.i == expectedI
```

For stream-local barriers, the expected checkpoint id is the current stream's `checkpointStreamId`. For standalone/write checkpoints, the expected checkpoint id is `STANDALONE_CHECKPOINT_ID`.

The `i` value matters because each checkpoint document is reused. It identifies a specific sentinel write, not just the checkpoint collection document in general.

## Embedded Global Sentinel in Barrier Documents

Each Cosmos batch checkpoint (`createBatchCheckpoint`) performs two writes, to two different documents:

```text
write 1: $inc _standalone_checkpoint.i        -> global coordinate N
write 2: $inc <checkpointStreamId>.i          -> this stream's barrier
```

The committed LSN pairs the global coordinate from write 1 with the resume token of write 2's change event. An earlier design relied on the change stream delivering write 1's event before write 2's event. That holds when both documents live on the same node, but change streams only guarantee ordering per document — delivery order across different documents is not guaranteed (relevant if `_powersync_checkpoints` is ever sharded, or under any feed reordering).

To remove that dependency, write 2 embeds the global value in the barrier document:

```text
{ $inc: { i: 1 }, $set: { globalSentinel: N } }
```

The barrier event is then self-describing: when the stream observes its own barrier, it reads `fullDocument.globalSentinel` directly. The committed LSN no longer depends on the standalone event having been delivered first.

### Commit triggers

Commits are not exclusively driven by barrier events. There are two triggers, and both matter:

```text
own barrier event
  normal batch flow; coordinate from the embedded globalSentinel

standalone checkpoint event (immediate commit when caught up)
  required for write checkpoint latency on an idle stream, and for the
  keepalive design (the keepalive bump is committed via this path);
  coordinate from the event's own i, which is the global counter itself
```

Both event types are self-describing for the coordinate, just via different fields.

### Why tracked position state still exists

The in-memory position (`SentinelCheckpointImplementation.position`) is not made redundant by the embedding. It is a monotonic merge point fed by three sources:

```text
resume seed        sentinel parsed from the stored LSN at startup
standalone events  fullDocument.i
own barrier events fullDocument.globalSentinel
```

It exists because one LSN producer is not self-describing: the `setResumeLsn` path builds an LSN from a plain data event during long catch-up stretches (20k+ changes without a commit — the batch barrier is a current-time write, so it sits at the end of the backlog and no commit happens until the stream reaches it). Data events carry no sentinel, so the coordinate must come from tracked state.

During such a stretch the coordinate is _frozen_ — data events do not bump the "LSN" counter, so only the resume token half of the LSN advances. That is exactly what resumption needs (`setResumeLsn` writes `snapshot_lsn` unconditionally, with no comparison, and resume only uses the token). The tracked position is not advancing the coordinate here; it is carrying the current frozen coordinate forward so the resume LSN stays a well-formed, non-regressing LSN that re-seeds `position` on the next restart.

The resume seed also lets the monotonic guard absorb replayed standalone events after a restart (their `i` is below the resumed position) without treating them as ordering violations.

The embedding changed what the tracking means: it is no longer load-bearing for cross-document ordering, only a cache of the highest coordinate proven so far. Any single source being late or absent no longer affects correctness.

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

Cosmos DB does not expose `updateDescription` on update events, so the oplog-style event-time delta is not available as an alternative. The implementation must rely on `fullDocument`.

Testing against a Microsoft dev cluster confirmed write-time semantics. With a change stream position captured, five rapid `$inc` writes were issued with no cursor reading, and the events were replayed afterwards:

```text
writes:         i = 4, 5, 6, 7, 8
replayed events i = 4, 5, 6, 7, 8
```

All writes completed before the replay cursor was opened, so read-time lookup would have reported `i = 8` on every event. Each event instead carried its own value. This also confirms Cosmos does not coalesce rapid updates to the same document — five increments produced five discrete events — which sentinel barrier matching by exact `i` depends on.

Despite the driver option being named `updateLookup` (Cosmos rejects `required` since it does not support `changeStreamPreAndPostImages`), the engine materializes the post-image into the change entry at write time.

This assumption is verified automatically by the `fullDocument on update events is the write-time post-image` test in `cosmosdb_mode.test.ts`. If a future Cosmos change moves to read-time lookups or starts coalescing updates, that test fails.

Note: the sample update event earlier in this document shows no `fullDocument` field — that capture was taken without the `fullDocument` option enabled, not because Cosmos cannot return it.

## Cosmos LSN Ordering

The committed Cosmos LSN now uses this shape:

```text
<standalone sentinel>|<resume token>
```

The standalone sentinel is the comparable coordinate. The resume token is retained for `resumeAfter`, but remains opaque.

The standalone sentinel must be global/shared, not stream-local. A stream-local sentinel starts its own `i` sequence for each `ChangeStream` instance:

```text
stream A checkpointStreamId: i = 25
stream B checkpointStreamId: i = 1
```

If that stream-local value were used as the LSN coordinate, a new stream or new sync rules deployment could appear to move replication backwards from `25` to `1`, or at least into a different coordinate system. That would be unsafe for clients, storage checkpoints, snapshot gates, and write checkpoint comparisons.

The standalone checkpoint document avoids that reset:

```text
_standalone_checkpoint.i = 100
_standalone_checkpoint.i = 101
_standalone_checkpoint.i = 102
```

Because every stream and every write checkpoint uses the same standalone document, the sentinel prefix stays in one shared, monotonic comparison domain.

The stream-local sentinel still matters, but only as a private commit barrier. It answers:

```text
Has this specific ChangeStream observed its own batching marker?
```

It should not become the LSN value exposed to storage, clients, or write checkpoint resolution.

This gives us three separate concepts:

```text
standalone sentinel
  shared sortable progress coordinate

stream-local sentinel
  private per-stream commit barrier

resume token
  opaque restart position
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
keepalive: $inc _standalone_checkpoint.i   (no checkpoint persisted here)
```

Two details matter:

1. **Bump, do not persist.** The keepalive only advances the counter; it does not call `batch.keepalive` directly. The bump's own change event flows through the stream and is committed by the standalone-checkpoint handling (the immediate-commit path), which advances the prefix and refreshes the token using the event's own token.

2. **Why not persist immediately.** Writes that landed after the empty batch was read — including a write checkpoint head — would be covered by an immediately-persisted LSN before the stream has actually processed them. That would let write checkpoints resolve before their data has replicated. Committing only when the bump's event is observed preserves the "commit only what the stream has seen" property.

A consequence of bumping is that the bump's event arrives moments later carrying the _same_ sentinel as the just-committed LSN (differing only in the opaque token). That descending-by-suffix comparison is expected, not an ordering violation: `isTolerableDescendingLsn` treats an equal-sentinel comparison as tolerable, so the commit simply no-ops in storage (`checkpointBlocked`) instead of throwing and restarting replication.

## Standalone Counter Persistence

The `_standalone_checkpoint` counter is the ordered component of every committed Cosmos LSN, including write checkpoint heads. It must therefore never move backwards. Two cases threaten it.

### Startup cleanup

`ChangeStream` clears the `_powersync_checkpoints` collection on startup to keep it tidy. The sentinel implementation's `checkpointClearFilter` excludes `_standalone_checkpoint` from that delete. Without this, the counter would reset on the next upsert, and new commits would compare below the persisted `last_checkpoint_lsn` — failing the out-of-order commit guard in a restart loop, and letting new write checkpoint heads resolve against old, higher committed LSNs.

### Consumer deletion and timestamp seeding

The counter lives in a user-visible collection in the _source_ database, so a consumer can delete it. Dropping the whole collection is detected (the streaming loop invalidates on the collection drop event, forcing a resync). Deleting just the document is not reliably detected.

To make that case safe rather than silently corrupting, `createCosmosCheckpointLsn` seeds a newly-created counter at the current epoch milliseconds instead of at `1`:

```text
counter exists:  $inc i
counter missing: $setOnInsert i = Date.now(), then retry the $inc
```

(`$inc` and `$setOnInsert` cannot touch the same field, so this is two writes with a small retry loop; the `$setOnInsert` is a no-op under concurrent creation.)

Because increments accumulate far slower than one per millisecond, a re-created counter always resumes _ahead_ of any previously issued coordinate. Deletion therefore produces a harmless forward jump in the LSN domain instead of a backwards reset — no detection logic required.

Verified by `standalone counter is seeded at a timestamp value on creation` in `cosmosdb_helpers.test.ts`.

## Write Checkpoint Observation

Managed write checkpoints associate a user/client write checkpoint with a replication head LSN. Later, sync resolves the write checkpoint when the current replication LSN reaches or passes that stored head.

For standard MongoDB, the replication head is based on `clusterTime` / `operationTime`, which is an ordered logical timestamp.

For Cosmos DB, `createReplicationHead` cannot directly capture a precise `clusterTime`. An earlier fallback used the current storage checkpoint LSN as the head. That is conceptually suspect.

A write checkpoint head should represent the source database replication head at the time the checkpoint is requested:

```text
source database head after the caller's write
```

The storage checkpoint LSN represents something different:

```text
latest source LSN already replicated into PowerSync storage
```

Those two positions can be separated by replication lag. If a write checkpoint stores the current storage LSN, it may point behind the sentinel barrier and behind the caller's source write. In that case the write checkpoint can resolve too early, because sync may already be at or beyond the stored storage LSN before the sentinel has actually been observed.

The intended Cosmos ordering is:

```text
caller source write <= sentinel write <= future committed checkpoint
```

The sentinel should force forward progress even on an idle stream, but the write checkpoint must be tied to observing that sentinel. Using the replicated storage LSN as a stand-in for the source replication head does not encode that dependency by itself.

The current branch now uses the standalone checkpoint sentinel as the Cosmos write checkpoint head. `createReplicationHead` increments `_standalone_checkpoint`, converts that `i` value into a Cosmos LSN, and gives that LSN to the write checkpoint callback:

```text
caller source write <= standalone sentinel N <= future committed checkpoint at or beyond N
```

Committed Cosmos replication checkpoints use a `CosmosDBLSN` shape:

```text
<standalone sentinel>|<resume token>
```

The standalone sentinel is the comparable coordinate. The resume token is retained for `resumeAfter`, but should still be treated as opaque. A write checkpoint head can use only the sentinel portion because write checkpoints are never used to resume replication; they only need to compare against the current replicated position.

This is also why write checkpoints use `_standalone_checkpoint` rather than the current stream's `checkpointStreamId`. Write checkpoints and client-visible storage checkpoints must compare in a stable coordinate system that survives new streams and new sync rules. A per-stream sentinel would reset or become incomparable when a new `ChangeStream` instance starts.

This is better than using either the last replicated storage LSN or `hello.operationTime`, because the write checkpoint head is now tied to a source-side sentinel write that the change stream can actually observe.

The sentinel write response itself does not include `operationTime`. An observed `_powersync_checkpoints` write response was:

```json
{
  "_id": "_standalone_checkpoint",
  "i": 4
}
```

There are two related concerns:

- source-head correctness: the stored write checkpoint head should not be an already-replicated storage head unless resolution is separately tied to the sentinel
- LSN ordering: the standalone sentinel prefix is sortable, but the resume-token suffix is opaque and should not be used as a documented ordering signal

The safer model is to resolve Cosmos write checkpoints based on sentinel observation or the storage checkpoint/op id produced after committing the sentinel. In order of preference:

```text
best:   resolve from the observed sentinel / committed storage checkpoint
better: use the standalone sentinel as the comparable Cosmos LSN head
worst:  use the last replicated storage LSN as the write checkpoint head
```

## Known Limitations

### Initial snapshot vs. change feed retention

Initial replication is currently sequential: `getSnapshotLsn` captures a resume position, the snapshot then runs to completion, and only afterwards does streaming resume from the captured position. The change stream is **not** consumed during the snapshot.

Cosmos DB retains only a limited amount of change feed history (in the order of a few hundred MB). For a large or busy source, the snapshot can take long enough that the captured resume position rolls out of the retention window before streaming resumes. The `resumeAfter` then fails, and replication restarts from scratch — potentially in a loop for sources where the snapshot consistently outlasts retention.

This is not specific to Cosmos in principle — standard MongoDB has the same shape — but MongoDB's oplog retention is typically time-based and far larger, so the window is rarely hit. Cosmos's smaller, size-bound retention makes it a practical concern.

Buffering the change stream in memory during the snapshot is **not** an acceptable fix: the buffer is unbounded with respect to source write volume and snapshot duration, so it would risk running the replicator out of memory.

The intended resolution is incremental reprocessing (see [powersync-ja/powersync-service discussion #349](https://github.com/orgs/powersync-ja/discussions/349)). A side effect of that design is that the change stream is consumed from the moment the snapshot begins, so the resume position is continuously advanced and never has the chance to age out — which addresses this limitation without an in-memory buffer.

Until then, treat Cosmos initial replication as suited to datasets small enough to snapshot well within the change feed retention window.
