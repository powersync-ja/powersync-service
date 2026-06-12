# Cosmos DB LSN, Sentinel Checkpoint, and Write Checkpoint Notes

These notes summarize findings from testing Azure Cosmos DB for MongoDB vCore change streams on the Cosmos DB support branch.

## Summary

Cosmos DB change streams currently require different checkpointing assumptions from standard MongoDB:

- Change events do not expose a usable `clusterTime`.
- The implementation falls back to `wallTime` for the timestamp portion of MongoDB LSNs.
- `wallTime` only gives second-level precision in our LSN conversion, so it is not safe as an equality boundary.
- Resume tokens are usable for `resumeAfter`, but should be treated as opaque and not relied on for ordering.
- Sentinel checkpoint documents provide explicit stream barriers that can be matched by content instead of timestamp ordering.

The practical model is:

```text
wallTime
  approximate timestamp prefix for LSN storage

resume token
  opaque position accepted by resumeAfter

sentinel document
  explicit barrier proving the change stream observed a specific write

storage checkpoint op id
  the sync checkpoint clients consume
```

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

## Why the `.lte()` Dedupe Guard Is Disabled for Cosmos

Standard MongoDB can use event timestamp comparison to skip duplicate boundary events after resume:

```ts
if (eventTimestamp.lte(startAfter)) {
  continue;
}
```

That works when `eventTimestamp` is based on `clusterTime`.

For Cosmos DB, the timestamp comes from second-precision `wallTime`. A valid new event can have the same derived timestamp as the last checkpoint:

```text
last checkpoint: { seconds: 100, increment: 0 }
new event:       { seconds: 100, increment: 0 }
```

Using `.lte()` would incorrectly drop the new event. The Cosmos path therefore avoids that guard and relies on `resumeAfter` for duplicate avoidance.

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
