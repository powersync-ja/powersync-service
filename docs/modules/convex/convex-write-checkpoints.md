# Convex write checkpoints

This note looks at whether the Convex implementation needs to write to the
`powersync_checkpoints` collection from
`createWriteCheckpointMarker(options?: { signal?: AbortSignal }): Promise<void>`.

TLDR: Yes, we need a `powersync_checkpoints` collection in the source database.

## Background

Write checkpoints let a client wait until its own uploaded write has been
replicated back through PowerSync before applying the corresponding synced data.
This avoids flicker and avoids accepting a local write as durable before the
backend-side effects of that write are visible in the sync stream.

The managed write checkpoint flow is:

1. The client or backend writes to the source database.
2. The PowerSync API creates a managed write checkpoint for the user/client.
3. The source connector records a source replication position that should include
   the write.
4. PowerSync stores a generated write checkpoint id together with that source
   position.
5. When replication commits or keeps alive at or past that source position,
   sync can expose the write checkpoint id to the client.

For Convex, the source position is the Convex replication cursor. The current
managed write-checkpoint flow:

1. calls `getHeadCursor()` to read the current global Convex head,
2. stores the managed write checkpoint mapping with the original head cursor (in
   the `createReplicationHead()` callback),
3. calls `createWriteCheckpointMarker()` only if the callback reports that storage
   advanced a managed checkpoint (`shouldAdvance: true`).

The marker write is intentionally not the write checkpoint position. It is a
later Convex mutation whose job is to advance the Convex delta stream beyond the
stored head after the managed mapping exists.

The key invariant is that PowerSync must observe a checkpoint update at or past
the stored head after the managed write checkpoint mapping exists. Other source
connectors make this ordering explicit by creating the mapping in the callback
and then triggering the stream. For Convex, the marker is still the mechanism
that creates the later source event, but the implementation should preserve the
same effective ordering: the client must not be left with a stored write
checkpoint id and no later observable checkpoint update.

## How the marker is used by replication

Convex `document_deltas` only advances when there is a source-side change. The
stream implementation ignores rows from the `powersync_checkpoints` table as
user data, but still treats marker-only delta pages as meaningful replication
progress:

- normal user-data delta pages are committed with `batch.commit(pageLsn)`,
- marker-only pages call `batch.keepalive(pageLsn)` immediately,
- the marker table is excluded from source schema discovery and row application.

That keepalive is enough to advance the stored checkpoint LSN. Once the stored
checkpoint LSN is at or past the managed write checkpoint head, the sync stream
can acknowledge the write checkpoint to the client.

## Case 1: no checkpoint collection

If there is no `powersync_checkpoints` collection, PowerSync can still read the
current Convex head and create a managed write checkpoint in storage. The problem
is that nothing guarantees the Convex delta stream will advance beyond that head.

### Case 1.1: no replication lag

If there is no replication lag, the latest Convex head may already include the
client's source write by the time `/write-checkpoint2.json` runs. PowerSync can
therefore associate the generated write checkpoint id with a correct source
head.

However, correctness of the association is not enough. The connected sync client
only learns about acknowledged write checkpoints when PowerSync sends a later
sync checkpoint whose source LSN is at or beyond the stored write checkpoint
head.

If the replicator has already processed past that head before the write
checkpoint mapping is created, there may be no future replication event to cause
a commit or keepalive. On an idle Convex app, repeated `document_deltas` calls can
return the same cursor, so the Convex head does not progress just because time
passes. The client can then wait indefinitely for a checkpoint acknowledgement
that is logically already true but is never published through a newer sync
checkpoint. If the backend mutation made additional changes, the client may also
not observe those changes as the confirmed result of its write.

### Case 1.2: manual write checkpoint creation

Manually calling `/write-checkpoint2.json` has the same shape. The write
checkpoint is associated with the current Convex head, but if no later source
change occurs, the stream does not advance. The connected client will not see the
write checkpoint acknowledgement until an unrelated external change moves the
Convex cursor and replication commits or keeps alive at that newer cursor.

## Case 2: checkpoint collection exists

With the `powersync_checkpoints` collection and mutation deployed,
`createWriteCheckpointMarker()` creates a small source-side write immediately
after the head is captured.

### Case 2.1: marker advances past the managed write checkpoint head

The managed write checkpoint is associated with the pre-marker head. The marker
mutation is committed after that head, so its delta cursor is greater than or
equal to the point needed to acknowledge the managed write checkpoint.

When the replicator sees the marker row in `document_deltas`, it ignores the row
as replicated data but calls `keepalive(pageLsn)`. That advances the PowerSync
checkpoint LSN past the managed write checkpoint head, allowing the generated
write checkpoint id to be sent to the client.

This works regardless of whether the normal user-data write was already
replicated before `/write-checkpoint2.json` ran, as long as the marker-driven
checkpoint update is observed after the managed write checkpoint mapping exists.
That is the event that makes the stored write checkpoint visible to sync.

## Comparison to other sources

This is the same role played by source-specific keepalive mechanisms elsewhere:

- Postgres stores the current WAL LSN and emits a logical replication message so
  replication proceeds past that LSN.
- MongoDB stores the current `clusterTime` and writes to
  `_powersync_checkpoints` so the change stream proceeds past that time.
- Convex stores the current cursor and writes to `powersync_checkpoints` so
  `document_deltas` proceeds past that cursor.

The common requirement is not "write checkpoint data must be replicated as user
data". The requirement is that, after creating the managed write checkpoint
mapping, the source stream must produce an ordered event beyond the stored
position.

## Filtered replication streams

There is an additional failure mode for source databases with filtered
replication streams. For example, imagine a Postgres setup where the logical
replication stream only includes table `A`.

1. The client writes to table `A`.
2. Some backend logic writes to table `B`.
3. The client then requests a write checkpoint.
4. PowerSync reads the current source head after the table `B` write.

That source head is a valid database LSN, but the filtered replication stream may
never emit the table `B` change. If no later table `A` change occurs, replication
will not observe an event at or beyond the stored write checkpoint LSN, even
though the source database head has advanced. The write checkpoint can then be
stuck waiting for a position that is real in the source database but not
reachable through the filtered stream.

That is why filtered sources need a marker that is guaranteed to appear in the
replication stream. In Postgres this can be a logical replication message; in
MongoDB this is a write to a collection included in the change stream. The marker
bridges the gap between "the database head advanced" and "the replication stream
observed progress".

This particular filtered-stream issue does not appear to apply to Convex in the
same way. Convex `document_deltas` is not filtered by table at the API level for
PowerSync; table filtering happens inside the replicator after it receives the
delta page. A write to another Convex table should still advance the Convex delta
cursor visible to the replicator, even if PowerSync later ignores that row
because it is not included in sync rules.

So for Convex, the checkpoint marker is less about overcoming source-side stream
filtering and more about guaranteeing source-side progress when there are no
other writes after the managed checkpoint mapping is created. The marker is still
required because an idle Convex deployment may otherwise keep returning the same
delta cursor, but the reason is different from a filtered Postgres slot that
cannot observe some source LSNs at all.

## Test results

Several manual tests were run to check whether Convex can safely omit the
`powersync_checkpoints` marker collection.

### Test 1: direct write checkpoint on an idle source

When `/write-checkpoint2.json` was called directly without any other client
mutation:

- without writing to `powersync_checkpoints`, the write checkpoint did not show
  up immediately in the client logs or PowerSync service sync logs,
- with the `powersync_checkpoints` marker write enabled, the write checkpoint did
  show up immediately.

This confirms the idle-source failure mode. A write checkpoint can be stored in
PowerSync bucket storage, but without a later Convex delta there may be no sync
checkpoint diff that exposes it to the connected client.

### Test 2: replication lag

When replication lag was introduced, the write checkpoint only appeared after
replication caught up.

This is the desired behavior. The client should not validate a write checkpoint
until PowerSync has replicated to a source position at or beyond the head stored
for that write checkpoint.

### Test 3: transaction boundaries and unfiltered deltas

Testing also indicates that `document_deltas` pages contain entire Convex
transactions, or groups of smaller complete transactions. The current replicator
commits after each page that contains changes, so the marker collection does not
appear to be needed for transaction-boundary correctness.

Testing also supports the expectation that Convex `document_deltas` is not
filtered per table at the API level. PowerSync filters rows inside the
replicator after receiving the delta page. This means Convex should not have the
same source-side filtered-stream problem described above for Postgres table
filters.

### Test 4: delayed write checkpoint association

Another test disabled the `powersync_checkpoints` marker write and added a
2-second delay in the Convex API handler before creating the managed write
checkpoint.

In that setup, the client did not receive a checkpoint associated with the write
checkpoint. The likely sequence is:

1. the client mutation committed in Convex,
2. the replicator saw the corresponding Convex cursor,
3. PowerSync sent the sync checkpoint to the client before the managed write
   checkpoint mapping existed,
4. the API handler created the managed write checkpoint after the delay,
5. no further Convex write occurred, so no later checkpoint diff was sent.

This reproduces the key race. The source head can be correct and the data can be
replicated, but the connected client can still miss the write checkpoint
acknowledgement if the write checkpoint mapping is created after the relevant
sync checkpoint has already been sent.

## Conclusion

With the current Convex replication API and managed write checkpoint design, the
write to `powersync_checkpoints` is required.

The marker collection is not required because PowerSync needs to sync the marker
document itself. It is required because Convex cursors only advance on source
writes, and managed write checkpoints are only acknowledged after bucket storage
commits or keeps alive at a cursor at or beyond the stored head. Without the
marker write, a write checkpoint can be associated with a correct Convex head but
never become visible to the connected client on an idle source, especially when
replication has already processed past that head before the write checkpoint
mapping was stored.

Avoiding source writes would require a different Convex capability or a different
PowerSync design, for example an API that can produce an ordered replication
barrier without a mutation, or storage logic that can safely publish a newly
created managed write checkpoint against an already-committed source head. Until
then, `CONVEX_CHECKPOINT_TABLE` is the mechanism that makes managed write
checkpoints reliably observable. The implementation should also preserve the
ordering invariant above; otherwise the marker can be replicated before the
managed mapping exists, recreating the same idle-source stall in a narrower race.
