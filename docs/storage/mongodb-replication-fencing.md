# MongoDB replication fencing

A lease assigns a replication stream to one process. A **fence** checks that the process still owns that lease when it writes. This stops a stalled process from publishing after another process takes over.

## How it works

Each writer keeps the lease token it started with. Every flush transaction checks that token against `sync_rules.lock.id` and updates the stream's existing heartbeat (`last_keepalive_ts`). The heartbeat always changes, even when two updates happen in the same millisecond.

Changing the stream document makes lease takeover conflict with the transaction. Either:

- Takeover happens first: the old writer's fence fails, so its transaction cannot publish.
- The old writer fences first: takeover must wait for that transaction to commit or abort.

The fence runs on **every flush**, not only on `writer.commit()`, which publishes a checkpoint. Checkpoint writes check ownership even when there are no pending rows.

Snapshot progress, table resolution, and activation also use fenced transactions. Updates confined to the stream document, such as resume positions and stream snapshot state, check the lease and update the heartbeat atomically.

Different streams use different documents, so the fence does not introduce a global lock.

## Reserved operation IDs

Reserving IDs happens outside the flush transaction. A stale process can still reserve IDs, but that does not let it publish them.

For example:

1. Process 1 stalls and loses its lease to process 2.
2. Process 2 reserves range A.
3. Process 1 reserves a higher range B.
4. Process 1 tries to flush B. Its lease check fails; none of those operations become visible.
5. Process 2 can safely write A. B is unused.

If process 1 finishes a flush **before** takeover, process 2 must also avoid writing below those persisted IDs. Every flush reads the stream's persisted head inside its fenced transaction and skips reserved IDs below that head. Legacy storage uses `max(last_checkpoint, keepalive_op)`; v3/v4 use `last_persisted_op`.

Both rules matter: fence every flush, and allocate above the persisted head. Fencing only checkpoint commits would leave a consistency gap.

## Limits

- The fence checks the lease token, not its expiry time. Expiry permits takeover; it does not itself stop the old writer. Lease renewal failures also abort the local lease signal.
- Graceful shutdown lets the connector finish its current page and save progress. Losing the lease prevents further fenced writes.
- The fence orders transactions within a stream. The connector must still submit source changes in the correct order.
- Writers without a lease may write only while the stream has no lease. They can become eligible again after a lease is released.
- Setting a stream to `STOP` does not revoke its lease by itself.
- Full storage clearing, error reporting, collection creation/drop, post-commit cleanup, and external uploads are outside this fence. They rely on separate lifecycle or cleanup rules. In particular, clearing is not protected against a stalled cleanup process resuming after lease loss.
- Older service versions do not gain these ownership checks merely by sharing compatible storage.

The main implementation is in [MongoSyncRulesLock](../../modules/module-mongodb-storage/src/storage/implementation/MongoSyncRulesLock.ts) and [MongoBucketBatch](../../modules/module-mongodb-storage/src/storage/implementation/MongoBucketBatch.ts).
