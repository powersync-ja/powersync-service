# Parameter Lookup Implementation

Most of the other docs focus on bucket data, but parameter lookup data also contains some tricky bits.

## Basic requirements

The essence of what we do when syncing data is:

1. Get the latest checkpoint.
2. Evaluate all parameter queries _at the state of the checkpoint_.
3. Return bucket data for the checkpoint.

This doc focuses on point 2.

## Current lookup implementation

We effectively store an "index" for exact lookups on parameter query tables.

The format is in MongoDB storage is:

    _id: OpId # auto-incrementing op-id, using the same sequence as checkpoints
    key: {g: <replication stream id>, t: <table id>, k: RowReplicationId } # uniquely identifies the source row
    lookup: doc # lookup entry for this source row
    bucket_parameters: data # results returned to the parameter query

If one row evaluates to multiple lookups, those are each stored as a separate document with the same key.

When a row is deleted, we empty `bucket_parameters` for the same (key, lookup) combinations.

To query, we do:

1. Filter by replication stream id: key.g.
2. Filter by lookup.
3. Filter by checkpoint: \_id <= checkpoint.
4. Return the last parameter data for each (key, lookup) combination (highest \_id)

## Compacting

In many cases, parameter query tables are updated infrequently, and compacting is not important. However, there are cases where parameter query tables are updated regularly in cron jobs (for example), and the resulting indefinite storage increase causes significant query overhead and other issues.

To handle this, we compact older data. For each (key.g, key, lookup) combination, we only need to keep the last copy (highest \_id). And if the last one is a remove operation (empty parameter_data), we can remove it completely.

One big consideration is sync clients may still need some of that data. To cover for this, parameter lookup queries should specifically use a _snapshot_ query mode, querying at the same snapshot that was used for the checkpoint lookup. This is different from the "Future Options: Snapshot queries" point above: We're not using a snapshot at the time the checkpoint was created, but rather a snapshot at the time the checkpoint was read. This means we always use a fresh snapshot.

### Incremental compaction

Compaction does not scan the entire collection. Since parameter entries use the replication stream's monotonic operation id as `_id`, those operation ids double as a work log. Each stream persists `parameter_compaction.compacted_before` on its `sync_rules` document: an exclusive operation-id boundary through which every parameter collection of the stream has been processed. A pass scans only `[compacted_before, checkpoint)`, and advances the cursor only after every collection has completed that range. All deletes are idempotent, so an interrupted pass is safely repeated.

V1 scans that range on the shared `bucket_parameters` collection using its `_id` index, and filters entries belonging to other streams in code. Since all V1 streams share the `main` op id sequence, a new stream's cursor is seeded with the sequence head when the stream is created - every entry it writes gets a higher op id, so its first compaction does not scan the history of previous deployments. V3 scans each `parameter_index_${stream_id}_${index_id}` collection separately, sharing the one stream-level cursor.

### Checkpoint change detection

Snapshot queries cover clients still reading parameter data at an older checkpoint, but not checkpoint _change detection_: on each new checkpoint, the API finds which parameter lookups changed by querying entries in `(lastCheckpoint, nextCheckpoint]`, and compaction may delete those entries before that query runs. Removing the tombstone of a deleted lookup is the worst case, since that is the only record that a client should stop using the associated buckets.

To cover this, a compaction pass persists `parameter_compaction.checkpoint_changes_invalid_before` before issuing its first delete. Every checkpoint read captures that boundary in the same snapshot as the checkpoint id, and a transition starting below it invalidates all parameter buckets rather than listing individual lookups. The change query itself also runs at the checkpoint's snapshot, so a checkpoint read before the boundary moved still sees the entries the pass deletes afterwards.

This is a narrower version of the "Globally invalidate checkpoints" alternative below: it invalidates parameter query results instead of the checkpoint, and needs no extra query, since the boundary is read together with the checkpoint state.

See [incremental-parameter-compaction.md](./incremental-parameter-compaction.md) for the full design, including the ordering requirements and failure handling.

# Alternatives

## Future Option: Snapshot queries

If we could do a snapshot query with a snapshot matching the checkpoint, the rest of the implementation could become quite simple. We could "just" replicate the latest copy of parameter tables, and run arbitrary parameter queries on them.

Unforunately, running snapshot queries for specific checkpoints are not that simple. Tricky parts include associating a snapshot with a specific checkpoint, and snapshots typically expiring after a short duration. Nonetheless, this remains an option to consider in the future.

To implement this with MongoDB:

1. Every time we `commit()` in the replication process, store the current clusterTime (we can use `$$CLUSTER_TIME` for this).
2. When we query for data, use that clustertime.
3. _Make sure we commit at least once every 5 minutes_, ideally every minute.

The last point means that replication issues could also turn into query issues:

1. Replication process being down for 5 minutes means queries stop working.
2. Being more than 5 minutes behind in replication is not an issue, as long as we keep doing new commits.
3. Taking longer than 5 minutes to complete replication for a _single transaction_ will cause API failures. This includes operations such as adding or removing tables.

In theory, we could take this even further to run query parameter queries directly on the _source_ database, without replicating.

## Compacting - Implementation alternatives

Instead of snapshot queries, some other alternatives are listed below. These are not used, just listed here in case we ever need to re-evaluate the implementation.

### 1. Last active checkpoint

Compute a "last active" checkpoint - a checkpoint that started being active at least 5 minutes ago, meaning that we can cleanup data only used for checkpoints older than that.

The issues here are:

1. We don't store older checkpoints, so it can be tricky to find an older checkpoint without waiting 5 minutes.
2. It is difficult to build in hard guarantees for parameter queries here, without relying on time-based heuristics.
3. Keep track of checkpoints used in the API service can be quite tricky.

### 2. Merge / invalidate lookups

Instead of deleting older parameter lookup records, we can merge them.

Say we have two records with the same key and lookup, and \_id of A and B (A < B). The above approach would just delete A, if A < lastActiveCheckpoint.

What we can do instead is merge into:

    _id: A
    parameter_data: B.parameter_data
    not_valid_before: B

The key here is the `not_valid_before` field: When we query for parameter data, we filter by \_id as usual. But if `checkpoint < not_valid_before`, we need to discard that checkpoint.

Now we still need to try to avoid merging recent parameter lookup records, otherwise we may keep on invalidating checkpoints as fast as we generate them. But this could function as a final safety check,
giving us proper consistency guarantees.

This roughly matches the design of `target_op` in MOVE operations.

This still does not cover deleted data: With this approach alone, we can never fully remove records after the source row was deleted, since we need that `not_valid_before` field. So this is not a complete solution.

### 3. Globally invalidate checkpoints

Another alternative is to globally invalidate checkpoints when compacting. So:

1. We pick a `lastActiveCheckpoint`.
2. Persist `noCheckpointBefore: lastActiveCheckpoint` in the sync_rules collection.
3. At some point between doing the parameter lookups and sending a `checkpoint_complete` message, we lookup the `noCheckpointBefore` checkpoint, and invalidate the checkpoint if required.

This allows us to cleanly delete older checkpoints, at the expense of needing to run another query.

This could also replace the current logic we have for `target_op` in MOVE operations.

To do the lookup very efficiently, we can apply some workarounds:

1. For each parameter query (and data query?), store the clusterTime of the results.
2. Right before sending checkpointComplete, query for the noCheckpointBefore value, using `afterClusterTime`.
3. _We can cache those results_, re-using it for other clients. As long as the `afterClusterTime` condition is satisfied, we can use the cached value.
