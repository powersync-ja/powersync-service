# V3 Compaction Design

This describes the design of compaction scheduling in MongoDB storage V3. For details on what compaction means on a protocol level, see [./compating-operations.md](./compacting-operations.md).

## Goals

Compaction should:

- make background work proportional to modified buckets and their modified data, rather than proportional to the overall number of buckets and/or operations in a stream.
- support frequent, resumable, concurrent runs;
- avoid turning regular small writes into repeated full rewrites; and
- ensure that every bucket with outstanding work eventually receives a full compact.

The design makes the bucket-state collection a persistent work queue. It deliberately replaces the V1-style dirty-operation estimate with explicit scheduling and state captured at the last compact.

## Scheduling

In earlier versions, compaction required a scheduled job that would either:

1. Iterate through all buckets, filter them according to stats in the bucket-state collection, then compact if needed. This was not safe for interruption.
2. Iterate through all buckets with `estimate_since_compact.count >= 10` or similar indexed condition, perform additional filtering, then compact. This had better resumability, but could repeatedly re-compact the same busy buckets, and fail to keep up with incoming changes on others.

For the scheduling approach here, we instead focus on a single new field: `next_compact_check`. This field is indexed and populated both during replication and during compaction. It supports multiple different scenarios:

1. New data was added to the bucket while replicating, which may or may not need a compact. This schedules a _check_ on the bucket.
2. A bucket was checked for compacting, but does not meet the threshold to compact just yet. Re-schedule another check for later, when the thresholds may be met.
3. A bucket was partially compacted, and may need a full compact later. Re-schedule the full compact.

This lets an index on `next_compact_check` act as a time-prioritized compact queue. We can incrementally process this queue without reading or rewriting bucket data when a check finds no compaction work. It also keeps the logic of when to compact out of the replication worker - the replication worker only has to schedule a compact check.

Workers may inspect a bounded batch before taking a lease. Checks already known to be no-ops are rescheduled together from their unleased snapshots. Each reschedule is conditional on the state still matching that snapshot and no lease being held, so concurrent writes or compactors make the reschedule a no-op rather than losing work.

## Compact types

We use two separate compact types:

1. Full compact. This is similar to a v1 storage compact: Iterate through the bucket, replace duplicate operations with MOVE operations; squash a leading sequence of MOVE/REMOVE operations into a CLEAR operation. Additionally, this merges small chunks into larger ones if applicable.
2. Chunk compaction. This only merges small chunks into larger ones, which can be much faster. We can compute whether chunks should be merged by just reading the metadata, avoiding reading the individual operations unless we need to merge. This can also incrementally continue from the last position, instead of re-reading the entire bucket.

Chunk compaction also replaces the separate "checksum pre-calculation" operation in MongoDB v1 storage, as a similar "fast to calculate" job.

Both types calculate and persist checksum state for their compacted data. When using S3 storage, the benefit is significantly reduced. However, S3 storage is still opt-in, and this is cheap to calculate together with compacting, so we keep the logic for now.

Chunk compaction is important to maintain checksum and data reading performance over large buckets.

Full compact is required to keep bucket sizes low if the same source rows are repeatedly modified. It is also required to "expire" historical data that should not be exposed to users indefinitely.

The delay before a full compact is inversely related to the amount of work since the previous full compact, and is capped by a maximum interval. This avoids frequent full rewrites for a small amount of new work while ensuring that outstanding work is eventually compacted.

## Bucket stats

To assist with deciding whether to perform a full compact, a chunk compact, or no compact, we store current aggregate counts and sizes, a cached snapshot at the latest compact, and the figures from the last full compact needed to schedule the next one.

To allow configuring minimum and maximum intervals between compacts, we also store timestamps of the oldest uncompacted write, the latest compact, and the most recent full compact.

## Concurrency

To allow running concurrent compact jobs on different buckets, we store a renewable lease for each bucket. The compact job checks out a lease on a bucket before starting any compact work. The lease helps to avoid redundant work, but it is not the only correctness mechanism: the individual compact operations are also designed to be safe under concurrency.

## Statistics during concurrent writes

Compaction must not hold a long transaction over bucket state, because replication must remain writable. At the same time, bucket stats must remain correct if there are new writes to a bucket while we compact it.

To cater for this, the compact process calculates the delta of statistics while compacting, then applies that to the total state after compacting. If there are no concurrent modifications while compacting, the aggregate state converges on the result for the compacted range.

Some care needs to be taken to take into account the "tail" of a bucket that exists while compacting, but cannot be included in the compact job.

`next_compact_check` and `first_uncompacted_write` are also affected by this: We cannot unilaterally clear these values if there were further writes to the bucket while compacting, but we also do not capture new values for writes during compacting. If writes are detected while finalizing (by checking `last_op` for the bucket), the compact process retains conservative scheduling values for them. Since they are only used for scheduling, the values do not have to be exact, as long as they are set.

## Initial replication

Initial replication uses the same scheduled work model, but forces chunk compaction and includes the first chunk-compaction interval of scheduled work. This makes the initial pass immediate and resumable without introducing a separate selection model.

The initial pass has a fixed boundary and does not chase writes that arrive while it runs. Chunk compaction performed concurrently during replication therefore reduces the work remaining for the post-replication pass.

## Compaction jobs

This design gives us flexibility in how compaction could be run:

1. Scheduled job running once per day - same as before.
2. Scheduled job running once per hour or even every 5 minutes. Actual work to perform is throttled in the job itself, and does not depend on job scheduling anymore.
3. As a tweak to the above, allow concurrent compaction jobs if the previous one has not finished. This would effectively increase the number of compaction jobs as the backlog increases.
4. Run a single process continuously polling for new buckets to compact.
5. Run multiple processes continously polling for new buckets to compact.
6. Use auto-scaling to dynamically configure the number of compaction processes depending on the backlog size.

Note that the options 4-6 are technically feasible, but not implemented yet.
